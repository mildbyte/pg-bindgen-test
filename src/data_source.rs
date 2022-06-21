use std::collections::VecDeque;
use std::ffi::{CString, OsString};
use std::fs;
use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;
use std::os::unix::prelude::OsStrExt;
use std::path::PathBuf;
use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::arrow::datatypes::Field;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::error::Result;

use datafusion::{
    arrow::{
        datatypes::{Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::TableProvider,
    execution::context::{SessionState, TaskContext},
    logical_expr::TableType,
    logical_plan::Expr,
    physical_expr::PhysicalSortExpr,
    physical_plan::{project_schema, ExecutionPlan, SendableRecordBatchStream, Statistics},
};
use pgx::pg_sys::{self, FormData_pg_attribute};
use pgx::{name_data_to_str, AllocatedByPostgres, PgBox, PgList};

use crate::cstore::cstore_schema_to_attributes;
use crate::cstore::cstore_sys::{CStoreBeginRead, CStoreEndRead, CStoreReadNextRow};
use crate::pg_to_arrow::{attr_to_appender_builder, pg_oid_to_arrow_datatype, DatumAppender};
use crate::postgres;

#[derive(Debug, Clone)]
pub struct CStoreDataSource {
    object_paths: Vec<OsString>,
    pg_schema: Vec<FormData_pg_attribute>,
}

impl CStoreDataSource {
    pub fn new(object_paths: Vec<OsString>) -> Self {
        // Assume the first object's schema is the same as all other ones
        let schema_json =
            fs::read_to_string(PathBuf::from(&object_paths[0]).with_extension("schema"))
                .expect("Something went wrong reading the file");

        let guard = postgres::PG_INTERNALS_LOCK.lock().unwrap();
        let attributes = cstore_schema_to_attributes(&schema_json);
        drop(guard);

        Self {
            object_paths: object_paths.iter().map(|p| p.to_os_string()).collect(),
            pg_schema: attributes,
        }
    }

    pub(crate) async fn create_physical_plan(
        &self,
        projections: &Option<Vec<usize>>,
        schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CStoreExec::new(projections, schema, self.clone())))
    }
}

fn pg_attribute_to_field(pg_attribute: &FormData_pg_attribute) -> Field {
    Field::new(
        name_data_to_str(&pg_attribute.attname),
        pg_oid_to_arrow_datatype(pg_attribute.type_oid()),
        true,
    )
}

#[async_trait]
impl TableProvider for CStoreDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        SchemaRef::new(Schema::new(
            self.pg_schema.iter().map(pg_attribute_to_field).collect(),
        ))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection, self.schema()).await;
    }
}

#[derive(Debug, Clone)]
struct CStoreExec {
    db: CStoreDataSource,
    projected_schema: SchemaRef,
    projections: Vec<usize>,
}

impl CStoreExec {
    fn new(projections: &Option<Vec<usize>>, schema: SchemaRef, db: CStoreDataSource) -> Self {
        let projected_schema = project_schema(&schema, projections.as_ref()).unwrap();
        Self {
            db,
            projected_schema,
            projections: projections
                .clone()
                .unwrap_or_else(|| (0..schema.fields().len()).collect()),
        }
    }
}

impl ExecutionPlan for CStoreExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(self.db.object_paths.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut column_list = PgList::<pg_sys::Var>::new();
        let mut appenders: Vec<Box<dyn DatumAppender>> = Vec::with_capacity(self.projections.len());

        // NB: from testing this with an expression selecting the same column with two aliases, self.projections
        // doesn't repeat columns, so we won't be doing extra work.

        // Because PostgreSQL isn't multi-threaded, we have to do this whole read
        // in a critical section. This code hits things like SysCaches etc.
        let guard = postgres::PG_INTERNALS_LOCK.lock().unwrap();
        for i in &self.projections {
            let attr = self.db.pg_schema[*i];
            column_list.push(
                PgBox::new(pg_sys::Var {
                    varattno: attr.num(),
                    vartype: attr.type_oid().value(),
                    ..Default::default()
                })
                .into_pg(),
            );
            appenders.push(attr_to_appender_builder(&attr, 1024))
        }

        let stream = CStoreExecStream {
            object_paths: VecDeque::from(vec![self.db.object_paths[_partition].clone()]),
            schema: self.projected_schema.to_owned(),
            projection: self.projections.clone(),
            column_list,
            appenders,
            tuple_desc: postgres::create_tuple_desc(&self.db.pg_schema),
        };
        drop(guard);
        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
            is_exact: false,
        }
    }
}

struct CStoreExecStream {
    object_paths: VecDeque<OsString>,
    schema: SchemaRef,
    projection: Vec<usize>,
    column_list: PgList<pg_sys::Var>,
    appenders: Vec<Box<dyn DatumAppender>>,
    tuple_desc: PgBox<pg_sys::TupleDescData, AllocatedByPostgres>,
}

impl CStoreExecStream {}

// YOLO
unsafe impl Send for CStoreExecStream {}

impl Iterator for CStoreExecStream {
    type Item = ArrowResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.object_paths.pop_front() {
            None => None,
            Some(object_path) => {
                let guard = postgres::PG_INTERNALS_LOCK.lock().unwrap();
                let read_state = unsafe {
                    let c_path = CString::new(object_path.as_bytes()).unwrap();
                    CStoreBeginRead(
                        c_path.as_ptr(),
                        self.tuple_desc.as_ptr(),
                        self.column_list.as_ptr(),
                        std::ptr::null_mut(),
                    )
                };

                let mut column_values: Vec<pg_sys::Datum> = vec![0; self.tuple_desc.natts as usize];
                let mut column_nulls: Vec<bool> = vec![false; self.tuple_desc.natts as usize];
                unsafe {
                    while CStoreReadNextRow(
                        read_state,
                        column_values.as_mut_ptr(),
                        column_nulls.as_mut_ptr(),
                    ) {
                        self.projection
                            .iter()
                            .enumerate()
                            .for_each(|(output_num, source_num)| {
                                self.appenders[output_num]
                                    .call(column_values[*source_num], column_nulls[*source_num])
                            });
                    }

                    CStoreEndRead(read_state);
                }

                drop(guard);
                Some(RecordBatch::try_new(
                    self.schema.clone(),
                    self.appenders.iter_mut().map(|b| b.finish()).collect(),
                ))
            }
        }
    }
}

impl Stream for CStoreExecStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(Iterator::next(&mut *self))
    }
}

impl RecordBatchStream for CStoreExecStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsString, sync::Arc};

    use datafusion::{
        arrow::datatypes::{DataType, Field},
        dataframe::DataFrame,
        datasource::TableProvider,
        logical_plan::{col, provider_as_source, Expr, LogicalPlanBuilder},
        prelude::SessionContext,
    };

    use crate::postgres::init_pg;

    use super::CStoreDataSource;

    use std::time::Duration;
    use tokio::time::timeout;

    #[test]
    fn data_source_initialization() {
        unsafe {
            init_pg();
        }

        let data_source = CStoreDataSource::new(vec![OsString::from("/home/mildbyte/pg-bindgen-test/data/o564173e5b42a103f7079e0401d6269e54b5930a9d2144911d3f1db41a3fa1b")]);

        assert_eq!(
            *data_source.schema().fields(),
            vec![
                Field::new("_airbyte_emitted_at", DataType::Binary, true),
                Field::new("_airbyte_ab_id", DataType::Binary, true),
                Field::new("_airbyte_data", DataType::Binary, true),
                Field::new("sg_ud_flag", DataType::Binary, true)
            ]
        );
    }

    #[tokio::test]
    async fn data_source_execution() {
        unsafe {
            init_pg();
        }
        let data_source = CStoreDataSource::new(vec![OsString::from("/home/mildbyte/pg-bindgen-test/data/o564173e5b42a103f7079e0401d6269e54b5930a9d2144911d3f1db41a3fa1b")]);

        let ctx = SessionContext::new();

        // create logical plan composed of a single TableScan
        let logical_plan = LogicalPlanBuilder::scan_with_filters(
            "test",
            provider_as_source(Arc::new(data_source)),
            None,
            vec![],
        )
        .unwrap()
        .build()
        .unwrap();

        let dataframe = DataFrame::new(ctx.state, &logical_plan)
            .select(vec![
                Expr::Alias(Box::new(col("_airbyte_emitted_at")), "col1".to_string()),
                Expr::Alias(Box::new(col("_airbyte_emitted_at")), "col2".to_string()),
            ])
            .unwrap();

        timeout(Duration::from_secs(10), async move {
            let result = dataframe.collect().await.unwrap();
            let record_batch = result.get(0).unwrap();
            dbg!(record_batch.columns());
        })
        .await
        .unwrap();
    }
}
