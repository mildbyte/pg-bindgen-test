use std::ffi::CString;
use std::fs;

use std::{any::Any, sync::Arc};

use super::postgres::init_pg;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field};
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
    physical_plan::{
        memory::MemoryStream, project_schema, ExecutionPlan, SendableRecordBatchStream, Statistics,
    },
};
use pgx::pg_sys::{self, AbortCurrentTransaction, FormData_pg_attribute, StartTransactionCommand};
use pgx::{name_data_to_str, PgList};

use crate::cstore::cstore_schema_to_attributes;
use crate::cstore::cstore_sys::{CStoreBeginRead, CStoreEndRead, CStoreReadNextRow};
use crate::pg_to_arrow::prepare_conversions;
use crate::postgres;

#[derive(Debug, Clone)]
pub struct CStoreDataSource {
    object_path: String,
    pg_schema: Vec<FormData_pg_attribute>,
}

impl CStoreDataSource {
    fn new(object_path: &str) -> Self {
        let schema_json = fs::read_to_string(object_path.to_owned() + ".schema")
            .expect("Something went wrong reading the file");

        let attributes = unsafe { cstore_schema_to_attributes(&schema_json) };

        Self {
            object_path: object_path.to_string(),
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
        DataType::Binary,
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
}

impl CStoreExec {
    fn new(projections: &Option<Vec<usize>>, schema: SchemaRef, db: CStoreDataSource) -> Self {
        let projected_schema = project_schema(&schema, projections.as_ref()).unwrap();
        Self {
            db,
            projected_schema,
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
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
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
        // Here:
        //   -> CStoreBeginScan?
        //   -> need to return a vec of array refs
        //   -> so we need to have an array of PrimitiveBuilder arrays (vec of dyn PrimitiveBuilders?)
        //   -> also a map of OIDs to output functions and types (so DataType and a Fn of Datum, &mut ArrayBuilder -> ())

        let (appenders, mut data_builders) = prepare_conversions(&self.db.pg_schema);

        // Let's roll

        // Prepare a list of columns that CStore uses to select just the required parts
        // TODO: we actually don't know this, we need to run that projection operation on our vector of
        // pg attributes
        // right now we pretend to select all columns
        let mut column_list = PgList::<pg_sys::Var>::new();

        for attr in &self.db.pg_schema {
            column_list.push(&mut pg_sys::Var {
                varattno: attr.num(),
                vartype: attr.type_oid().value(),
                ..Default::default()
            })
        }

        let read_state = unsafe {
            let c_path = CString::new(self.db.object_path.as_str()).unwrap();
            CStoreBeginRead(
                c_path.as_ptr(),
                postgres::create_tuple_desc(&self.db.pg_schema).as_ptr(),
                column_list.into_pg(),
                std::ptr::null_mut(),
            )
        };

        let mut column_values: Vec<pg_sys::Datum> = vec![0; self.db.pg_schema.len()];
        let mut column_nulls: Vec<bool> = vec![false; self.db.pg_schema.len()];

        unsafe {
            while CStoreReadNextRow(
                read_state,
                column_values.as_mut_ptr(),
                column_nulls.as_mut_ptr(),
            ) {
                // NB: we have as many vals/nulls as the original schema, but we'll have
                // fewer appenders so we won't be able to just zip like this

                for i in 0..self.db.pg_schema.len() {
                    appenders[i](column_values[i], column_nulls[i], &mut data_builders[i]);
                }
            }

            CStoreEndRead(read_state);
        }

        Ok(Box::pin(MemoryStream::try_new(
            vec![RecordBatch::try_new(
                self.projected_schema.clone(),
                data_builders.iter_mut().map(|b| b.finish()).collect(),
            )?],
            self.schema(),
            None,
        )?))
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::datatypes::{DataType, Field},
        datasource::TableProvider,
    };

    use crate::postgres::init_pg;

    use super::CStoreDataSource;

    #[test]
    fn data_source_initialization() {
        unsafe {
            init_pg();
        }

        let data_source = CStoreDataSource::new("/home/mildbyte/pg-bindgen-test/data/o564173e5b42a103f7079e0401d6269e54b5930a9d2144911d3f1db41a3fa1b");

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
}
