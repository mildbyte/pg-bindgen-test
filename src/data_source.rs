use std::fs;
use std::{any::Any, sync::Arc};

use super::postgres::init_pg;
use async_trait::async_trait;
use datafusion::arrow::array::{UInt64Builder, UInt8Builder};
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
use pgx::name_data_to_str;
use pgx::pg_sys::{AbortCurrentTransaction, FormData_pg_attribute, StartTransactionCommand};

use crate::cstore::cstore_schema_to_attributes;

#[derive(Debug, Clone)]
pub struct CStoreDataSource {
    object_path: String,
    pg_schema: Vec<FormData_pg_attribute>,
}

impl CStoreDataSource {
    fn new(object_path: &str) -> Self {
        let schema_json = fs::read_to_string(object_path.to_owned() + ".schema")
            .expect("Something went wrong reading the file");

        let attributes = unsafe {
            StartTransactionCommand();
            let attributes = cstore_schema_to_attributes(&schema_json);
            AbortCurrentTransaction();
            attributes
        };

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

        let mut id_array = UInt8Builder::new(0);
        let mut account_array = UInt64Builder::new(0);

        Ok(Box::pin(MemoryStream::try_new(
            vec![RecordBatch::try_new(
                self.projected_schema.clone(),
                vec![
                    Arc::new(id_array.finish()),
                    Arc::new(account_array.finish()),
                ],
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
