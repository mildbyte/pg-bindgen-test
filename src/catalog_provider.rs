use std::{fs, path::PathBuf, sync::Arc};

use datafusion::{
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    datasource::TableProvider,
};

use crate::data_source::CStoreDataSource;

pub struct CStoreCatalogProvider {
    pub basepath: String,
}

impl CatalogProvider for CStoreCatalogProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["public".to_string()]
    }

    fn schema(&self, _name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(CStoreSchemaProvider {
            basepath: self.basepath.clone(),
        }))
    }
}

pub struct CStoreSchemaProvider {
    basepath: String,
}

fn is_valid_cstore_file(r: &PathBuf) -> bool {
    if !r.is_file() {
        return false;
    }
    let file_name = r.file_name().unwrap().to_str().unwrap().to_string();
    if file_name.ends_with(".footer") || file_name.ends_with(".schema") {
        return false;
    }
    if !r.with_extension("schema").exists() || !r.with_extension("footer").exists() {
        return false;
    }
    true
}

impl SchemaProvider for CStoreSchemaProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        vec!["table".to_string()]
    }

    fn table(&self, _name: &str) -> Option<Arc<dyn TableProvider>> {
        // TODO: here we actually do want to order objects by how they show up
        let paths = fs::read_dir(&self.basepath)
            .unwrap()
            .into_iter()
            .filter(|r| r.is_ok())
            .map(|r| r.unwrap().path())
            .filter(is_valid_cstore_file)
            .map(|r| r.file_name().unwrap().to_str().unwrap().to_string())
            .map(|f| PathBuf::from(&self.basepath).join(f).as_os_str().to_owned())
            .collect();

        let data_source = CStoreDataSource::new(paths);
        Some(Arc::new(data_source))
    }

    fn table_exist(&self, name: &str) -> bool {
        let path = PathBuf::from(&self.basepath).join(name);
        path.exists() && path.is_file()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::catalog::{catalog::CatalogProvider, schema::SchemaProvider};

    use super::{CStoreCatalogProvider, CStoreSchemaProvider};

    #[test]
    fn catalog_provider() {
        let provider = CStoreCatalogProvider {
            basepath: "/home/mildbyte/pg-bindgen-test/data".to_string(),
        };
        assert_eq!(provider.schema_names(), vec!["public"]);
    }

    #[test]
    fn schema_provider() {
        let provider = CStoreSchemaProvider {
            basepath: "/home/mildbyte/pg-bindgen-test/data".to_string(),
        };
        assert!(
            provider.table_exist("o564173e5b42a103f7079e0401d6269e54b5930a9d2144911d3f1db41a3fa1b")
        );
        assert!(!provider.table_exist("doesntexist"));
        assert_eq!(
            provider.table_names(),
            // vec!["o564173e5b42a103f7079e0401d6269e54b5930a9d2144911d3f1db41a3fa1b"]
            vec!["table"]
        );
    }
}
