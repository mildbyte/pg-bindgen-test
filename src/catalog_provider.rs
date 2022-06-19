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

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        return Some(Arc::new(CStoreSchemaProvider {
            basepath: self.basepath.clone(),
        }));
    }
}

pub struct CStoreSchemaProvider {
    basepath: String,
}

impl SchemaProvider for CStoreSchemaProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        fs::read_dir(&self.basepath)
            .unwrap()
            .into_iter()
            .filter(|r| r.is_ok())
            .map(|r| r.unwrap().path())
            .filter(|r| r.is_file())
            .map(|r| r.file_name().unwrap().to_str().unwrap().to_string())
            .filter(|n| !n.ends_with(".footer") && !n.ends_with(".schema"))
            .collect()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let path = PathBuf::from(&self.basepath).join(name);
        let data_source = CStoreDataSource::new(path.as_os_str());
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
            vec!["o564173e5b42a103f7079e0401d6269e54b5930a9d2144911d3f1db41a3fa1b"]
        );
    }
}
