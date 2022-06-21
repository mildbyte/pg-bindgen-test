pub mod cstore_sys;



use super::postgres;


use pgx::{
    pg_sys::{FormData_pg_attribute},
};

pub fn cstore_schema_to_attributes(schema: &str) -> Vec<FormData_pg_attribute> {
    let json: Vec<(i16, String, String, bool)> =
        serde_json::from_str(schema).expect("JSON was not well-formatted");

    json.iter()
        .map(|(ordinal, name, type_name, _)| postgres::build_attribute(*ordinal, name, type_name))
        .collect()
}

#[cfg(test)]
mod tests {

    use crate::{cstore::cstore_schema_to_attributes, postgres::init_pg};

    #[test]
    fn cstore_schema_to_attributes_basic() {
        unsafe {
            init_pg();
            let schema = r#"[[1, "_airbyte_emitted_at", "timestamp with time zone", true], [2, "_airbyte_ab_id", "character varying", true], [3, "_airbyte_data", "jsonb", false], [4, "sg_ud_flag", "boolean", false]]"#;
            let attributes = cstore_schema_to_attributes(schema);

            assert_eq!(attributes.len(), 4);
            assert_eq!(attributes[0].name(), "_airbyte_emitted_at");
        }
    }
}
