pub mod cstore_sys;

use std::{ffi::CString};

use super::postgres;

use cstore_sys::{CStoreBeginRead, CStoreEndRead, CStoreReadNextRow};
use pgx::{
    pg_sys::{self, FormData_pg_attribute},
    FromDatum, PgList,
};

const SCHEMA: &str = r#"[[1, "_airbyte_emitted_at", "timestamp with time zone", true], [2, "_airbyte_ab_id", "character varying", true], [3, "_airbyte_data", "jsonb", false], [4, "sg_ud_flag", "boolean", false]]"#;

pub fn cstore_schema_to_attributes(schema: &str) -> Vec<FormData_pg_attribute> {
    let json: Vec<(i16, String, String, bool)> =
        serde_json::from_str(schema).expect("JSON was not well-formatted");

    json.iter()
        .map(|(ordinal, name, type_name, _)| postgres::build_attribute(*ordinal, name, type_name))
        .collect()
}

pub fn do_something() -> () {
    unsafe {
        postgres::init_pg();
    }

    let attribute_num: usize = 3;
    let file_path = CString::new("/home/mildbyte/pg-bindgen-test/data/o564173e5b42a103f7079e0401d6269e54b5930a9d2144911d3f1db41a3fa1b").unwrap();

    let all_attrs = cstore_schema_to_attributes(SCHEMA);

    let mut column_list = PgList::<pg_sys::Var>::new();
    column_list.push(&mut pg_sys::Var {
        varattno: attribute_num.try_into().unwrap(),
        vartype: all_attrs[attribute_num].type_oid().value(),
        ..Default::default()
    });

    let read_state = unsafe {
        CStoreBeginRead(
            file_path.as_ptr(),
            postgres::create_tuple_desc(&all_attrs).as_ptr(),
            column_list.into_pg(),
            std::ptr::null_mut(),
        )
    };

    let mut column_values: Vec<pg_sys::Datum> = vec![0; attribute_num];
    let mut column_nulls: Vec<bool> = vec![false; attribute_num];

    unsafe {
        while CStoreReadNextRow(
            read_state,
            column_values.as_mut_ptr(),
            column_nulls.as_mut_ptr(),
        ) {
            let date = pgx::JsonB::from_datum(
                column_values[attribute_num - 1],
                column_nulls[attribute_num - 1],
                all_attrs[attribute_num].type_oid().value(),
            );
            println!("{:?}", date);
        }

        CStoreEndRead(read_state);
        pg_sys::AbortCurrentTransaction();
    }
}

#[cfg(test)]
mod tests {

    

    use crate::{cstore::cstore_schema_to_attributes, postgres::init_pg};

    use super::do_something;

    #[test]
    fn do_something_smoke() {
        unsafe {
            do_something();
        }
    }

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
