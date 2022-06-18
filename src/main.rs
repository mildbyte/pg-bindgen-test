#![feature(extern_types)]

use std::{ffi::CString, borrow::BorrowMut};

use cstore_bindings::{CStoreBeginRead, CStoreEndRead, CStoreReadNextRow};
use pgx::{
    pg_sys::{self, CreateTupleDesc, FormData_pg_attribute, FormData_pg_type, NameData, Form_pg_attribute},
    FromDatum, PgList,
};

mod cstore_bindings;

use lazy_static::lazy_static;
use std::sync::{LockResult, Mutex, MutexGuard};

// lazy_static! {
//     static ref PG_INTERNALS_LOCK: Mutex<()> = Mutex::new(());
//     static ref ONETIME_SETUP: () = {
//         unsafe {
//             pg_sys::MemoryContextInit();
//         }
//     };
// }

// Build pg with
// CFLAGS="-fPIC -ggdb -Og -g3 -fno-omit-frame-pointer" LDFLAGS_EX="--shared" ./configure --enable-cassert --enable-debug
// make
// cp src/backend/postgres $PROJECT_DIR/lib/libpostgres.so

const SCHEMA: &str = r#"[[1, "_airbyte_emitted_at", "timestamp with time zone", true], [2, "_airbyte_ab_id", "character varying", true], [3, "_airbyte_data", "jsonb", false], [4, "sg_ud_flag", "boolean", false]]"#;
// We care about
// - atttypid (OID of the type)
// - attbyval
// - attlen
// - attalign (for arrays)
// - attnum (for defaults?)
// - for defaults (we don't care since we don't support defaults in sg)
// - atthasdef
// - TupleDesc->constr
// - TupleConstraints->defval
// - attname (for errors)
/*
sgr@localhost:splitgraph> SELECT attname, atttypid, attbyval, attlen, attalign, attnum, atthasdef
FROM pg_attribute WHERE attrelid = (
    SELECT oid FROM pg_class WHERE relname = 'o564173e5b42a103f7079e0401d6269e54b5930a9d2144911d3f1db41a3fa1b'
) AND attnum > 0 ORDER BY attnum ASC;
+---------------------+----------+----------+--------+----------+--------+-----------+
| attname             | atttypid | attbyval | attlen | attalign | attnum | atthasdef |
|---------------------+----------+----------+--------+----------+--------+-----------|
| _airbyte_emitted_at | 1184     | True     | 8      | d        | 1      | False     |
| _airbyte_ab_id      | 1043     | False    | -1     | i        | 2      | False     |
| _airbyte_data       | 3802     | False    | -1     | i        | 3      | False     |
| sg_ud_flag          | 16       | True     | 1      | c        | 4      | False     |
+---------------------+----------+----------+--------+----------+--------+-----------+
*/

fn make_name_data(name: &str) -> NameData {
    let mut inner = [0; 64];
    inner[..name.len()]
        .copy_from_slice(unsafe { &*(name.as_bytes() as *const [u8] as *const [i8]) });
    NameData { data: inner }
}

// #[repr(C)]
// pub struct TransactionStateData {
//     fullTransactionId: pg_sys::FullTransactionId,
//     subTransactionId: pg_sys::SubTransactionId,
//     name: usize,
//     savepointLevel: ::std::os::raw::c_int,
//     // TODO C enums aren't always 1 byte/1 int large, they
//     // seem to change based on the compiler
//     state: u8, // There's more stuff here but we don't care about it
// }

extern "C" {
    // pub static mut CurrentTransactionState: *mut TransactionStateData;
    fn InitializeTimeouts();
}

unsafe fn init_pg() -> () {
    // Pretend to be a standalone Postgres backend (as if we were running postgres --single)
    // To do that, we're executing all the commands from PostgresMain from postgres.c
    // required to initialize various subsystems.
    // Why is all this required? CStore uses the system catalog to look up operators and their
    // implementations (to e.g. refute qualifiers). That code uses internal PG functionality
    // like the caches, the pg_operator/pg_type relations etc. These routines in exchange do
    // a bunch of assertions about the system state and other global variables, including
    // transactions, shared memory etc. When I tried manually setting up the required state just
    // to get CStore working, it quickly cascaded and mimicking what postgres --single does
    // is the simplest way to get all the PG globals in the right state.
    //
    // This is also why we're pointing PG to a "postgres" database initialized by pgx for us:
    // that database contains the bootstrapped values for pg_ catalog tables which we'll also
    // need to map type names etc.

    let postgres = CString::new("postgres").unwrap();
    let data_dir = CString::new("/home/mildbyte/.pgx/data-12").unwrap();
    let exec_path = CString::new("/home/mildbyte/.pgx/12.11/pgx-install/bin/postgres").unwrap();

    unsafe {
        // Initialize the memory context subsystem
        pg_sys::MemoryContextInit();

        // Infer the correct executable paths
        pg_sys::InitStandaloneProcess(exec_path.as_ptr());

        // Manually set the Mode enum to InitProcessing
        pg_sys::Mode = 1;

        pg_sys::InitializeGUCOptions();
        InitializeTimeouts();

        // Initialize the data directory and the settings. We're connecting to the database
        // "postgres" as the user "postgres", but since we're running in the standalone backend
        // mode, we don't need to care about authentication.
        pg_sys::SelectConfigFiles(data_dir.as_ptr(), postgres.as_ptr());
        pg_sys::ChangeToDataDir();
        pg_sys::CreateDataDirLockFile(false);
        pg_sys::LocalProcessControlFile(true);
        pg_sys::InitializeMaxBackends();

        // Initialize all the transaction management subsystems, shared memory etc
        pg_sys::BaseInit();
        pg_sys::InitProcess();
        pg_sys::InitPostgres(
            postgres.as_ptr(),
            pg_sys::InvalidOid,
            postgres.as_ptr(),
            pg_sys::InvalidOid,
            std::ptr::null_mut(),
            false,
        );
    }
}

unsafe fn parse_type(name: &str) -> Option<(pgx::PgOid, i32)> {
    let mut oid: pg_sys::Oid = 0;
    let mut typmods: i32 = 0;

    pg_sys::parseTypeString(
        CString::new(name).unwrap().into_raw(),
        &mut oid,
        &mut typmods,
        true,
    );

    Some((pgx::PgOid::from(oid), typmods))
}

unsafe fn build_attribute(ordinal: i16, name: &str, type_name: &str) -> FormData_pg_attribute {
    let (oid, typmods) = parse_type(type_name).unwrap();

    let tuple = pg_sys::typeidType(oid.value());
    let pg_type = pg_sys::heap_tuple_get_struct::<pg_sys::FormData_pg_type>(tuple);

    let attribute = FormData_pg_attribute {
        attname: make_name_data(name),
        atttypid: oid.value(),
        attlen: (*pg_type).typlen,
        attnum: ordinal,
        attbyval: (*pg_type).typbyval,
        attalign: (*pg_type).typalign,
        atthasdef: false,
        ..Default::default()
    };

    pg_sys::ReleaseSysCache(tuple);

    attribute
}

unsafe fn schema_to_attributes(schema: &str) -> Vec<FormData_pg_attribute> {
    let json: Vec<(i16, String, String, bool)> =
        serde_json::from_str(schema).expect("JSON was not well-formatted");

    json.iter()
        .map(|(ordinal, name, type_name, _)| build_attribute(*ordinal, name, type_name))
        .collect()
}

unsafe fn do_something() -> () {
    init_pg();
    // We need to pretend that we're in a transaction for this
    pg_sys::StartTransactionCommand();

    let mut attrs: [*mut FormData_pg_attribute] = schema_to_attributes(SCHEMA).iter().map(|s| s.borrow_mut()).collect();

    let tuple_descriptor = CreateTupleDesc(4, attrs.as_mut_ptr());

    let mut column_list = PgList::<pg_sys::Var>::new();
    column_list.push(&mut pg_sys::Var {
        varattno: 3,
        vartype: 3802,
        ..Default::default()
    });

    let file_path = CString::new("/home/mildbyte/pg-bindgen-test/data/o564173e5b42a103f7079e0401d6269e54b5930a9d2144911d3f1db41a3fa1b").unwrap();

    let read_state = CStoreBeginRead(
        file_path.as_ptr(),
        tuple_descriptor,
        column_list.into_pg(),
        std::ptr::null_mut(),
    );

    let mut column_values = vec![0; 4];
    let mut column_nulls = vec![false; 4];

    while CStoreReadNextRow(
        read_state,
        column_values.as_mut_ptr(),
        column_nulls.as_mut_ptr(),
    ) {
        let date = pgx::JsonB::from_datum(column_values[2], column_nulls[2], 3802);
        println!("{:?}", date);
    }

    CStoreEndRead(read_state);
    pg_sys::AbortCurrentTransaction();
}

fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod tests {

    use pgx::{
        pg_sys::{self},
        PgBuiltInOids, PgOid,
    };

    use crate::{build_attribute, do_something, init_pg, parse_type, schema_to_attributes};

    #[test]
    fn do_something_smoke() {
        unsafe {
            do_something();
        }
    }

    #[test]
    fn parse_type_basic() {
        unsafe {
            init_pg();
            pg_sys::StartTransactionCommand();
            assert_eq!(
                parse_type("timestamp with time zone"),
                Some((PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID), -1))
            );
            pg_sys::AbortCurrentTransaction();
        }
    }

    #[test]
    fn build_attribute_basic() {
        unsafe {
            init_pg();
            pg_sys::StartTransactionCommand();
            let attribute = build_attribute(1, "col_name", "timestamp with time zone");
            assert_eq!(
                attribute.type_oid(),
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID)
            );
            pg_sys::AbortCurrentTransaction();
        }
    }

    #[test]
    fn schema_to_attributes_basic() {
        unsafe {
            init_pg();
            pg_sys::StartTransactionCommand();
            let schema = r#"[[1, "_airbyte_emitted_at", "timestamp with time zone", true], [2, "_airbyte_ab_id", "character varying", true], [3, "_airbyte_data", "jsonb", false], [4, "sg_ud_flag", "boolean", false]]"#;
            let attributes = schema_to_attributes(schema);

            assert_eq!(attributes.len(), 4);
            assert_eq!(attributes[0].name(), "_airbyte_emitted_at");
            pg_sys::AbortCurrentTransaction();
        }
    }
}
