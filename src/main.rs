#![feature(extern_types)]

use std::ffi::CString;

use cstore_bindings::{CStoreBeginRead, CStoreEndRead, CStoreReadNextRow};
use pgx::{
    pg_sys::{self, CreateTupleDesc, FormData_pg_attribute, NameData},
    PgList,
};

mod cstore_bindings;

use lazy_static::lazy_static;
use std::sync::{LockResult, Mutex, MutexGuard};

lazy_static! {
    static ref PG_INTERNALS_LOCK: Mutex<()> = Mutex::new(());
    static ref ONETIME_SETUP: () = {
        unsafe {
            pg_sys::MemoryContextInit();
        }
    };
}

// Build pg with
// CFLAGS="-fPIC -ggdb -Og -g3 -fno-omit-frame-pointer" LDFLAGS_EX="--shared" ./configure --enable-cassert --enable-debug
// make
// cp src/backend/postgres $PROJECT_DIR/lib/libpostgres.so

// [[1, "_airbyte_emitted_at", "timestamp with time zone", true], [2, "_airbyte_ab_id", "character varying", true], [3, "_airbyte_data", "jsonb", false], [4, "sg_ud_flag", "boolean", false]]
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

#[repr(C)]
pub struct TransactionStateData {
    fullTransactionId: pg_sys::FullTransactionId,
    subTransactionId: pg_sys::SubTransactionId,
    name: usize,
    savepointLevel: ::std::os::raw::c_int,
    // TODO C enums aren't always 1 byte/1 int large, they
    // seem to change based on the compiler
    state: u8, // There's more stuff here but we don't care about it
}

extern "C" {
    pub static mut CurrentTransactionState: *mut TransactionStateData;
}

fn do_something() -> () {
    unsafe {
        // Pretend to basically be a standalone Postgres backend (running with --single)
        pg_sys::MemoryContextInit();

        let progname = CString::new("postgres").unwrap();
        let user_d_option = CString::new("/home/mildbyte/.pgx/data-12").unwrap();
        let exec_path: CString =
            CString::new("/home/mildbyte/.pgx/12.11/pgx-install/bin/postgres").unwrap();
        let dbname: CString = CString::new("postgres").unwrap();
        pg_sys::IsUnderPostmaster = false;

        pg_sys::InitStandaloneProcess(exec_path.as_ptr());
        pg_sys::Mode = 1; // InitProcessing;
        pg_sys::InitializeGUCOptions();
        // TODO get this one to stop complaining
        pg_sys::InitializeTimeouts();

        // pg_sys::find_my_exec(exec_path.as_ptr(), pg_sys::my_exec_path.as_mut_ptr());
        pg_sys::SelectConfigFiles(user_d_option.as_ptr(), progname.as_ptr());
        pg_sys::ChangeToDataDir();
        pg_sys::CreateDataDirLockFile(false);
        pg_sys::LocalProcessControlFile(true);
        pg_sys::InitializeMaxBackends();

        pg_sys::BaseInit();
        pg_sys::InitProcess();
        pg_sys::InitPostgres(
            dbname.as_ptr(),
            pg_sys::InvalidOid,
            dbname.as_ptr(),
            pg_sys::InvalidOid,
            std::ptr::null_mut(),
            false,
        );

        // // Catalog cache (cstore calls GetDefaultOpClass)
        // pg_sys::InitCatalogCache();

        // // Fake us being in a transaction
        // (*CurrentTransactionState).state = 2; // TRANS_INPROGRESS

        // // AAAAA

        // pg_sys::CreateAuxProcessResourceOwner();
        // pg_sys::StartupXLOG();
        // pg_sys::ReleaseAuxProcessResources(true);
        // pg_sys::CurrentResourceOwner = std::ptr::null_mut();
        // pg_sys::StartTransactionCommand();
        // TODO: actually load ./src/backend/catalog/postgres.bki here?
        let mut attrs = [
            &mut FormData_pg_attribute {
                attname: make_name_data("_airbyte_emitted_at"),
                atttypid: 1184,
                attlen: 8,
                attnum: 1,
                attbyval: true,
                attalign: 'd' as i8,
                atthasdef: false,
                ..Default::default()
            } as _,
            &mut FormData_pg_attribute {
                attname: make_name_data("_airbyte_ab_id"),
                atttypid: 1043,
                attlen: -1,
                attnum: 2,
                attbyval: false,
                attalign: 'i' as i8,
                atthasdef: false,
                ..Default::default()
            } as _,
            &mut FormData_pg_attribute {
                attname: make_name_data("_airbyte_data"),
                atttypid: 3802,
                attlen: -1,
                attnum: 3,
                attbyval: false,
                attalign: 'i' as i8,
                atthasdef: false,
                ..Default::default()
            } as _,
            &mut FormData_pg_attribute {
                attname: make_name_data("sg_ud_flag"),
                atttypid: 16,
                attlen: 1,
                attnum: 4,
                attbyval: true,
                attalign: 'c' as i8,
                atthasdef: false,
                ..Default::default()
            } as _,
        ];

        let tuple_descriptor = CreateTupleDesc(4, attrs.as_mut_ptr());

        let mut column_list = PgList::<pg_sys::Var>::new();
        column_list.push(&mut pg_sys::Var {
            varattno: 1,
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

        let more = CStoreReadNextRow(
            read_state,
            column_values.as_mut_ptr(),
            column_nulls.as_mut_ptr(),
        );

        CStoreEndRead(read_state);
    }
}

fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod tests {
    use crate::do_something;

    #[test]
    fn do_something_smoke() {
        do_something();
    }
}
