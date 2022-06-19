use lazy_static::lazy_static;
use pgx::pg_sys;
use std::{
    ffi::CString,
    sync::{LockResult, Mutex, MutexGuard},
};

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

pub fn make_name_data(name: &str) -> pg_sys::NameData {
    let mut inner = [0; 64];
    inner[..name.len()]
        .copy_from_slice(unsafe { &*(name.as_bytes() as *const [u8] as *const [i8]) });
    pg_sys::NameData { data: inner }
}

extern "C" {
    fn InitializeTimeouts();
}

pub unsafe fn init_pg() -> () {
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

        pg_sys::StartTransactionCommand();

        // Technically we should wrap PG operations in StartTransactionCommand/AbortCurrentTransaction
        // to be friendly to the internal state. But we're not actually accepting connections here
        // or doing anything other than scanning through internal catalog tables for operators/types.
        // So, we're effectively running a really long transaction that never ends. This is simpler
        // to organize than figuring out how (and at what granularity) to wrap code that calls into PG.

        // TODO: figure out memory contexts and shared locking
        pg_sys::AllocSetContextCreateExtended(
            PgMemoryContexts::CurrentMemoryContext.value(),
            "Execution context".as_pg_cstr(),
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            (pg_sys::ALLOCSET_DEFAULT_MAXSIZE * 16) as usize,
        );
    }
}

pub fn parse_type(name: &str) -> (pgx::PgOid, i32) {
    let mut oid: pg_sys::Oid = 0;
    let mut typmods: i32 = 0;

    unsafe {
        pg_sys::parseTypeString(
            CString::new(name).unwrap().into_raw(),
            &mut oid,
            &mut typmods,
            true,
        );
    }

    (pgx::PgOid::from(oid), typmods)
}

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
pub fn build_attribute(ordinal: i16, name: &str, type_name: &str) -> pg_sys::FormData_pg_attribute {
    let (oid, typmods) = parse_type(type_name);

    unsafe {
        let tuple = pg_sys::typeidType(oid.value());
        let pg_type = pg_sys::heap_tuple_get_struct::<pg_sys::FormData_pg_type>(tuple);

        let attribute = pg_sys::FormData_pg_attribute {
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
}

pub fn create_tuple_desc(
    attributes: &Vec<pg_sys::FormData_pg_attribute>,
) -> pgx::PgBox<pg_sys::TupleDescData, pgx::AllocatedByPostgres> {
    let mut attrs = attributes
        .iter()
        .map(|s| s as *const _ as *mut pg_sys::FormData_pg_attribute)
        .collect::<Vec<*mut pg_sys::FormData_pg_attribute>>();

    unsafe {
        pgx::PgBox::from_pg(pg_sys::CreateTupleDesc(
            attrs.len().try_into().unwrap(),
            attrs.as_mut_ptr(),
        ))
    }
}

#[cfg(test)]
mod tests {

    use pgx::{
        pg_sys::{self},
        PgBuiltInOids, PgOid,
    };

    use crate::postgres::{build_attribute, init_pg};

    #[test]
    fn parse_type_basic() {
        unsafe {
            init_pg();
            assert_eq!(
                crate::postgres::parse_type("timestamp with time zone"),
                (PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID), -1)
            );
        }
    }

    #[test]
    fn build_attribute_basic() {
        unsafe {
            init_pg();
            let attribute = build_attribute(1, "col_name", "timestamp with time zone");
            assert_eq!(
                attribute.type_oid(),
                PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID)
            );
        }
    }
}
