use std::ffi::CStr;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use pgx::pg_sys::{self, Datum, FmgrInfo, FormData_pg_attribute, InvalidOid, Oid};
use pgx::{FromDatum, Numeric, PgBuiltInOids as O, PgOid};

pub trait DatumAppender {
    fn call(&mut self, datum: Datum, is_null: bool);
    fn finish(&mut self) -> ArrayRef;
}

struct GenericStringAppender {
    builder: Box<StringBuilder>,
    finfo: FmgrInfo,
}

impl GenericStringAppender {
    fn new(finfo: FmgrInfo, capacity: usize) -> Self {
        Self {
            finfo,
            builder: Box::new(StringBuilder::new(capacity)),
        }
    }
}

impl DatumAppender for GenericStringAppender {
    fn call(&mut self, datum: Datum, is_null: bool) {
        if is_null {
            self.builder.append_null().unwrap();
        } else {
            let out_str = unsafe {
                CStr::from_ptr(pg_sys::OutputFunctionCall(&mut self.finfo as *mut _, datum))
            }
            .to_str()
            .unwrap();
            self.builder.append_value(out_str).unwrap();
        }
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

macro_rules! append_datum {
    ($name:tt, $datum_type:ty, $builder_type:ty) => {
        struct $name {
            builder: Box<$builder_type>,
        }
        impl $name {
            fn new(capacity: usize) -> Self {
                Self {
                    builder: Box::new(<$builder_type>::new(capacity)),
                }
            }
        }
        impl DatumAppender for $name {
            fn call(&mut self, datum: Datum, is_null: bool) {
                let mval = unsafe { <$datum_type>::from_datum(datum, is_null, InvalidOid) };
                match mval {
                    None => self.builder.append_null().unwrap(),
                    Some(val) => self.builder.append_value(val).unwrap(),
                }
            }
            fn finish(&mut self) -> ArrayRef {
                Arc::new(self.builder.finish())
            }
        }
    };
}

append_datum!(BoolAppender, bool, BooleanBuilder);
append_datum!(BinaryAppender, &[u8], BinaryBuilder);
append_datum!(Int2Appender, i16, Int16Builder);
append_datum!(Int4Appender, i32, Int32Builder);
append_datum!(Int8Appender, i64, Int64Builder);
append_datum!(Float4Appender, f32, Float32Builder);
append_datum!(Float8Appender, f64, Float64Builder);

// Customized append code (casts, faster conversion by not converting to a Rust
// type first)

struct DateAppender {
    builder: Box<Date32Builder>,
}

impl DateAppender {
    fn new(capacity: usize) -> Self {
        Self {
            builder: Box::new(Date32Builder::new(capacity)),
        }
    }
}

impl DatumAppender for DateAppender {
    fn call(&mut self, datum: Datum, is_null: bool) {
        if is_null {
            self.builder.append_null().unwrap();
        } else {
            self.builder
                .append_value(datum as i32 + pg_sys::POSTGRES_EPOCH_JDATE as i32)
                .unwrap();
        }
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

struct TimeAppender {
    builder: Box<Time64MicrosecondBuilder>,
}

impl TimeAppender {
    fn new(capacity: usize) -> Self {
        Self {
            builder: Box::new(Time64MicrosecondBuilder::new(capacity)),
        }
    }
}

impl DatumAppender for TimeAppender {
    fn call(&mut self, datum: Datum, is_null: bool) {
        if is_null {
            self.builder.append_null().unwrap();
        } else {
            // Datum is pointing to either 8 or 12 bytes (depending on whether we have
            // timezone information). We dereference the pointer and grab the first 8 bytes,
            // treating it as microseconds since epoch.
            self.builder
                .append_value(unsafe { *(datum as *const i64) })
                .unwrap();
        }
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

struct TimestampAppender {
    builder: Box<TimestampMicrosecondBuilder>,
}

impl TimestampAppender {
    fn new(capacity: usize) -> Self {
        Self {
            builder: Box::new(TimestampMicrosecondBuilder::new(capacity)),
        }
    }
}

impl DatumAppender for TimestampAppender {
    fn call(&mut self, datum: Datum, is_null: bool) {
        if is_null {
            self.builder.append_null().unwrap();
        } else {
            // On 32-bit platforms, Datum has a size of 32 bits and, in case of
            // returning 8-byte values like float8/int8 and these timestamps,
            // uses pass-by-reference (the Datum is pointing to the actual 8-byte
            // memory location). To speed this up, assert we're using pass-by-value and
            // just cast the datum.
            assert!(pg_sys::USE_FLOAT8_BYVAL == 1);
            self.builder
                .append_value(
                    datum as i64
                        // Shift to account for PG/Unix "epoch" difference
                        + (pg_sys::POSTGRES_EPOCH_JDATE - pg_sys::UNIX_EPOCH_JDATE) as i64
                            * pg_sys::SECS_PER_DAY as i64
                            * 1000000i64,
                )
                .unwrap();
        }
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

struct NumericAppender {
    builder: Box<Float64Builder>,
}

impl NumericAppender {
    fn new(capacity: usize) -> Self {
        Self {
            builder: Box::new(Float64Builder::new(capacity)),
        }
    }
}

impl DatumAppender for NumericAppender {
    fn call(&mut self, datum: Datum, is_null: bool) {
        if is_null {
            self.builder.append_null().unwrap();
        } else {
            self.builder
                .append_value(
                    unsafe { Numeric::from_datum(datum, is_null, InvalidOid) }
                        .and_then(|n| n.to_string().parse::<f64>().ok())
                        .unwrap(),
                )
                .unwrap();
        }
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

pub fn pg_oid_to_arrow_datatype(oid: PgOid) -> DataType {
    match oid {
        PgOid::InvalidOid => DataType::Null,
        PgOid::Custom(_) => DataType::Utf8,
        PgOid::BuiltIn(builtin) => match builtin {
            // Integers
            O::BOOLOID => DataType::Boolean,
            O::BYTEAOID => DataType::Binary,
            O::CHAROID => DataType::UInt8,
            O::INT2OID => DataType::Int16,
            O::INT4OID => DataType::Int32,
            O::INT8OID => DataType::Int64,

            // Floats
            O::FLOAT4OID => DataType::Float32,
            O::FLOAT8OID => DataType::Float64,
            O::NUMERICOID => DataType::Float64,

            // Types we output as text
            O::NAMEOID | O::TEXTOID | O::JSONBOID | O::JSONOID | O::UUIDOID => DataType::Utf8,

            // Dates/times
            // Postgres stores all dates as 32-bit (signed) numbers of days since 4714-11-24 BC
            O::DATEOID => DataType::Date32,

            // Timezones are all stored as numbers of microseconds since epoch. The timezone information
            // isn't actually stored (timestamptz stores as UTC but converts to session's timezone on SELECT)
            O::TIMESTAMPOID | O::TIMESTAMPTZOID => DataType::Timestamp(TimeUnit::Microsecond, None),

            // Time is stored as 8 bytes of microseconds since midnight + (for TZ) 4 bytes of offset
            // _WEST_ (???) of UTC in seconds. It looks like in the case of TZ, the 8 bytes are still
            // in UTC.
            O::TIMEOID | O::TIMETZOID => DataType::Time64(TimeUnit::Microsecond),

            // Output records as text for now so that we don't have to build an Arrow Record
            O::RECORDOID => DataType::Utf8,

            // DataFusion doesn't support arrays anyway for now, so output those as strings.
            // Same for all other types where we'll use a generic output function.
            _ => DataType::Utf8,
        },
    }
}

fn make_generic_string_appender(oid: u32, capacity: usize) -> Box<dyn DatumAppender> {
    // We allocated this FmgrInfo struct, so we own it, though note:
    //  The caller's CurrentMemoryContext is used as the fn_mcxt of the info
    //  struct; this means that any subsidiary data attached to the info struct
    //  (either by fmgr_info itself, or later on by a function call handler)
    //  will be allocated in that context.  The caller must ensure that this
    //  context is at least as long-lived as the info struct itself.
    let finfo = unsafe {
        let fmgr_oid: *mut Oid = &mut Oid::default();
        let is_varlena: *mut bool = &mut false;
        pg_sys::getTypeOutputInfo(oid, fmgr_oid, is_varlena);
        let finfo: *mut pg_sys::FmgrInfo = &mut FmgrInfo::default();
        pg_sys::fmgr_info(*fmgr_oid, finfo);
        *finfo
    };

    Box::new(GenericStringAppender::new(finfo, capacity))
}

pub fn attr_to_appender_builder(
    attr: &FormData_pg_attribute,
    capacity: usize,
) -> Box<dyn DatumAppender> {
    match attr.type_oid() {
        PgOid::InvalidOid => unimplemented!(),
        PgOid::BuiltIn(builtin) => match builtin {
            // Integers
            O::BOOLOID => Box::new(BoolAppender::new(capacity)),
            O::BYTEAOID => Box::new(BinaryAppender::new(capacity)),
            O::INT2OID => Box::new(Int2Appender::new(capacity)),
            O::INT4OID => Box::new(Int4Appender::new(capacity)),
            O::INT8OID => Box::new(Int8Appender::new(capacity)),
            O::FLOAT4OID => Box::new(Float4Appender::new(capacity)),
            O::FLOAT8OID => Box::new(Float8Appender::new(capacity)),
            O::NUMERICOID => Box::new(NumericAppender::new(capacity)),
            O::DATEOID => Box::new(DateAppender::new(capacity)),
            O::TIMESTAMPOID | O::TIMESTAMPTZOID => Box::new(TimestampAppender::new(capacity)),
            O::TIMEOID | O::TIMETZOID => Box::new(TimeAppender::new(capacity)),
            _ => make_generic_string_appender(builtin.value(), capacity),
        },
        PgOid::Custom(o) => make_generic_string_appender(o, capacity),
    }
}
