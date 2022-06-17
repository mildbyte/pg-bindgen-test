// Generated with bindgen and manually altered to work with pgx's
// PG definitions rather than the bindgen-generated ones (since they're)
// nicer to work with from Rust, let users unpack lists etc
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]

use pgx::pg_sys;

// CStore structs

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct TableReadState {
    pub tableFile: *mut pg_sys::FILE,
    pub tableFooter: *mut TableFooter,
    pub tupleDescriptor: pg_sys::TupleDesc,
    pub projectedColumnList: *mut pg_sys::List,
    pub whereClauseList: *mut pg_sys::List,
    pub stripeReadContext: pg_sys::MemoryContext,
    pub stripeBuffers: *mut StripeBuffers,
    pub readStripeCount: pg_sys::uint32,
    pub stripeReadRowCount: pg_sys::uint64,
    pub blockDataArray: *mut *mut ColumnBlockData,
    pub deserializedBlockIndex: pg_sys::int32,
}
pub const CompressionType_COMPRESSION_TYPE_INVALID: CompressionType = -1;
pub const CompressionType_COMPRESSION_NONE: CompressionType = 0;
pub const CompressionType_COMPRESSION_PG_LZ: CompressionType = 1;
pub const CompressionType_COMPRESSION_COUNT: CompressionType = 2;
pub type CompressionType = ::std::os::raw::c_int;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct TableFooter {
    pub stripeMetadataList: *mut pg_sys::List,
    pub blockRowCount: pg_sys::uint64,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct StripeBuffers {
    pub columnCount: pg_sys::uint32,
    pub rowCount: pg_sys::uint32,
    pub columnBuffersArray: *mut *mut ColumnBuffers,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ColumnBuffers {
    pub blockBuffersArray: *mut *mut ColumnBlockBuffers,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ColumnBlockBuffers {
    pub existsBuffer: pg_sys::StringInfo,
    pub valueBuffer: pg_sys::StringInfo,
    pub valueCompressionType: CompressionType,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ColumnBlockData {
    pub existsArray: *mut bool,
    pub valueArray: *mut pg_sys::Datum,
    pub valueBuffer: pg_sys::StringInfo,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ColumnBlockSkipNode {
    pub hasMinMax: bool,
    pub minimumValue: pg_sys::Datum,
    pub maximumValue: pg_sys::Datum,
    pub rowCount: pg_sys::uint64,
    pub valueBlockOffset: pg_sys::uint64,
    pub valueLength: pg_sys::uint64,
    pub existsBlockOffset: pg_sys::uint64,
    pub existsLength: pg_sys::uint64,
    pub valueCompressionType: CompressionType,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct CStoreFdwOptions {
    pub filename: *mut ::std::os::raw::c_char,
    pub compressionType: CompressionType,
    pub stripeRowCount: pg_sys::uint64,
    pub blockRowCount: pg_sys::uint32,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct TableWriteState {
    pub tableFile: *mut pg_sys::FILE,
    pub tableFooter: *mut TableFooter,
    pub tableFooterFilename: pg_sys::StringInfo,
    pub compressionType: CompressionType,
    pub tupleDescriptor: pg_sys::TupleDesc,
    pub comparisonFunctionArray: *mut *mut pg_sys::FmgrInfo,
    pub currentFileOffset: pg_sys::uint64,
    pub relation: pg_sys::Relation,
    pub stripeWriteContext: pg_sys::MemoryContext,
    pub stripeBuffers: *mut StripeBuffers,
    pub stripeSkipList: *mut StripeSkipList,
    pub stripeMaxRowCount: pg_sys::uint32,
    pub blockDataArray: *mut *mut ColumnBlockData,
    pub compressionBuffer: pg_sys::StringInfo,
}
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct StripeSkipList {
    pub blockSkipNodeArray: *mut *mut ColumnBlockSkipNode,
    pub columnCount: pg_sys::uint32,
    pub blockCount: pg_sys::uint32,
}

// CStore functions

extern "C" {
    pub fn CStoreBeginWrite(
        filename: *const ::std::os::raw::c_char,
        compressionType: CompressionType,
        stripeMaxRowCount: pg_sys::uint64,
        blockRowCount: pg_sys::uint32,
        tupleDescriptor: pg_sys::TupleDesc,
    ) -> *mut TableWriteState;
}
extern "C" {
    pub fn CStoreWriteRow(
        state: *mut TableWriteState,
        columnValues: *mut pg_sys::Datum,
        columnNulls: *mut bool,
    );
}
extern "C" {
    pub fn CStoreEndWrite(state: *mut TableWriteState);
}
extern "C" {
    pub fn CStoreBeginRead(
        filename: *const ::std::os::raw::c_char,
        tupleDescriptor: pg_sys::TupleDesc,
        projectedColumnList: *mut pg_sys::List,
        qualConditions: *mut pg_sys::List,
    ) -> *mut TableReadState;
}
extern "C" {
    pub fn CStoreReadFooter(tableFooterFilename: pg_sys::StringInfo) -> *mut TableFooter;
}
extern "C" {
    pub fn CStoreReadFinished(state: *mut TableReadState) -> bool;
}
extern "C" {
    pub fn CStoreReadNextRow(
        state: *mut TableReadState,
        columnValues: *mut pg_sys::Datum,
        columnNulls: *mut bool,
    ) -> bool;
}
extern "C" {
    pub fn CStoreEndRead(state: *mut TableReadState);
}
extern "C" {
    pub fn CStoreTableRowCount(filename: *const ::std::os::raw::c_char) -> pg_sys::uint64;
}
extern "C" {
    pub fn CStoreGetOptions(foreignTableId: pg_sys::Oid) -> *mut CStoreFdwOptions;
}
