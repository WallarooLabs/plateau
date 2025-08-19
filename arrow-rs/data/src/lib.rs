//! Tools and helpers for working with ordered and limited chunks, and raw
//! storage wrappers for persisting these chunks to disk.
pub mod chunk;
pub mod compatible;
pub use compatible as compat;
pub mod index;
pub mod limit;
pub mod records;
pub mod segment;

// Use the arrow-rs versions of plateau crates
pub use plateau_client_arrow_rs as client;
// Import Arrow modules
pub use arrow;
pub use arrow_array;
pub use arrow_buffer;
pub use arrow_cast;
pub use arrow_data;
pub use arrow_ipc;
pub use arrow_json;
pub use arrow_schema;
pub use arrow_select;
pub use plateau_transport_arrow_rs as transport;

// Adding explicit type re-exports for arrow crates to make migration easier
pub use arrow_array::Array;
pub use arrow_array::ArrayRef;
pub use arrow_array::RecordBatch;
pub use arrow_schema::DataType;
pub use arrow_schema::Field;
pub use arrow_schema::Fields;
pub use arrow_schema::Schema;
pub use arrow_schema::SchemaRef;

#[cfg(test)]
pub use plateau_test_arrow_rs as test;

pub use chunk::IndexedChunk;
pub use index::{Ordering, RecordIndex};
pub use limit::{LimitedBatch, Retention};

pub const DEFAULT_BYTE_LIMIT: usize = 10240000;
