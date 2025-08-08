//! Tools and helpers for working with ordered and limited chunks, and raw
//! storage wrappers for persisting these chunks to disk.
pub mod chunk;
pub mod compatible;
pub use compatible as compat;
pub mod index;
pub mod limit;
pub mod records;
pub mod segment;

pub use plateau_client as client;
pub use plateau_client::arrow2;
pub use plateau_transport as transport;

#[cfg(test)]
pub use plateau_test as test;

pub use chunk::IndexedChunk;
pub use index::{Ordering, RecordIndex};
pub use limit::{LimitedBatch, Retention};

pub const DEFAULT_BYTE_LIMIT: usize = 10240000;
