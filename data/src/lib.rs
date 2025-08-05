pub mod chunk;
pub mod compatible;
pub use compatible as compat;
pub mod index;
pub mod limit;
pub mod segment;

pub use plateau_client as client;
pub use plateau_client::arrow2;
pub use plateau_transport as transport;

pub use index::{Ordering, RecordIndex};
pub use limit::Retention;

pub const DEFAULT_BYTE_LIMIT: usize = 10240000;
