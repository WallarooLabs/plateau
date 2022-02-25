use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Retention {
    pub max_segment_count: Option<usize>,
    pub max_bytes: usize,
}

impl Default for Retention {
    fn default() -> Self {
        Retention {
            max_segment_count: Some(10000),
            max_bytes: 1 * 1024 * 1024 * 1024,
        }
    }
}
