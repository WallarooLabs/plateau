use std::ops::Range;

use anyhow::Result;
use async_trait::async_trait;
use sample_std::Random;
use serde::Deserialize;

use crate::{BoxedTask, Config, Task, TaskBuilder, WorkerConfig};

#[derive(Deserialize, Debug, Clone)]
pub struct SummarizerJob {
    pub topic: String,
    #[serde(flatten)]
    pub worker_config: WorkerConfig,
}

impl TaskBuilder for SummarizerJob {
    fn to_jobs(&self, _: &mut Random) -> Result<Vec<BoxedTask>> {
        Ok(vec![Box::new(Summarizer {
            topic: self.topic.clone(),
            count: 0,
        })])
    }

    fn config(&self) -> &WorkerConfig {
        &self.worker_config
    }
}

struct Summarizer {
    topic: String,
    count: usize,
}

#[async_trait]
impl Task for Summarizer {
    async fn run(&mut self, config: &Config) -> (usize, String) {
        self.count += 1;
        let partitions = config
            .client
            .get_partitions(&self.topic)
            .await
            .unwrap()
            .partitions;

        let count: usize = partitions
            .values()
            .map(|span| Range::from(*span).len())
            .sum();

        (1, format!("[sum]   {} - {}", self.topic, count))
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct HealthCheckJob {
    #[serde(flatten)]
    pub worker_config: WorkerConfig,
}

impl TaskBuilder for HealthCheckJob {
    fn to_jobs(&self, _: &mut Random) -> Result<Vec<BoxedTask>> {
        Ok(vec![Box::new(HealthCheck {
            count: 0,
            errors: 0,
        })])
    }

    fn config(&self) -> &WorkerConfig {
        &self.worker_config
    }
}

struct HealthCheck {
    count: usize,
    errors: usize,
}

#[async_trait]
impl Task for HealthCheck {
    async fn run(&mut self, config: &Config) -> (usize, String) {
        let result = config.client.healthcheck().await;
        if result.is_ok() {
            self.count += 1;
        } else {
            self.errors += 1;
        }

        if self.errors == 0 {
            (1, format!("[ok]    ✓ {:?}", result))
        } else {
            (0, format!("[ok]    ✗ {} {:?}", self.errors, result))
        }
    }
}
