use anyhow::Result;
use async_trait::async_trait;
use plateau_client::{ArrowIterationReply, Iterate, TopicIterationQuery, TopicIterationStatus};
use sample_std::Random;
use serde::Deserialize;

use crate::{BoxedTask, Config, Task, TaskBuilder, WorkerConfig};

#[derive(Deserialize, Debug, Clone)]
pub struct IteratorJob {
    pub topic: String,
    pub page_size: usize,
    #[serde(flatten)]
    pub worker_config: WorkerConfig,
}

impl TaskBuilder for IteratorJob {
    fn to_jobs(&self, _: &mut Random) -> Result<Vec<BoxedTask>> {
        Ok(vec![Box::new(Iterator {
            topic: self.topic.clone(),
            page_size: self.page_size,
            count: 0,
            state: Default::default(),
        })])
    }

    fn config(&self) -> &WorkerConfig {
        &self.worker_config
    }
}

struct Iterator {
    topic: String,
    page_size: usize,

    state: Option<TopicIterationStatus>,
    count: usize,
}

#[async_trait]
impl Task for Iterator {
    async fn run(&mut self, config: &Config) -> (usize, String) {
        let result = config
            .client
            .iterate_topic(
                &self.topic,
                &TopicIterationQuery {
                    page_size: Some(self.page_size),
                    ..Default::default()
                },
                self.state.as_ref().map(|status| &status.next),
            )
            .await;

        let data: ArrowIterationReply = result.unwrap();
        let rows = data.chunks.len();

        self.count += rows;
        if let Some(status) = data.status {
            self.state.replace(status);
        }

        (
            rows,
            format!("[iter]  {} - read {}", self.topic, self.count),
        )
    }
}
