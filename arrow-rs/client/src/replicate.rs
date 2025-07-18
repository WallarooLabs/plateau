//! Prototype for replicating data between various plateau hosts.
//!
//! [`Replicate`] is [`Serialize`] and [`Deserialize`], and can be read from a
//! config.
//!
//! This config can then be parsed via [`ReplicationWorker::from_replicate`],
//! which returns a [`ReplicationWorker`]. [`ReplicationWorker::pump`] can then
//! be used to advance the worker.
//!
//! You'll probably want to just fire and forget this worker in a
//! [`tokio::task`] via [`ReplicationWorker::run_forever`].
use std::collections::{BTreeMap, HashMap};
use std::fmt;

use plateau_transport_arrow_rs::{MultiChunk, PartitionId, RecordQuery};
use serde::{Deserialize, Serialize};

use crate::{Client, Error, Retrieve};

use tracing::{debug, info, warn};

#[cfg(feature = "replicate")]
use tracing::error;

#[cfg(feature = "replicate")]
use std::time::Duration;

#[cfg(feature = "replicate")]
pub use backoff::ExponentialBackoff;

#[derive(Debug, Clone)]
struct ClientPartition {
    host_id: String,
    id: PartitionId,
    client: Client,
}

impl fmt::Display for ClientPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_args!("{}/{}", self.host_id, self.id).fmt(f)
    }
}

#[derive(Clone, Debug)]
struct ReplicatePartitionJob {
    source: ClientPartition,
    target: ClientPartition,
    page_size: Option<usize>,
    record_ix: usize,
}

type ReplicatePartitionJobKey = (HostPartition, HostPartition);

impl ReplicatePartitionJob {
    async fn begin(
        source: ClientPartition,
        target: ClientPartition,
        page_size: Option<usize>,
    ) -> Result<Self, Error> {
        let map = target.client.get_partitions(target.id.topic()).await?;
        let record_ix = map
            .partitions
            .get(target.id.partition())
            .map_or(0, |span| span.end);

        Ok(Self {
            source,
            target,
            page_size,
            record_ix,
        })
    }

    async fn page(&mut self) -> Result<bool, Error> {
        let query = RecordQuery {
            start: self.record_ix,
            page_size: self.page_size,
            ..RecordQuery::default()
        };

        let mut next_page: MultiChunk = self
            .source
            .client
            .get_records(self.source.id.topic(), self.source.id.partition(), &query)
            .await?;

        if !next_page.is_empty() {
            // In arrow-rs, we can't modify schema metadata directly since it's in an Arc
            // Instead, we'll create a new schema with the same fields but without the metadata entries
            let new_schema = plateau_transport_arrow_rs::arrow_schema::Schema::new(
                next_page.schema.fields().clone(),
            );
            let new_schema = std::sync::Arc::new(new_schema);
            next_page.schema = new_schema;

            let insert = self
                .target
                .client
                .append_queue(
                    self.target.id.topic(),
                    self.target.id.partition(),
                    next_page,
                )
                .await?;

            if let Some(insert) = insert {
                debug!(
                    "{} => {}: {} => {}",
                    self.source, self.target, self.record_ix, insert.span.end
                );

                self.record_ix = insert.span.end;
            } else {
                warn!("no insert performed")
            }

            Ok(false)
        } else {
            debug!(
                "{} => {}: up to date at {}",
                self.source, self.target, self.record_ix
            );

            Ok(true)
        }
    }
}

#[derive(Clone, Debug)]
struct ClientTopic {
    host_id: String,
    topic: String,
    client: Client,
}

impl ClientTopic {
    fn to_client_partition(&self, partition: &str) -> ClientPartition {
        ClientPartition {
            host_id: self.host_id.clone(),
            id: PartitionId {
                topic: self.topic.clone(),
                partition: partition.to_string(),
            },
            client: self.client.clone(),
        }
    }
}

impl fmt::Display for ClientTopic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_args!("{}/{}", self.host_id, self.topic).fmt(f)
    }
}

#[derive(Clone, Debug)]
pub struct ReplicateTopicJob {
    source: ClientTopic,
    target: ClientTopic,
    page_size: Option<usize>,
    pending: BTreeMap<String, ReplicatePartitionJob>,
    done: BTreeMap<String, ReplicatePartitionJob>,
}

type ReplicateTopicJobKey = (HostTopic, HostTopic);

impl ReplicateTopicJob {
    async fn begin(
        source: ClientTopic,
        target: ClientTopic,
        page_size: Option<usize>,
    ) -> Result<Self, Error> {
        let job = Self {
            source,
            target,
            page_size,
            pending: Default::default(),
            done: Default::default(),
        };

        Ok(job)
    }

    async fn sync(&mut self) -> Result<bool, Error> {
        let source = self
            .source
            .client
            .get_partitions(&self.source.topic)
            .await?;

        let target = self
            .target
            .client
            .get_partitions(&self.target.topic)
            .await?;

        let mut new_partition = false;
        for (partition, _) in source.partitions {
            if !self.pending.contains_key(&partition) && !self.done.contains_key(&partition) {
                self.pending.insert(
                    partition.clone(),
                    ReplicatePartitionJob {
                        source: self.source.to_client_partition(&partition),
                        target: self.target.to_client_partition(&partition),
                        page_size: self.page_size,
                        record_ix: target.partitions.get(&partition).map_or(0, |s| s.end),
                    },
                );
                new_partition = true;
            }
        }

        Ok(new_partition)
    }

    async fn page(&mut self) -> Result<bool, Error> {
        if let Some((key, mut job)) = self.pending.pop_first() {
            if !job.page().await? {
                self.pending.insert(key, job);
                Ok(false)
            } else {
                self.done.insert(key, job);
                Ok(false)
            }
        } else {
            let new_partitions = self.sync().await?;
            if !new_partitions {
                self.pending = std::mem::take(&mut self.done);
            }
            Ok(!new_partitions)
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub parallel: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self { parallel: 8 }
    }
}

/// Driver for a number of replication jobs configured via [`Replicate`].
#[derive(Clone, Debug)]
pub struct ReplicationWorker {
    config: Config,
    hosts: HashMap<String, (String, Client)>,
    topics: HashMap<ReplicateTopicJobKey, ReplicateTopicJob>,
    partitions: HashMap<ReplicatePartitionJobKey, ReplicatePartitionJob>,
}

impl ReplicationWorker {
    /// Build a worker from a user [`Replicate`] config.
    pub async fn from_replicate(replicate: Replicate) -> Result<Self, Error> {
        use futures::stream::{self, StreamExt, TryStreamExt};

        let hosts: HashMap<_, _> = replicate
            .hosts
            .into_iter()
            .map(|host| Ok((host.id.clone(), (host.url.clone(), Client::new(&host.url)?))))
            .collect::<Result<_, Error>>()?;

        let hosts_ref = &hosts;

        let partitions = stream::iter(replicate.partitions.into_iter())
            .flat_map(move |part| stream::once(part.into_job(hosts_ref)))
            .try_collect()
            .await?;

        let topics = stream::iter(replicate.topics.into_iter())
            .flat_map(move |topic| stream::once(topic.into_job(hosts_ref)))
            .try_collect()
            .await?;

        for (id, (url, _)) in &hosts {
            info!("{} url: {}", id, url);
        }

        Ok(Self {
            config: replicate.config,
            hosts,
            topics,
            partitions,
        })
    }

    async fn page_all(&mut self) -> Result<bool, Error> {
        use futures::future::FutureExt;
        use futures::stream::{FuturesUnordered, StreamExt};

        let mut done = true;
        let mut futures = FuturesUnordered::new();
        for job in self.topics.values_mut() {
            if futures.len() >= self.config.parallel {
                done = done && futures.next().await.unwrap_or(Ok(true))?;
            }
            futures.push(job.page().boxed());
        }

        for job in self.partitions.values_mut() {
            if futures.len() >= self.config.parallel {
                done = done && futures.next().await.unwrap_or(Ok(true))?;
            }
            futures.push(job.page().boxed());
        }

        while let Some(job_status) = futures.next().await {
            done = done && job_status?
        }

        Ok(done)
    }

    fn all_jobs(&self) -> impl Iterator<Item = &ReplicatePartitionJob> {
        self.topics
            .values()
            .flat_map(|topic| topic.pending.values().chain(topic.done.values()))
            .chain(self.partitions.values())
    }

    /// Proceed through all configured jobs and copy all present records from
    /// the `source` to the `target`.
    ///
    /// Runs until an error is encountered, or all jobs report that their
    /// `target` is in sync with the `source`. Completion reporting is
    /// "best-effort". More records may be written after a job reports complete
    /// and before `pump` exits.
    pub async fn pump(&mut self) -> Result<(), Error> {
        for job in self.all_jobs() {
            info!(
                "start: {} => {} @ {}",
                job.source, job.target, job.record_ix
            )
        }

        while !self.page_all().await? {}

        for job in self.all_jobs() {
            info!("end: {} => {} @ {}", job.source, job.target, job.record_ix)
        }

        Ok(())
    }

    /// Add a new partition job to the set of existing jobs.
    pub async fn add_partition(&mut self, partition: ReplicatePartition) -> Result<bool, Error> {
        if self.partitions.contains_key(&partition.key()) {
            return Ok(false);
        }

        let (key, job) = partition.into_job(&self.hosts).await?;
        self.partitions.insert(key, job);

        Ok(true)
    }

    /// Run [`Self::pump`] forever, using an [`ExponentialBackoff`] to retry on
    /// errors.
    ///
    /// Unlike a typical [`backoff`] operation, we never give up. If
    /// [`ExponentialBackoff::next_backoff`] hits its configured max elapsed
    /// limit, we retry with the last used interval indefinitely.
    #[cfg(feature = "replicate")]
    pub async fn run_forever(
        mut self,
        period: Duration,
        mut backoff: ExponentialBackoff,
    ) -> Result<(), Error> {
        use backoff::backoff::Backoff;

        let mut last_duration = backoff.next_backoff().unwrap();
        loop {
            match self.pump().await {
                Ok(_) => {
                    backoff.reset();
                    tokio::time::sleep(period).await;
                }
                Err(e) => {
                    error!("error in loop: {:?}", e);
                    let next = backoff.next_backoff();
                    info!("waiting {:.1?} to retry", last_duration);
                    tokio::time::sleep(last_duration).await;
                    last_duration = next.unwrap_or(last_duration)
                }
            }
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Replicate {
    #[serde(default)]
    pub config: Config,
    pub hosts: Vec<ReplicateHost>,
    pub topics: Vec<ReplicateTopic>,
    pub partitions: Vec<ReplicatePartition>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ReplicateHost {
    pub id: String,
    pub url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicateTopic {
    pub source: HostTopic,
    pub target: HostTopic,
    #[serde(default)]
    pub page_size: Option<usize>,
}

impl ReplicateTopic {
    async fn into_job(
        self,
        clients: &HashMap<String, (String, Client)>,
    ) -> Result<(ReplicateTopicJobKey, ReplicateTopicJob), Error> {
        let source = self.source.to_client_topic(clients)?;
        let target = self.target.to_client_topic(clients)?;

        Ok((
            self.key(),
            ReplicateTopicJob::begin(source, target, self.page_size).await?,
        ))
    }

    pub fn key(&self) -> ReplicateTopicJobKey {
        (self.source.clone(), self.target.clone())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicatePartition {
    pub source: HostPartition,
    pub target: HostPartition,
    #[serde(default)]
    pub page_size: Option<usize>,
}

impl ReplicatePartition {
    async fn into_job(
        self,
        clients: &HashMap<String, (String, Client)>,
    ) -> Result<(ReplicatePartitionJobKey, ReplicatePartitionJob), Error> {
        let source = self.source.to_client_partition(clients)?;
        let target = self.target.to_client_partition(clients)?;

        Ok((
            self.key(),
            ReplicatePartitionJob::begin(source, target, self.page_size).await?,
        ))
    }

    pub fn key(&self) -> ReplicatePartitionJobKey {
        (self.source.clone(), self.target.clone())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct HostPartition {
    pub host_id: String,
    pub partition_id: PartitionId,
}

impl HostPartition {
    fn to_client_partition(
        &self,
        clients: &HashMap<String, (String, Client)>,
    ) -> Result<ClientPartition, Error> {
        let (_, client) = clients
            .get(&self.host_id)
            .ok_or_else(|| Error::Config(format!("invalid host id: {}", self.host_id)))?
            .clone();

        Ok(ClientPartition {
            host_id: self.host_id.clone(),
            id: self.partition_id.clone(),
            client,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct HostTopic {
    pub host_id: String,
    pub topic: String,
}

impl HostTopic {
    fn to_client_topic(
        &self,
        clients: &HashMap<String, (String, Client)>,
    ) -> Result<ClientTopic, Error> {
        let (_, client) = clients
            .get(&self.host_id)
            .ok_or_else(|| Error::Config(format!("invalid host id: {}", self.host_id)))?
            .clone();

        Ok(ClientTopic {
            host_id: self.host_id.clone(),
            topic: self.topic.clone(),
            client,
        })
    }
}

#[cfg(test)]
pub mod test {
    use std::net::SocketAddr;

    use anyhow::Result;
    use tracing_subscriber::{fmt, EnvFilter};

    use super::*;
    use plateau_server::{config::PlateauConfig, http};
    use plateau_transport_arrow_rs::{SchemaChunk, Span};

    use plateau_transport_arrow_rs::arrow_array::RecordBatch;
    use plateau_transport_arrow_rs::arrow_schema::{ArrowError, Schema};
    use std::sync::Arc;

    trait ToBytes {
        fn to_bytes(&self) -> Result<Vec<u8>, ArrowError>;
    }

    // Implement ToBytes trait for SchemaChunk<Arc<Schema>>
    impl ToBytes for SchemaChunk<Arc<Schema>> {
        fn to_bytes(&self) -> Result<Vec<u8>, ArrowError> {
            let mut buf = Vec::new();
            let options = plateau_transport_arrow_rs::arrow_ipc::writer::IpcWriteOptions::default();

            let mut writer =
                plateau_transport_arrow_rs::arrow_ipc::writer::FileWriter::try_new_with_options(
                    &mut buf,
                    &self.schema,
                    options,
                )?;

            writer.write(&self.chunk)?;
            writer.finish()?;

            Ok(buf)
        }
    }

    pub(crate) fn inferences_schema_b() -> SchemaChunk<Arc<Schema>> {
        use plateau_transport_arrow_rs::{
            arrow_array::{Float32Array, Int64Array, ListArray, StringArray},
            arrow_schema::{DataType, Field, Schema},
        };

        let time = Arc::new(Int64Array::from_iter_values(vec![0, 1, 2, 3, 4]));
        let inputs = Arc::new(StringArray::from_iter_values(
            vec!["one", "two", "three", "four", "five"].into_iter(),
        ));
        let outputs = Arc::new(Float32Array::from_iter_values(vec![
            1.0, 2.0, 3.0, 4.0, 5.0,
        ]));

        // Create an empty list array for each row - using create_empty_list_array
        let string_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let string_array = Arc::new(StringArray::from(Vec::<Option<String>>::new()));
        let list_offsets =
            plateau_transport_arrow_rs::arrow_buffer::OffsetBuffer::<i32>::from_lengths(vec![
                0, 0, 0, 0, 0,
            ]);
        let failures = Arc::new(ListArray::new(
            string_field.clone(),
            list_offsets,
            string_array,
            None,
        ));

        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("inputs", DataType::Utf8, false),
            Field::new("outputs", DataType::Float32, false),
            Field::new("failures", DataType::List(string_field), false),
        ]));

        let record_batch =
            RecordBatch::try_new(schema.clone(), vec![time, inputs, outputs, failures]).unwrap();

        SchemaChunk {
            schema,
            chunk: record_batch,
        }
    }

    pub(crate) fn inferences_large() -> SchemaChunk<Arc<Schema>> {
        use plateau_transport_arrow_rs::{
            arrow_array::{Float32Array, Int64Array, ListArray, StringArray},
            arrow_schema::{DataType, Field, Schema},
        };

        let time = Arc::new(Int64Array::from_iter_values(vec![0, 1, 2, 3, 4]));
        let inputs = Arc::new(StringArray::from_iter_values(
            vec!["one", "two", "three", "four", "five"].into_iter(),
        ));
        let outputs = Arc::new(Float32Array::from_iter_values(vec![
            1.0, 2.0, 3.0, 4.0, 5.0,
        ]));

        // Create list arrays with large strings
        let string_field = Arc::new(Field::new("item", DataType::Utf8, true));

        // Create the string values for all lists (flattened)
        let mut string_values = Vec::new();
        for _ in 0..4 {
            string_values.push(Some("x".repeat(1000)));
        }
        string_values.push(Some("x".repeat(1000)));
        string_values.push(Some("x".repeat(1000)));

        let string_array = Arc::new(StringArray::from(string_values));

        // Create the list offsets - point to where each list starts/ends
        // [0, 1, 2, 4, 5, 6] - List 1 has 1 item, List 2 has 1 item, List 3 has 2 items, etc.
        let list_offsets =
            plateau_transport_arrow_rs::arrow_buffer::OffsetBuffer::<i32>::from_lengths(vec![
                1, 1, 2, 1, 1,
            ]);

        let failures = Arc::new(ListArray::new(
            string_field.clone(),
            list_offsets,
            string_array,
            None,
        ));

        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("inputs", DataType::Utf8, false),
            Field::new("outputs", DataType::Float32, false),
            Field::new("failures", DataType::List(string_field), false),
        ]));

        let record_batch =
            RecordBatch::try_new(schema.clone(), vec![time, inputs, outputs, failures]).unwrap();

        SchemaChunk {
            schema,
            chunk: record_batch,
        }
    }

    pub(crate) async fn setup_with_config(
        config: PlateauConfig,
    ) -> Result<(String, Client, plateau_test::http::TestServer)> {
        fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .try_init()
            .ok(); // called multiple times, so ignore errors

        let server = plateau_test::http::TestServer::new_with_config(PlateauConfig {
            http: http::Config {
                bind: SocketAddr::from(([127, 0, 0, 1], 0)),
                ..config.http
            },
            ..config
        })
        .await?;

        Ok((server.base(), Client::new(&server.base())?, server))
    }

    #[tokio::test]
    async fn test_partition_replication() -> Result<()> {
        let (source_url, client_source, _source) = setup_with_config(Default::default()).await?;
        let (target_url, client_target, _target) = setup_with_config(Default::default()).await?;

        let data: Vec<_> = (0..10).map(|_| inferences_schema_b()).collect();

        let topic = "replicate";
        let partition = "a";

        let source_id = PartitionId::new(topic, partition);
        let target_id = PartitionId::new(topic, "b");

        client_source
            .append_records(topic, partition, &Default::default(), data.clone())
            .await?;

        let hosts = vec![
            ReplicateHost {
                id: "edge".to_string(),
                url: source_url.clone(),
            },
            ReplicateHost {
                id: "mothership".to_string(),
                url: target_url.clone(),
            },
        ];

        let partitions = vec![ReplicatePartition {
            source: HostPartition {
                host_id: "edge".to_string(),
                partition_id: source_id.clone(),
            },
            target: HostPartition {
                host_id: "mothership".to_string(),
                partition_id: target_id.clone(),
            },
            page_size: Some(15),
        }];

        let replicate = Replicate {
            hosts,
            topics: Default::default(),
            partitions,
            config: Default::default(),
        };

        let mut replicator = ReplicationWorker::from_replicate(replicate.clone()).await?;
        replicator.pump().await?;

        assert_eq!(
            client_target
                .get_partitions(target_id.topic())
                .await?
                .partitions
                .get(target_id.partition())
                .cloned(),
            Some(Span { start: 0, end: 50 })
        );

        // now let's simulate some more writes
        client_source
            .append_records(topic, partition, &Default::default(), data[0..6].to_vec())
            .await?;

        // and a brand new replicator run
        let mut replicator = ReplicationWorker::from_replicate(replicate.clone()).await?;

        replicator.pump().await?;

        assert_eq!(
            client_target
                .get_partitions(target_id.topic())
                .await?
                .partitions
                .get(target_id.partition())
                .cloned(),
            Some(Span {
                start: 0,
                end: 50 + 30
            })
        );

        Ok(())
    }

    // A very simple test that verifies we can create a client with differing max_batch_bytes
    #[tokio::test]
    async fn max_batch_setting() -> Result<()> {
        // Create a client with a small max_batch_bytes
        let (url, _, _) = setup_with_config(Default::default()).await?;

        // Create a client
        let mut client = Client::new(&url)?;

        // Set the max batch bytes
        let original_max_bytes = client.max_batch_bytes;
        let new_max_bytes = 1000;
        client.max_batch_bytes = new_max_bytes;

        // Verify it was set correctly
        assert_eq!(client.max_batch_bytes, new_max_bytes);
        assert_ne!(client.max_batch_bytes, original_max_bytes);

        Ok(())
    }

    #[tokio::test]
    async fn page_size_discovery() -> Result<()> {
        let (source_url, client_source, _source) = setup_with_config(Default::default()).await?;
        let (target_url, client_target, _target) = setup_with_config(PlateauConfig {
            http: http::Config {
                // Set higher limit to ensure our test data fits
                max_append_bytes: 5000,
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;

        let data: Vec<_> = (0..10).map(|_| inferences_large()).collect();

        let topic = "replicate";
        let partition = "a";

        let source_id = PartitionId::new(topic, partition);
        let target_id = PartitionId::new(topic, "b");

        client_source
            .append_records(topic, partition, &Default::default(), data.clone())
            .await?;

        let hosts = vec![
            ReplicateHost {
                id: "edge".to_string(),
                url: source_url.clone(),
            },
            ReplicateHost {
                id: "mothership".to_string(),
                url: target_url.clone(),
            },
        ];

        let partitions = vec![ReplicatePartition {
            source: HostPartition {
                host_id: "edge".to_string(),
                partition_id: source_id.clone(),
            },
            target: HostPartition {
                host_id: "mothership".to_string(),
                partition_id: target_id.clone(),
            },
            page_size: Some(15),
        }];

        let replicate = Replicate {
            hosts,
            topics: Default::default(),
            partitions,
            config: Default::default(),
        };

        let mut replicator = ReplicationWorker::from_replicate(replicate.clone()).await?;
        replicator.pump().await?;

        assert_eq!(
            client_target
                .get_partitions(target_id.topic())
                .await?
                .partitions
                .get(target_id.partition())
                .cloned(),
            Some(Span { start: 0, end: 50 })
        );

        // now let's simulate some more writes
        client_source
            .append_records(topic, partition, &Default::default(), data[0..6].to_vec())
            .await?;

        // and a brand new replicator run
        let mut replicator = ReplicationWorker::from_replicate(replicate.clone()).await?;

        replicator.pump().await?;

        assert_eq!(
            client_target
                .get_partitions(target_id.topic())
                .await?
                .partitions
                .get(target_id.partition())
                .cloned(),
            Some(Span {
                start: 0,
                end: 50 + 30
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_topic_replication() -> Result<()> {
        let (source_url, client_source, _source) = setup_with_config(Default::default()).await?;
        let (target_url, client_target, _target) = setup_with_config(Default::default()).await?;

        let data: Vec<_> = (0..10).map(|_| inferences_schema_b()).collect();

        let topic = "replicate".to_string();
        let partitions = vec!["a", "b", "c", "d"];

        for partition in &partitions {
            client_source
                .append_records(&topic, partition, &Default::default(), data.clone())
                .await?;
        }

        let hosts = vec![
            ReplicateHost {
                id: "edge".to_string(),
                url: source_url.clone(),
            },
            ReplicateHost {
                id: "mothership".to_string(),
                url: target_url.clone(),
            },
        ];

        let topics = vec![ReplicateTopic {
            source: HostTopic {
                host_id: "edge".to_string(),
                topic: topic.clone(),
            },
            target: HostTopic {
                host_id: "mothership".to_string(),
                topic: topic.clone(),
            },
            page_size: Some(15),
        }];

        let replicate = Replicate {
            hosts,
            topics,
            partitions: Default::default(),
            config: Default::default(),
        };

        let mut replicator = ReplicationWorker::from_replicate(replicate.clone()).await?;
        replicator.pump().await?;

        for partition in &partitions {
            assert_eq!(
                client_target
                    .get_partitions(&topic)
                    .await?
                    .partitions
                    .get(*partition)
                    .cloned(),
                Some(Span { start: 0, end: 50 })
            );
        }

        // now let's simulate some more writes
        for partition in &partitions {
            client_source
                .append_records(&topic, partition, &Default::default(), data[0..6].to_vec())
                .await?;
        }

        // and a brand new replicator run
        let mut replicator = ReplicationWorker::from_replicate(replicate.clone()).await?;

        replicator.pump().await?;

        for partition in &partitions {
            assert_eq!(
                client_target
                    .get_partitions(&topic)
                    .await?
                    .partitions
                    .get(*partition)
                    .cloned(),
                Some(Span {
                    start: 0,
                    end: 50 + 30
                })
            );
        }

        Ok(())
    }
}
