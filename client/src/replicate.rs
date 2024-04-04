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

use plateau_transport::{ArrowSchema, PartitionId, RecordQuery, SchemaChunk};
use serde::{Deserialize, Serialize};

use crate::{Client, Error, Retrieve};

use tracing::{debug, info};

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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}", self.host_id, self.id)
    }
}

#[derive(Clone)]
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
            .map(|span| span.end)
            .unwrap_or(0);

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

        let mut next_page: Vec<SchemaChunk<ArrowSchema>> = self
            .source
            .client
            .get_records(self.source.id.topic(), self.source.id.partition(), &query)
            .await?;

        if !next_page.is_empty() {
            // this contains iteration information that would result in a schema
            // change per request.
            next_page.iter_mut().for_each(|chunk| {
                chunk.schema.metadata.remove("span");
                chunk.schema.metadata.remove("status");
            });

            let insert = self
                .target
                .client
                .append_records(
                    self.target.id.topic(),
                    self.target.id.partition(),
                    &Default::default(),
                    next_page,
                )
                .await?;

            debug!(
                "{} => {}: {} => {}",
                self.source, self.target, self.record_ix, insert.span.end
            );

            self.record_ix = insert.span.end;

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

#[derive(Clone)]
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}", self.host_id, self.topic)
    }
}

#[derive(Clone)]
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
                        record_ix: target
                            .partitions
                            .get(&partition)
                            .map(|s| s.end)
                            .unwrap_or(0),
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
#[derive(Clone)]
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

        Ok(ReplicationWorker {
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
mod tests {
    use std::net::SocketAddr;

    use anyhow::Result;
    use tracing_subscriber::{fmt, EnvFilter};

    use super::*;
    use crate::Client;
    use plateau::{config::PlateauConfig, http};
    use plateau_transport::{
        arrow2::{
            array::{
                Array, ListArray, MutableListArray, MutableUtf8Array, PrimitiveArray, TryExtend,
                Utf8Array,
            },
            chunk::Chunk,
            datatypes::{Field, Metadata, Schema},
        },
        SchemaChunk, Span,
    };

    pub(crate) fn inferences_schema_b() -> SchemaChunk<Schema> {
        let time = PrimitiveArray::<i64>::from_values(vec![0, 1, 2, 3, 4]);
        let inputs = Utf8Array::<i32>::from_trusted_len_values_iter(
            vec!["one", "two", "three", "four", "five"].into_iter(),
        );
        let outputs = PrimitiveArray::<f32>::from_values(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let mut failures = MutableListArray::<i32, MutableUtf8Array<i32>>::new();
        let values: Vec<Option<Vec<Option<String>>>> = vec![
            Some(vec![]),
            Some(vec![]),
            Some(vec![]),
            Some(vec![]),
            Some(vec![]),
        ];
        failures.try_extend(values).unwrap();
        let failures = ListArray::from(failures);

        let schema = Schema {
            fields: vec![
                Field::new("time", time.data_type().clone(), false),
                Field::new("inputs", inputs.data_type().clone(), false),
                Field::new("outputs", outputs.data_type().clone(), false),
                Field::new("failures", failures.data_type().clone(), false),
            ],
            metadata: Metadata::default(),
        };

        SchemaChunk {
            schema,
            chunk: Chunk::try_new(vec![
                time.boxed(),
                inputs.boxed(),
                outputs.boxed(),
                failures.boxed(),
            ])
            .unwrap(),
        }
    }

    async fn setup_with_config(config: http::Config) -> Result<(String, Client, http::TestServer)> {
        fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .try_init()
            .ok(); // called multiple times, so ignore errors

        let server = http::TestServer::new_with_config(PlateauConfig {
            http: http::Config {
                bind: SocketAddr::from(([127, 0, 0, 1], 0)),
                ..config
            },
            ..PlateauConfig::default()
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
