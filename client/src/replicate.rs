use std::collections::HashMap;
use std::fmt;

use plateau_transport::{ArrowSchema, PartitionId, RecordQuery, SchemaChunk};
use serde::{Deserialize, Serialize};

use crate::{Client, Error, Retrieve};

use log::{debug, info};

#[derive(Clone)]
pub struct ClientPartition {
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
pub struct ReplicatePartitionJob {
    source: ClientPartition,
    target: ClientPartition,
    page_size: Option<usize>,
    record_ix: usize,
}

impl ReplicatePartitionJob {
    pub async fn begin(
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

    pub async fn page(&mut self) -> Result<bool, Error> {
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
            for data in next_page.iter_mut() {
                data.schema.metadata.remove("span");
            }

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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub parallel: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self { parallel: 8 }
    }
}

#[derive(Clone)]
pub struct ReplicationWorker {
    config: Config,
    host_urls: Vec<(String, String)>,
    partitions: Vec<ReplicatePartitionJob>,
}

impl ReplicationWorker {
    pub async fn from_replicate(replicate: Replicate) -> Result<Self, Error> {
        use futures::stream::{self, StreamExt, TryStreamExt};

        let host_urls = replicate
            .hosts
            .iter()
            .map(|host| (host.id.clone(), host.url.clone()))
            .collect();

        let hosts: HashMap<_, _> = replicate
            .hosts
            .into_iter()
            .map(|host| Ok((host.id.clone(), Client::new(&host.url)?)))
            .collect::<Result<_, Error>>()?;

        let hosts_ref = &hosts;

        let partitions: Vec<ReplicatePartitionJob> = stream::iter(replicate.partitions.into_iter())
            .flat_map(move |part| stream::once(part.into_job(hosts_ref)))
            .try_collect()
            .await?;

        Ok(ReplicationWorker {
            config: replicate.config,
            host_urls,
            partitions,
        })
    }

    pub async fn page_all(&mut self) -> Result<bool, Error> {
        use futures::stream::{FuturesUnordered, StreamExt};

        let mut done = true;
        let mut futures = FuturesUnordered::new();
        for job in self.partitions.iter_mut() {
            if futures.len() >= self.config.parallel {
                done = done && futures.next().await.unwrap_or(Ok(true))?;
            }
            futures.push(job.page());
        }

        while let Some(job_status) = futures.next().await {
            done = done && job_status?
        }

        Ok(done)
    }

    pub async fn pump(&mut self) -> Result<(), Error> {
        for (id, url) in &self.host_urls {
            info!("{} url: {}", id, url);
        }

        for job in &self.partitions {
            info!(
                "start: {} => {} @ {}",
                job.source, job.target, job.record_ix
            )
        }

        while !self.page_all().await? {}

        for job in &self.partitions {
            info!("end: {} => {} @ {}", job.source, job.target, job.record_ix)
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Replicate {
    #[serde(default)]
    pub config: Config,
    pub hosts: Vec<ReplicateHost>,
    pub partitions: Vec<ReplicatePartition>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicateHost {
    pub id: String,
    pub url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicatePartition {
    pub source: PartitionClientId,
    pub target: PartitionClientId,
    #[serde(default)]
    pub page_size: Option<usize>,
}

impl ReplicatePartition {
    async fn into_job(
        self,
        clients: &HashMap<String, Client>,
    ) -> Result<ReplicatePartitionJob, Error> {
        let source = self.source.to_client_partition(clients)?;
        let target = self.target.to_client_partition(clients)?;

        ReplicatePartitionJob::begin(source, target, self.page_size).await
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionClientId {
    pub host_id: String,
    pub partition_id: PartitionId,
}

impl PartitionClientId {
    fn to_client_partition(
        &self,
        clients: &HashMap<String, Client>,
    ) -> Result<ClientPartition, Error> {
        let client = clients
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

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use std::net::SocketAddr;

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
        pretty_env_logger::try_init().ok(); // called multiple times, so ignore errors

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
    async fn test_basic_replication() -> Result<()> {
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
            source: PartitionClientId {
                host_id: "edge".to_string(),
                partition_id: source_id.clone(),
            },
            target: PartitionClientId {
                host_id: "mothership".to_string(),
                partition_id: target_id.clone(),
            },
            page_size: Some(15),
        }];

        let replicate = Replicate {
            hosts,
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
}
