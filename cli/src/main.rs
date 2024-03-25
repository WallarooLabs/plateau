use anyhow::{anyhow, Error};
use futures::StreamExt;
#[cfg(feature = "polars")]
use plateau_client::IterateUnlimited;
use plateau_transport::{
    Insert, InsertQuery, RecordQuery, Records, TopicIterationQuery, TopicIterationReply,
};
use std::{collections::HashMap, fmt::Display, path::PathBuf, pin::Pin, str::FromStr};
use structopt::StructOpt;
use tokio::{fs::File, io::AsyncWriteExt};
use tokio_util::io::ReaderStream;

use plateau_client::{
    localhost, ArrowSchema, ArrowStream, Client, Iterate, Retrieve, SchemaChunk, SizedArrowStream,
};

mod display;
pub use display::CliDisplay;

#[derive(Debug)]
struct Url(url::Url);

impl Default for Url {
    fn default() -> Url {
        Url(localhost())
    }
}

impl Display for Url {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for Url {
    type Err = url::ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Url(url::Url::from_str(s)?))
    }
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "plateau-cli",
    about = "Command-line interfact to a plateau server."
)]
struct Cli {
    /// Host URL
    #[structopt(short, long, parse(try_from_str), default_value)]
    host: Url,

    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    /// Retrieve list of all topics
    Topics,
    /// Retrieve list of partitions for topic
    Partitions {
        /// Topic name
        topic_name: String,
    },
    /// Retrieve records in partition of topic
    Records {
        /// Topic name
        topic_name: String,
        /// Partition name
        partition_name: String,
        /// Desired output format
        #[structopt(short, long, default_value)]
        format: OutputFormat,
        #[structopt(flatten)]
        params: RecordQuery,
    },
    /// Iterate through records
    Iterate {
        /// Topic name
        topic_name: String,
        /// Partition name
        partition_name: String,
        /// Iterator position
        position: usize,
        #[structopt(flatten)]
        params: TopicIterationQuery,
    },
    #[cfg(feature = "polars")]
    /// Iterate through records with Polars
    IteratePolars {
        /// Topic name
        topic_name: String,
        /// Partition name
        partition_name: String,
        /// Iterator position
        position: usize,
        #[structopt(flatten)]
        params: TopicIterationQuery,
    },
    #[cfg(feature = "polars")]
    /// Iterate through records with Polars
    IterateUnlimited {
        /// Topic name
        topic_name: String,
        #[structopt(flatten)]
        params: TopicIterationQuery,
    },
    /// Insert a single record
    Insert {
        /// Topic name
        topic_name: String,
        /// Partition name
        partition_name: String,
        #[structopt(flatten)]
        params: InsertQuery,
        #[structopt(flatten)]
        record: InsertSingle,
    },
    // Append a set of records in arrow format
    Append {
        /// Topic name
        topic_name: String,
        /// Partition name
        partition_name: String,
        #[structopt(flatten)]
        params: InsertQuery,
        #[structopt(flatten)]
        records: InsertArrow,
    },
}

#[derive(Debug, StructOpt)]
pub struct InsertSingle {
    /// Record to insert
    pub record: String,
}

#[derive(Debug, StructOpt)]
pub struct InsertArrow {
    /// Filename containing arrow data chunk in IPC format
    #[structopt(parse(from_os_str))]
    pub records_path: PathBuf,
}

#[derive(Debug, StructOpt, Default)]
pub enum OutputFormat {
    Arrow {
        path: PathBuf,
    },
    Plaintext {
        path: PathBuf,
    },
    #[default]
    Stdout,
    ArrowStdout,
    #[cfg(feature = "polars")]
    Polars {
        path: PathBuf,
    },
    #[cfg(feature = "polars")]
    PolarsStdout,
}

impl Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Arrow { path } => write!(f, "arrow={}", path.display()),
            OutputFormat::Plaintext { path } => write!(f, "plaintext={}", path.display()),
            OutputFormat::Stdout => write!(f, "plaintext"),
            OutputFormat::ArrowStdout => write!(f, "arrow"),
            #[cfg(feature = "polars")]
            OutputFormat::Polars { path } => write!(f, "polars={}", path.display()),
            #[cfg(feature = "polars")]
            OutputFormat::PolarsStdout => write!(f, "polars"),
        }
    }
}

impl FromStr for OutputFormat {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut equal_split = s.split('=');
        let (format, path) = (equal_split.next(), equal_split.next());

        match (format, path) {
            (Some("text" | "plaintext"), None) => Ok(OutputFormat::Stdout),
            (Some("text" | "plaintext"), Some(filename)) => Ok(OutputFormat::Plaintext {
                path: filename.into(),
            }),
            (Some("arrow"), Some(filename)) => Ok(OutputFormat::Arrow {
                path: filename.into(),
            }),
            (Some("arrow"), None) => Ok(OutputFormat::ArrowStdout),
            #[cfg(feature = "polars")]
            (Some("polars"), None) => Ok(OutputFormat::PolarsStdout),
            #[cfg(feature = "polars")]
            (Some("polars"), Some(filename)) => Ok(OutputFormat::Polars {
                path: filename.into(),
            }),
            _ => Err(anyhow!("invalid output format specificiation: {s}")),
        }
    }
}

async fn make_request<'a>(client: &Client, cmd: Command) -> Result<(), Error> {
    match cmd {
        Command::Topics => {
            print!("{}", client.get_topics().await?.into_string());
        }
        Command::Partitions { topic_name } => {
            print!("{}", client.get_partitions(topic_name).await?.into_string());
        }
        Command::Records {
            topic_name,
            partition_name,
            format,
            mut params,
        } => match format {
            OutputFormat::Stdout => {
                let response: Records = client
                    .get_records(topic_name, partition_name, &params)
                    .await?;
                print!("{}", response.into_string());
            }
            OutputFormat::Plaintext { path } => {
                let response: Records = client
                    .get_records(topic_name, partition_name, &params)
                    .await?;
                tokio::fs::write(path, response.into_string().as_bytes()).await?;
            }
            OutputFormat::Arrow { path } => {
                let mut response: Pin<Box<dyn ArrowStream>> = client
                    .get_records(topic_name, partition_name, &params)
                    .await?;
                let mut file = File::create(path).await?;
                while let Some(chunk) = response.next().await {
                    file.write_all(&chunk?).await?;
                }
            }
            OutputFormat::ArrowStdout => {
                params.data_focus.dataset_separator = Some(".".to_owned());
                let mut response: Vec<SchemaChunk<ArrowSchema>> = client
                    .get_records(topic_name, partition_name, &params)
                    .await?;

                for (i, sc) in response.drain(..).enumerate() {
                    print!("-- Chunk {}\n{}", i + params.start, sc.into_string());
                }
            }
            #[cfg(feature = "polars")]
            OutputFormat::PolarsStdout => {
                let response: polars::frame::DataFrame = client
                    .get_records(topic_name, partition_name, &params)
                    .await?;

                print!("\n{}", response);

                let df = response.unnest(["out", "in", "metadata"])?;

                print!("\n{:?}", df);
            }
            #[cfg(feature = "polars")]
            OutputFormat::Polars { path } => {
                let response: polars::frame::DataFrame = client
                    .get_records(topic_name, partition_name, &params)
                    .await?;

                let mut file = File::create(path).await?;
                file.write_all(response.to_string().as_bytes()).await?;
            }
        },
        Command::Iterate {
            topic_name,
            partition_name,
            position,
            params,
        } => {
            let response: TopicIterationReply = client
                .iterate_topic(
                    topic_name,
                    &params,
                    &Some(HashMap::from([(partition_name, position)])),
                )
                .await?;
            print!("{}", response.into_string());
        }
        #[cfg(feature = "polars")]
        Command::IteratePolars {
            topic_name,
            partition_name,
            position,
            params,
        } => {
            let response: polars::frame::DataFrame = client
                .iterate_topic(
                    topic_name,
                    &params,
                    &Some(HashMap::from([(partition_name, position)])),
                )
                .await?;
            print!("{}", response);
        }
        #[cfg(feature = "polars")]
        Command::IterateUnlimited { topic_name, params } => {
            let response: polars::frame::DataFrame =
                client.iterate_topic_unlimited(&topic_name, &params).await?;

            print!("{:#?}", response.schema());
            print!("{}", response);
        }
        Command::Insert {
            topic_name,
            partition_name,
            params,
            record,
        } => {
            print!(
                "{}",
                client
                    .append_records(
                        topic_name,
                        partition_name,
                        &params,
                        Insert {
                            records: vec![record.record]
                        },
                    )
                    .await?
                    .into_string()
            );
        }
        Command::Append {
            topic_name,
            partition_name,
            params,
            records,
        } => {
            let file = File::open(&records.records_path).await?;
            let file_len = file.metadata().await?.len();
            print!(
                "{}",
                client
                    .append_records(
                        topic_name,
                        partition_name,
                        &params,
                        SizedArrowStream {
                            stream: Box::pin(ReaderStream::new(file)),
                            size: file_len
                        },
                    )
                    .await?
                    .into_string()
            );
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let matches = Cli::clap().get_matches();
    let Cli { host, cmd } = Cli::from_clap(&matches);

    let client: Client = host.0.into();

    make_request(&client, cmd).await?;

    Ok(())
}
