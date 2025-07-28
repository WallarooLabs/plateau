use std::fs::File;
use std::path::Path;
use std::time::{Duration, SystemTime};

use arrow2::io::ipc;
use plateau_client::arrow2::chunk::Chunk;
use plateau_client::arrow2::datatypes::{DataType, Field};
use plateau_client::{arrow2, ArrowSchema, MultiChunk};
use sample_arrow2::array::sampler_from_example;
use sample_arrow2::primitive::primitive_len_sampler;
use sample_arrow2::{AlwaysValid, SetLen};
use sample_std::{sample_all, Random, Sample, TryConvert};

use crate::{HealthCheckJob, IteratorJob, SummarizerJob, WorkerConfig, WorkerTask, WriterJob};

pub trait ChunkLen: Sample<Output = MultiChunk> + SetLen {}
impl<S, F, I> ChunkLen for TryConvert<S, F, I>
where
    S: Sample + SetLen,
    F: Fn(S::Output) -> MultiChunk,
    I: Fn(MultiChunk) -> Option<S::Output>,
{
}
pub type MultiChunkSampler = Box<dyn ChunkLen>;

pub struct Now;

impl Sample for Now {
    type Output = i64;

    fn generate(&mut self, _: &mut Random) -> Self::Output {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }
}

pub fn build_sampler(path: &Path) -> anyhow::Result<MultiChunkSampler> {
    let mut file = File::open(path)?;

    let metadata = ipc::read::read_file_metadata(&mut file)?;
    let schema = metadata.schema.clone();
    let mut reader = ipc::read::FileReader::new(&mut file, metadata, None, None);
    let result = reader.next().ok_or_else(|| anyhow::anyhow!("no data"))?;
    let chunk = result?;

    let metadata = schema.metadata;

    let mut samplers = vec![primitive_len_sampler(Now, AlwaysValid)];

    let without_time = schema
        .fields
        .iter()
        .zip(chunk.into_arrays())
        .filter_map(|(f, array)| if f.name == "time" { None } else { Some(array) });

    samplers.extend(without_time.map(|array| sampler_from_example(array.as_ref())));

    let mut fields = vec![Field::new("time", DataType::Int64, false)];
    fields.extend(schema.fields.into_iter().filter(|f| f.name != "time"));

    let schema = ArrowSchema { fields, metadata };

    Ok(Box::new(sample_all(samplers).try_convert(
        move |arrays| MultiChunk {
            schema: schema.clone(),
            chunks: [Chunk::new(arrays)].into(),
        },
        |_| None,
    )))
}

fn worker(stats_group: &str, interval: Duration) -> WorkerConfig {
    WorkerConfig {
        stats_group: stats_group.into(),
        interval,
    }
}

pub fn basic_load_gen(
    sample_path: impl AsRef<Path>,
    topics: usize,
    partitions: usize,
    rows: usize,
    write_interval: Duration,
) -> Vec<WorkerTask> {
    let mut tasks = vec![];
    for topic in 0..topics {
        let name = format!("pipeline-{topic}");

        tasks.extend([
            Box::new(HealthCheckJob {
                worker_config: worker("check", Duration::from_secs(1)),
            }) as WorkerTask,
            Box::new(SummarizerJob {
                topic: name.clone(),
                worker_config: worker("sum", Duration::from_millis(10)),
            }),
            Box::new(IteratorJob {
                topic: name.clone(),
                page_size: 2000,
                worker_config: worker("read", Duration::from_micros(10)),
            }),
        ]);

        for ix in 0..partitions {
            tasks.push(Box::new(WriterJob {
                topic: name.clone(),
                partition: format!("partition-{ix}"),
                sample_path: sample_path.as_ref().to_path_buf(),
                rows: rows..(rows + 1),
                workers: 1,
                worker_config: worker("write", write_interval),
                // worker_config: worker("write", Duration::from_millis(8)),
            }));
        }
    }

    tasks
}

#[cfg(test)]
mod test {
    use super::*;

    use std::path::PathBuf;
    use std::time::Instant;
    use tracing::{debug, trace};
    use tracing_subscriber::{fmt, EnvFilter};

    #[test]
    fn test_sampling() -> anyhow::Result<()> {
        fmt().with_env_filter(EnvFilter::from_default_env()).init();

        let path = PathBuf::from("./samples/image_224x224.arrow");
        let mut sampler = build_sampler(&path)?;

        let mut random = Random::new();

        let now = Instant::now();
        for _ in 0..1 {
            sampler.set_len(1);
            let data = sampler.generate(&mut random);
            trace!("data: {:?}", data);
        }
        debug!("elapsed: {:?}", now.elapsed());

        Ok(())
    }
}
