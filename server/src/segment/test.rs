use std::borrow::Borrow;

use chrono::{TimeZone, Utc};
use plateau_test::inferences_schema_a;
use plateau_transport::arrow2::datatypes::DataType;
use plateau_transport::SchemaChunk;
use sample_arrow2::{
    array::ArbitraryArray,
    chunk::ArbitraryChunk,
    datatypes::{sample_flat, ArbitraryDataType},
};
use sample_std::{Chance, Regex};
use tempfile::{tempdir, TempDir};
use test::arrow::test::partial_write;
use test_log::test;

use super::*;

use crate::chunk::iter_legacy;

impl Config {
    pub fn nocommit() -> Self {
        Self {
            durable_checkpoints: false,
            arrow: false,
        }
    }

    pub fn parquet() -> Self {
        Self {
            arrow: false,
            ..Self::default()
        }
    }

    pub fn arrow() -> Self {
        Self {
            arrow: true,
            ..Self::default()
        }
    }
}

impl Writer {
    pub fn log_arrow<S: Borrow<Schema> + Clone + PartialEq>(
        &mut self,
        data: SchemaChunk<S>,
        active: Option<SegmentChunk>,
    ) -> Result<()> {
        self.log_arrows(data.schema.borrow(), vec![data.chunk], active)
    }

    pub fn update_cache(&mut self, active: SegmentChunk) -> Result<()> {
        self.cache.update(self.chunk_ix, &self.schema, active)
    }
}

pub fn build_records<I: Iterator<Item = (i64, String)>>(it: I) -> Vec<Record> {
    it.map(|(ix, message)| Record {
        time: Utc.timestamp_opt(ix, 0).unwrap(),
        message: message.into_bytes(),
    })
    .collect()
}

pub fn collect_records(
    schema: Schema,
    iter: impl Iterator<Item = Result<SegmentChunk, anyhow::Error>>,
) -> Vec<Record> {
    iter_legacy(schema, iter).flat_map(Result::unwrap).collect()
}

// nulls=true breaks arrow2's parquet support, but is fine for feather
pub fn deep_chunk(depth: usize, len: usize, nulls: bool) -> ArbitraryChunk<Regex, Chance> {
    let names = Regex::new("[a-z]{4,8}");
    let data_type = ArbitraryDataType {
        struct_branch: 1..3,
        names: names.clone(),
        nullable: if nulls { Chance(0.5) } else { Chance(0.0) },
        flat: sample_flat,
    }
    .sample_depth(depth);

    let array = ArbitraryArray {
        names,
        branch: 0..10,
        len: len..(len + 1),
        null: Chance(0.1),
        // this appears to break arrow2's parquet support
        // is_nullable: true,
        is_nullable: false,
    };

    ArbitraryChunk {
        chunk_len: 10..1000,
        array_count: 1..2,
        data_type,
        array,
    }
}

#[test]
fn test_interrupted_cache_write() -> Result<()> {
    let root = tempdir()?;
    let path = root.path().join("partial-write.parquet");
    let s = Segment::at(path.clone(), None);

    let a = inferences_schema_a();
    let mut w = s.create(a.schema.clone(), Config::default())?;
    w.log_arrow(a.clone(), Some(a.chunk.clone()))?;
    drop(w);

    let f = fs::File::options().append(true).open(s.cache_path())?;
    f.set_len(f.metadata()?.len() - 15)?;

    let mut r = s.iter()?;
    assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk));
    assert_eq!(r.next().map(|v| v.ok()), None);

    Ok(())
}

#[test]
fn test_partial_cache_write() -> Result<()> {
    let root = tempdir()?;
    let path = root.path().join("partial-write.parquet");
    let s = Segment::at(path.clone(), None);

    let a = inferences_schema_a();
    let mut w = s.create(a.schema.clone(), Config::default())?;
    w.log_arrow(a.clone(), Some(a.chunk.clone()))?;

    let more = crate::chunk::concatenate(&[a.chunk.clone(), a.chunk.clone()])?;
    w.log_arrows(&a.schema, vec![], Some(more))?;
    drop(w);

    let f = fs::File::options().append(true).open(s.cache_path())?;
    f.set_len(f.metadata()?.len() - 15)?;

    let mut r = s.iter()?;
    assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
    assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk));
    assert_eq!(r.next().map(|v| v.ok()), None);

    Ok(())
}

#[test]
fn test_arrow_with_truncated_cache() -> Result<()> {
    let root = tempdir()?;
    let path = root.path().join("partial-write.arrow");
    let s = Segment::at(path.clone(), None);

    let a = inferences_schema_a();
    let mut w = s.create(a.schema.clone(), Config::arrow())?;
    w.log_arrow(a.clone(), Some(a.chunk.clone()))?;
    w.log_arrow(a.clone(), Some(a.chunk.clone()))?;

    let more = crate::chunk::concatenate(&[a.chunk.clone(), a.chunk.clone()])?;
    w.log_arrows(&a.schema, vec![], Some(more))?;
    drop(w);

    let f = fs::File::options().append(true).open(s.cache_path())?;
    f.set_len(f.metadata()?.len() - 15)?;

    let mut r = s.iter()?;
    // two chunks from file, one from cache (the other will have its frame
    // interrupted by above corruption)
    assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
    assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
    assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
    assert_eq!(r.next().map(|v| v.ok()), None);

    Ok(())
}

#[test]
fn test_arrow_corruption_with_cache_write() -> Result<()> {
    let root = tempdir()?;
    let path = root.path().join("partial-write.arrow");
    let s = Segment::at(path.clone(), None);

    let a = inferences_schema_a();
    let mut w = s.create(a.schema.clone(), Config::arrow())?;
    w.log_arrow(a.clone(), Some(a.chunk.clone()))?;
    w.log_arrow(a.clone(), Some(a.chunk.clone()))?;

    let more = crate::chunk::concatenate(&[a.chunk.clone(), a.chunk.clone()])?;
    w.log_arrows(&a.schema, vec![], Some(more))?;
    drop(w);

    let f = fs::File::options().append(true).open(s.path())?;
    f.set_len(f.metadata()?.len() - 15)?;

    let mut r = s.iter()?;
    assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
    // we need to discard the whole cache because of the gap created above
    assert_eq!(r.next().map(|v| v.ok()), None);

    Ok(())
}

#[test]
fn test_dual_format() -> Result<()> {
    let root = tempdir()?;
    let parquet = Segment::at(root.path().join("test.parquet"), None);
    let arrow = Segment::at(root.path().join("test.arrow"), None);

    let a = inferences_schema_a();

    let mut w = parquet.create(a.schema.clone(), Config::default())?;
    w.log_arrow(a.clone(), Some(a.chunk.clone()))?;
    w.end()?;

    let mut w = arrow.create(a.schema.clone(), Config::arrow())?;
    w.log_arrow(a.clone(), Some(a.chunk.clone()))?;
    w.end()?;

    // verify we don't need to provide the format here, it's autodetected
    let mut r = parquet.iter()?;
    assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
    assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
    assert_eq!(r.next().map(|v| v.ok()), None);

    let mut r = arrow.iter()?;
    assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk.clone()));
    assert_eq!(r.next().map(|v| v.unwrap()), Some(a.chunk));
    assert_eq!(r.next().map(|v| v.ok()), None);

    Ok(())
}

#[test]
fn test_parquet_cache_updates() -> Result<()> {
    let root = tempdir()?;

    let a = inferences_schema_a();

    let all_counts = [1, 3, 4, 2, 1];
    for ix in 1..all_counts.len() {
        trace!("iter: {ix} counts: 1 + {:?}", &all_counts[0..ix]);
        let mut chunk = a.chunk.clone();

        let path = root.path().join(format!("{ix:?}.parquet"));
        let s = Segment::at(path.clone(), None);
        let mut w = s.create(a.schema.clone(), Config::parquet())?;

        for count in &all_counts[0..ix] {
            let new_parts: Vec<_> = std::iter::once(chunk.clone())
                .chain(std::iter::repeat(a.chunk.clone()).take(*count))
                .collect();
            chunk = crate::chunk::concatenate(&new_parts)?;
            w.update_cache(chunk.clone())?;
        }

        drop(w);

        let mut r = s.iter()?;
        assert_eq!(r.next().map(|v| v.unwrap()), Some(chunk));
    }

    Ok(())
}

#[test]
fn test_arrow_cache_updates() -> Result<()> {
    let root = tempdir()?;

    let a = inferences_schema_a();

    let all_counts = [1, 3, 4, 2, 1];
    for ix in 1..all_counts.len() {
        trace!("iter: {ix} counts: 1 + {:?}", &all_counts[0..ix]);
        let mut chunk = a.chunk.clone();

        let path = root.path().join(format!("{ix:?}.arrow"));
        let s = Segment::at(path.clone(), None);
        let mut w = s.create(a.schema.clone(), Config::arrow())?;

        for count in &all_counts[0..ix] {
            let new_parts: Vec<_> = std::iter::once(chunk.clone())
                .chain(std::iter::repeat(a.chunk.clone()).take(*count))
                .collect();
            chunk = crate::chunk::concatenate(&new_parts)?;
            w.update_cache(chunk.clone())?;
        }

        drop(w);

        let mut r = s.iter()?;
        assert_eq!(r.next().map(|v| v.unwrap()), Some(chunk));
    }

    Ok(())
}

#[test]
fn test_partial_write_size_destroy() -> Result<()> {
    let root = tempdir()?;
    let a = inferences_schema_a();
    let arrow_segment = partial_write(root.path(), a.clone())?;

    let paths: Vec<_> = arrow_segment.clone().parts().collect();
    let segment = Segment::at(arrow_segment.into_path(), None);

    segment.iter()?.count();

    assert!(segment.size_estimate()? > fs::metadata(&segment.path)?.len() as usize);
    segment.destroy()?;

    for path in paths {
        assert!(!path.exists());
    }

    Ok(())
}

#[test]
fn focus_vs_unfocus() -> Result<()> {
    let (_root, path) = prepare_segment("focus.arrow", true)?;

    // Make sure unfocused data has all the datasets
    let mut full = Segment::at(&path, None).iter()?;
    let chunk1 = full.next().unwrap().unwrap();
    let chunk2 = full.next().unwrap().unwrap();
    assert!(full.next().is_none());
    assert_eq!(chunk1.arrays().len(), 4);
    assert_eq!(chunk2.arrays().len(), 4);

    // And now narrow down the reader to only one dataset
    let focus = DataFocus::with_dataset("time");
    let mut focused = Segment::at(&path, focus).iter()?;
    let chunk1 = focused.next().unwrap().unwrap();
    let chunk2 = focused.next().unwrap().unwrap();
    assert!(focused.next().is_none());
    assert_eq!(chunk1.arrays().len(), 1);
    assert_eq!(chunk2.arrays().len(), 1);

    Ok(())
}

#[test]
fn focus_include_single() -> Result<()> {
    let (_root, path) = prepare_segment("focus.arrow", true)?;

    let focus = DataFocus::with_dataset("time");
    let mut focused = Segment::at(path, focus).iter()?;
    let arrays1 = focused.next().unwrap().unwrap().into_arrays();
    let arrays2 = focused.next().unwrap().unwrap().into_arrays();
    assert!(focused.next().is_none());

    assert_eq!(arrays1.len(), 1);
    assert_eq!(arrays2.len(), 1);

    assert_eq!(arrays1[0].len(), 5);
    assert_eq!(arrays2[0].len(), 5);

    assert_eq!(arrays1[0].data_type(), &DataType::Int64);
    assert_eq!(arrays2[0].data_type(), &DataType::Int64);

    Ok(())
}

#[test]
fn focus_include_multiple() -> Result<()> {
    let (_root, path) = prepare_segment("focus.arrow", true)?;

    let focus = DataFocus::with_datasets(&["inputs", "outputs"]);
    let mut focused = Segment::at(path, focus).iter()?;
    let arrays1 = focused.next().unwrap().unwrap().into_arrays();
    let arrays2 = focused.next().unwrap().unwrap().into_arrays();
    assert!(focused.next().is_none());

    assert_eq!(arrays1.len(), 2);
    assert_eq!(arrays2.len(), 2);

    assert_eq!(arrays1[0].len(), 5);
    assert_eq!(arrays2[0].len(), 5);

    assert_eq!(arrays1[0].data_type(), &DataType::Float32);
    assert_eq!(arrays2[0].data_type(), &DataType::Float32);

    Ok(())
}

#[test]
fn focus_exclude_single() -> Result<()> {
    let (_root, path) = prepare_segment("focus.arrow", true)?;
    let focus = DataFocus::without_dataset("time");
    let mut focused = Segment::at(path, focus).iter()?;
    let arrays1 = focused.next().unwrap().unwrap().into_arrays();
    let arrays2 = focused.next().unwrap().unwrap().into_arrays();
    assert!(focused.next().is_none());

    assert_eq!(arrays1.len(), 3);
    assert_eq!(arrays2.len(), 3);

    assert_eq!(arrays1[0].len(), 5);
    assert_eq!(arrays1[1].len(), 5);
    assert_eq!(arrays1[2].len(), 5);

    assert_eq!(arrays2[0].len(), 5);
    assert_eq!(arrays2[1].len(), 5);
    assert_eq!(arrays2[2].len(), 5);

    assert!(matches!(arrays1[0].data_type(), DataType::List(_)));
    assert!(matches!(arrays1[1].data_type(), DataType::Float32));
    assert!(matches!(arrays1[2].data_type(), DataType::Struct(_)));

    assert!(matches!(arrays2[0].data_type(), DataType::List(_)));
    assert!(matches!(arrays2[1].data_type(), DataType::Float32));
    assert!(matches!(arrays2[2].data_type(), DataType::Struct(_)));

    Ok(())
}

#[test]
fn focus_exclude_multiple() -> Result<()> {
    let (_root, path) = prepare_segment("focus.arrow", true)?;
    let focus = DataFocus::without_datasets(&["inputs", "outputs"]);
    let mut focused = Segment::at(path, focus).iter()?;
    let arrays1 = focused.next().unwrap().unwrap().into_arrays();
    let arrays2 = focused.next().unwrap().unwrap().into_arrays();
    assert!(focused.next().is_none());

    assert_eq!(arrays1.len(), 2);
    assert_eq!(arrays2.len(), 2);

    assert_eq!(arrays1[0].len(), 5);
    assert_eq!(arrays1[1].len(), 5);

    assert_eq!(arrays2[0].len(), 5);
    assert_eq!(arrays2[1].len(), 5);

    assert!(matches!(arrays1[0].data_type(), DataType::Int64));
    assert!(matches!(arrays1[1].data_type(), DataType::List(_)));
    assert!(matches!(arrays2[0].data_type(), DataType::Int64));
    assert!(matches!(arrays2[1].data_type(), DataType::List(_)));

    Ok(())
}

mod cache {
    use super::*;
    use test_log::test;

    #[test]
    fn focus_vs_unfocus() -> Result<()> {
        let (_root, path) = prepare_segment("focus.arrow", false)?;

        // Make sure unfocused data has all the datasets
        let mut full = Segment::at(&path, None).iter()?;
        let chunk1 = full.next().unwrap().unwrap();
        let chunk2 = full.next().unwrap().unwrap();
        assert!(full.next().is_none());
        assert_eq!(chunk1.arrays().len(), 4);
        assert_eq!(chunk2.arrays().len(), 4);

        // And now narrow down the reader to only one dataset
        let focus = DataFocus::with_dataset("time");
        let mut focused = Segment::at(&path, focus).iter()?;
        let chunk1 = focused.next().unwrap().unwrap();
        let chunk2 = focused.next().unwrap().unwrap();
        assert!(focused.next().is_none());
        assert_eq!(chunk1.arrays().len(), 1);
        assert_eq!(chunk2.arrays().len(), 1);

        Ok(())
    }

    #[test]
    fn focus_include_single() -> Result<()> {
        let (_root, path) = prepare_segment("focus.arrow", false)?;

        let focus = DataFocus::with_dataset("time");
        let segment = Segment::at(path, focus);
        tracing::debug!(
            ?segment,
            path = %segment.path().display(),
            cache_path = %segment.cache_path().display()
        );

        // std::thread::sleep(std::time::Duration::from_secs(180));

        let mut focused = segment.iter()?;

        // tracing::debug!(?focused);

        let arrays1 = focused.next().unwrap().unwrap().into_arrays();
        let arrays2 = focused.next().unwrap().unwrap().into_arrays();
        assert!(focused.next().is_none());

        assert_eq!(arrays1.len(), 1);
        assert_eq!(arrays2.len(), 1);

        assert_eq!(arrays1[0].len(), 5);
        assert_eq!(arrays2[0].len(), 5);

        assert_eq!(arrays1[0].data_type(), &DataType::Int64);
        assert_eq!(arrays2[0].data_type(), &DataType::Int64);

        Ok(())
    }

    #[test]
    fn no_focus_include_single() -> Result<()> {
        let (_root, path) = prepare_segment("focus.arrow", false)?;

        // let focus = DataFocus::with_dataset("time");
        let mut focused = Segment::at(path, None).iter()?;
        let arrays1 = focused.next().unwrap().unwrap().into_arrays();
        let arrays2 = focused.next().unwrap().unwrap().into_arrays();
        assert!(focused.next().is_none());

        // assert_eq!(arrays1.len(), 1);
        // assert_eq!(arrays2.len(), 1);

        // assert_eq!(arrays1[0].len(), 5);
        // assert_eq!(arrays2[0].len(), 5);

        // assert_eq!(arrays1[0].data_type(), &DataType::Int64);
        // assert_eq!(arrays2[0].data_type(), &DataType::Int64);

        Ok(())
    }

    #[test]
    fn focus_include_multiple() -> Result<()> {
        let (_root, path) = prepare_segment("focus.arrow", false)?;

        let focus = DataFocus::with_datasets(&["inputs", "outputs"]);
        let mut focused = Segment::at(path, focus).iter()?;
        let arrays1 = focused.next().unwrap().unwrap().into_arrays();
        let arrays2 = focused.next().unwrap().unwrap().into_arrays();
        assert!(focused.next().is_none());

        assert_eq!(arrays1.len(), 2);
        assert_eq!(arrays2.len(), 2);

        assert_eq!(arrays1[0].len(), 5);
        assert_eq!(arrays2[0].len(), 5);

        assert_eq!(arrays1[0].data_type(), &DataType::Float32);
        assert_eq!(arrays2[0].data_type(), &DataType::Float32);

        Ok(())
    }

    #[test]
    fn focus_exclude_single() -> Result<()> {
        let (_root, path) = prepare_segment("focus.arrow", false)?;
        let focus = DataFocus::without_dataset("time");
        let mut focused = Segment::at(path, focus).iter()?;
        let arrays1 = focused.next().unwrap().unwrap().into_arrays();
        let arrays2 = focused.next().unwrap().unwrap().into_arrays();
        assert!(focused.next().is_none());

        assert_eq!(arrays1.len(), 3);
        assert_eq!(arrays2.len(), 3);

        assert_eq!(arrays1[0].len(), 5);
        assert_eq!(arrays1[1].len(), 5);
        assert_eq!(arrays1[2].len(), 5);

        assert_eq!(arrays2[0].len(), 5);
        assert_eq!(arrays2[1].len(), 5);
        assert_eq!(arrays2[2].len(), 5);

        assert!(matches!(arrays1[0].data_type(), DataType::List(_)));
        assert!(matches!(arrays1[1].data_type(), DataType::Float32));
        assert!(matches!(arrays1[2].data_type(), DataType::Struct(_)));

        assert!(matches!(arrays2[0].data_type(), DataType::List(_)));
        assert!(matches!(arrays2[1].data_type(), DataType::Float32));
        assert!(matches!(arrays2[2].data_type(), DataType::Struct(_)));

        Ok(())
    }

    #[test]
    fn focus_exclude_multiple() -> Result<()> {
        let (_root, path) = prepare_segment("focus.arrow", false)?;
        let focus = DataFocus::without_datasets(&["inputs", "outputs"]);
        let mut focused = Segment::at(path, focus).iter()?;
        let arrays1 = focused.next().unwrap().unwrap().into_arrays();
        let arrays2 = focused.next().unwrap().unwrap().into_arrays();
        assert!(focused.next().is_none());

        assert_eq!(arrays1.len(), 2);
        assert_eq!(arrays2.len(), 2);

        assert_eq!(arrays1[0].len(), 5);
        assert_eq!(arrays1[1].len(), 5);

        assert_eq!(arrays2[0].len(), 5);
        assert_eq!(arrays2[1].len(), 5);

        assert!(matches!(arrays1[0].data_type(), DataType::Int64));
        assert!(matches!(arrays1[1].data_type(), DataType::List(_)));
        assert!(matches!(arrays2[0].data_type(), DataType::Int64));
        assert!(matches!(arrays2[1].data_type(), DataType::List(_)));

        Ok(())
    }
}

fn prepare_segment(segment: impl AsRef<Path>, finalize_cache: bool) -> Result<(TempDir, PathBuf)> {
    let root = tempdir()?;
    let segment = root.path().join(segment);
    let arrow = Segment::at(root.path().join(&segment), None);
    let a = inferences_schema_a();
    let mut w = arrow.create(a.schema.clone(), Config::arrow())?;
    w.log_arrow(a.clone(), Some(a.chunk.clone()))?;
    if finalize_cache {
        w.end()?;
    }

    Ok((root, segment))
}
