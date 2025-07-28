//! Display routines for types that the CLI prints out

use std::{collections::HashMap, fmt::Display};

use plateau_client::{ArrowSchema, SchemaChunk};
use plateau_transport::{Inserted, Partitions, RecordStatus, Records, TopicIterationReply, Topics};

pub trait CliDisplay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result;
    fn into_string(self) -> String
    where
        Self: Sized,
    {
        format!("{}", CliOutput::from(self))
    }
}

impl CliDisplay for Topics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for topic in &self.topics {
            writeln!(f, "{}", topic.name)?;
        }
        Ok(())
    }
}

impl CliDisplay for Partitions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (name, span) in &self.partitions {
            writeln!(f, "{} ({} -> {})", name, span.start, span.end)?;
        }
        Ok(())
    }
}

impl CliDisplay for Records {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let header_started = if let Some(ref span) = self.span {
            write!(f, "({} -> {}) ", span.start, span.end)?;
            true
        } else {
            false
        };
        match self.status {
            RecordStatus::All => {
                // if we didn't start a header line, we don't need to add the newline
                if header_started {
                    writeln!(f)?;
                }
            }
            RecordStatus::ByteLimited => {
                writeln!(f, "Records truncated (byte-limited)")?;
            }
            RecordStatus::RecordLimited => {
                writeln!(f, "Records truncated (record-limited)")?;
            }
            RecordStatus::SchemaChange => {
                writeln!(f, "Records truncated (schema change encountered)")?;
            }
        }
        for record in &self.records {
            writeln!(f, "{record}")?;
        }
        Ok(())
    }
}

fn write_iterator(
    iter: &HashMap<String, usize>,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    write!(f, "{{")?;
    let mut first = true;
    for (k, v) in iter {
        if first {
            write!(f, " ")?;
            first = false;
        } else {
            write!(f, ", ")?;
        }
        write!(f, "\"{k}\": {v}")?;
    }
    write!(f, " }}")
}

impl CliDisplay for TopicIterationReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for record in &self.records {
            writeln!(f, "{record}")?;
        }
        match self.status.status {
            RecordStatus::All => {
                write!(f, "All current records returned. ")?;
            }
            RecordStatus::ByteLimited => {
                write!(f, "Partial response (byte-limited). ")?;
            }
            RecordStatus::RecordLimited => {
                write!(f, "Partial response (record-limited). ")?;
            }
            RecordStatus::SchemaChange => {
                write!(f, "Partial response (schema change). ")?;
            }
        }
        write!(f, "Position: ")?;
        write_iterator(&self.status.next, f)?;
        writeln!(f)?;
        Ok(())
    }
}

impl CliDisplay for Inserted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Inserted: ({} -> {})", self.span.start, self.span.end)?;
        Ok(())
    }
}

impl CliDisplay for SchemaChunk<ArrowSchema> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for field in &self.schema.fields {
            write!(f, "\x1b[1m{}:\x1b[0m ", field.name)?;
            match self.get_array([field.name.as_ref()]) {
                Ok(arr) => writeln!(f, "{arr:?}"),
                Err(_) => writeln!(f, "unknown type (not an array)"),
            }?;
        }

        Ok(())
    }
}

struct CliOutput<T> {
    inner: T,
}

impl<T: CliDisplay> Display for CliOutput<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: CliDisplay> From<T> for CliOutput<T> {
    fn from(orig: T) -> Self {
        Self { inner: orig }
    }
}
