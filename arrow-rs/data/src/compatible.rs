use std::iter;

use crate::arrow2::datatypes::{DataType, Field, IntervalUnit, Metadata, Schema, TimeUnit};

pub(crate) trait Compatible<Rhs = Self>
where
    Rhs: ?Sized,
{
    fn compatible(&self, other: &Rhs) -> bool;
}

impl Compatible for Schema {
    #[tracing::instrument(level = "trace", target = "server::compatible::schema")]
    fn compatible(&self, other: &Self) -> bool {
        if !self.fields.compatible(&other.fields) {
            tracing::trace!("Two schemas are incompatible due to different fields");
            return false;
        }

        if self.metadata != other.metadata {
            tracing::trace!("Two schemas are incompatible due to different metadata");
            return false;
        }

        true
    }
}

impl Compatible for Field {
    fn compatible(&self, other: &Self) -> bool {
        if self.name != other.name {
            tracing::trace!("Field objects have different names");
            return false;
        }

        if self.is_nullable != other.is_nullable {
            tracing::trace!("Field objects have different is_nullable");
            return false;
        }

        if self.metadata != other.metadata {
            tracing::trace!("Field objects have different metadata");
            return false;
        }

        if !self.data_type().compatible(other.data_type()) {
            tracing::trace!("Field objects have different types");
            return false;
        }

        true
    }
}

impl Compatible for DataType {
    fn compatible(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Boolean, Self::Boolean) => true,
            (Self::Int8, Self::Int8) => true,
            (Self::Int16, Self::Int16) => true,
            (Self::Int32, Self::Int32) => true,
            (Self::Int64, Self::Int64) => true,
            (Self::UInt8, Self::UInt8) => true,
            (Self::UInt16, Self::UInt16) => true,
            (Self::UInt32, Self::UInt32) => true,
            (Self::UInt64, Self::UInt64) => true,
            (Self::Float16, Self::Float16) => true,
            (Self::Float32, Self::Float32) => true,
            (Self::Float64, Self::Float64) => true,
            (Self::Timestamp(stu, stz), Self::Timestamp(otu, otz)) => {
                stu.compatible(otu) && stz == otz
            }
            (Self::Date32, Self::Date32) => true,
            (Self::Date64, Self::Date64) => true,
            (Self::Time32(a), Self::Time32(b)) => a.compatible(b),
            (Self::Time64(a), Self::Time64(b)) => a.compatible(b),
            (Self::Duration(a), Self::Duration(b)) => a.compatible(b),
            (Self::Interval(a), Self::Interval(b)) => a.compatible(b),
            (Self::Binary, Self::Binary) => true,
            (Self::FixedSizeBinary(a), Self::FixedSizeBinary(b)) => *a == *b,
            (Self::LargeBinary, Self::LargeBinary) => true,
            (Self::Utf8, Self::Utf8) => true,
            (Self::LargeUtf8, Self::LargeUtf8) => true,
            (Self::List(a), Self::List(b)) => a.compatible(b),
            (Self::FixedSizeList(sf, sl), Self::FixedSizeList(of, ol)) => {
                sf.compatible(of) && *sl == *ol
            }
            (Self::LargeList(a), Self::LargeList(b)) => a.compatible(b),
            (Self::Struct(a), Self::Struct(b)) => a.compatible(b),
            (Self::Union(sa, sb, sc), Self::Union(oa, ob, oc)) => {
                sa.compatible(oa) && *sb == *ob && *sc == *oc
            }
            (Self::Map(sa, sb), Self::Map(oa, ob)) => sa.compatible(oa) && sb == ob,
            (Self::Dictionary(sa, sb, sc), Self::Dictionary(oa, ob, oc)) => {
                *sa == *oa && sb.compatible(ob) && *sc == *oc
            }
            (Self::Decimal(sa, sb), Self::Decimal(oa, ob)) => *sa == *oa && *sb == *ob,
            (Self::Decimal256(sa, sb), Self::Decimal256(oa, ob)) => *sa == *oa && *sb == *ob,
            (Self::Extension(sa, sb, sc), Self::Extension(oa, ob, oc)) => {
                sa == oa && sb.compatible(ob) && *sc == *oc
            }
            (a, b) => {
                tracing::trace!(?a, ?b, "DataTypes items are of different type");
                false
            }
        }
    }
}

impl Compatible for TimeUnit {
    fn compatible(&self, other: &Self) -> bool {
        *self == *other
    }
}

impl Compatible for IntervalUnit {
    fn compatible(&self, other: &Self) -> bool {
        *self == *other
    }
}

impl<T> Compatible for [T]
where
    T: Compatible,
{
    fn compatible(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            tracing::trace!("Two schemas are incompatible due to fields of different length");
            return false;
        }

        iter::zip(self, other).all(|(a, b)| a.compatible(b))
    }
}

impl Compatible for Metadata {
    fn compatible(&self, other: &Self) -> bool {
        *self == *other
    }
}
