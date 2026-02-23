use std::ops;

use plateau_transport_arrow_rs::TopicIterationOrder;

/// Each record also has a global unique sequential index
#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct RecordIndex(pub usize);

impl ops::Add<usize> for RecordIndex {
    type Output = Self;

    fn add(self, span: usize) -> Self {
        Self(self.0 + span)
    }
}

impl ops::AddAssign<usize> for RecordIndex {
    fn add_assign(&mut self, span: usize) {
        self.0 += span;
    }
}

impl ops::Sub<usize> for RecordIndex {
    type Output = Self;

    fn sub(self, rhs: usize) -> Self::Output {
        Self(self.0 - rhs)
    }
}

impl ops::SubAssign<usize> for RecordIndex {
    fn sub_assign(&mut self, rhs: usize) {
        self.0 -= rhs;
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Ordering {
    Forward,
    Reverse,
}

impl Ordering {
    pub fn is_reverse(&self) -> bool {
        *self == Self::Reverse
    }
}

impl From<TopicIterationOrder> for Ordering {
    fn from(value: TopicIterationOrder) -> Self {
        match value {
            TopicIterationOrder::Asc => Self::Forward,
            TopicIterationOrder::Desc => Self::Reverse,
        }
    }
}
