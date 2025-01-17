use std::any::Any;

use polars_core::prelude::*;

use crate::executors::sinks::groupby::aggregates::AggregateFn;

#[derive(Clone)]
pub struct NullAgg(DataType);

impl NullAgg {
    pub(crate) fn new(dt: DataType) -> Self {
        Self(dt)
    }
}

impl AggregateFn for NullAgg {
    fn pre_agg(&mut self, _chunk_idx: IdxSize, _item: &mut dyn ExactSizeIterator<Item = AnyValue>) {
        // no-op
    }

    fn dtype(&self) -> DataType {
        self.0.clone()
    }

    fn combine(&mut self, _other: &dyn Any) {
        // no-op
    }

    fn finalize(&mut self) -> AnyValue<'static> {
        AnyValue::Null
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
