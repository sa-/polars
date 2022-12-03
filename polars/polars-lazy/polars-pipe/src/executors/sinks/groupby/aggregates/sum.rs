use std::any::Any;
use std::ops::Add;

use polars_core::datatypes::{AnyValue, DataType};
use polars_core::export::num::NumCast;
use polars_core::prelude::NumericNative;
use polars_utils::unwrap::UnwrapUncheckedRelease;

use super::*;
use crate::operators::{ArrowDataType, IdxSize};

pub struct SumAgg<K: NumericNative> {
    sum: Option<K>,
}

impl<K: NumericNative> SumAgg<K> {
    pub(crate) fn new() -> Self {
        SumAgg { sum: None }
    }
}

impl<K: NumericNative + Add<Output = K> + NumCast> SumAgg<K> {
    fn pre_agg_primitive<T: NumCast>(&mut self, item: Option<T>) {
        match (item.map(|v| K::from(v).unwrap()), self.sum) {
            (Some(val), Some(sum)) => self.sum = Some(sum + val),
            (Some(val), None) => self.sum = Some(val),
            (None, _) => {}
        }
    }
}

impl<K: NumericNative + Add<Output = K> + NumCast> AggregateFn for SumAgg<K> {
    fn has_physical_agg(&self) -> bool {
        true
    }

    fn pre_agg(&mut self, _chunk_idx: IdxSize, item: &mut dyn ExactSizeIterator<Item = AnyValue>) {
        let item = unsafe { item.next().unwrap_unchecked_release() };
        self.pre_agg_primitive(item.extract::<K>())
    }
    fn pre_agg_i16(&mut self, _chunk_idx: IdxSize, item: Option<i16>) {
        self.pre_agg_primitive(item)
    }
    fn pre_agg_u16(&mut self, _chunk_idx: IdxSize, item: Option<u16>) {
        self.pre_agg_primitive(item)
    }
    fn pre_agg_i32(&mut self, _chunk_idx: IdxSize, item: Option<i32>) {
        self.pre_agg_primitive(item)
    }
    fn pre_agg_i64(&mut self, _chunk_idx: IdxSize, item: Option<i64>) {
        self.pre_agg_primitive(item)
    }
    fn pre_agg_u32(&mut self, _chunk_idx: IdxSize, item: Option<u32>) {
        self.pre_agg_primitive(item)
    }
    fn pre_agg_u64(&mut self, _chunk_idx: IdxSize, item: Option<u64>) {
        self.pre_agg_primitive(item)
    }
    fn pre_agg_f32(&mut self, _chunk_idx: IdxSize, item: Option<f32>) {
        self.pre_agg_primitive(item)
    }
    fn pre_agg_f64(&mut self, _chunk_idx: IdxSize, item: Option<f64>) {
        self.pre_agg_primitive(item)
    }

    fn dtype(&self) -> DataType {
        (&ArrowDataType::from(K::PRIMITIVE)).into()
    }

    fn combine(&mut self, other: &dyn Any) {
        let other = unsafe { other.downcast_ref::<Self>().unwrap_unchecked_release() };
        let sum = match (self.sum, other.sum) {
            (Some(lhs), Some(rhs)) => Some(lhs + rhs),
            (Some(lhs), None) => Some(lhs),
            (None, Some(rhs)) => Some(rhs),
            (None, None) => None,
        };
        self.sum = sum;
    }

    fn finalize(&mut self) -> AnyValue<'static> {
        if let Some(val) = self.sum {
            val.into()
        } else {
            AnyValue::Null
        }
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}