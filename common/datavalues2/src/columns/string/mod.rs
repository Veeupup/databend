// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod iterator;
mod mutable;

use std::sync::Arc;

use common_arrow::arrow::array::*;
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::compute::cast::binary_to_large_binary;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::types::Index;
pub use iterator::*;
pub use mutable::*;

use crate::prelude::*;
use super::vee_experiment::*;

// TODO adaptive offset
#[derive(Debug, Clone)]
pub struct StringColumn {
    offsets: Buffer<i64>,
    values: Buffer<u8>,
}

impl From<LargeBinaryArray> for StringColumn {
    fn from(array: LargeBinaryArray) -> Self {
        Self {
            offsets: array.offsets().clone(),
            values: array.values().clone(),
        }
    }
}

impl StringColumn {
    pub fn new(array: LargeBinaryArray) -> Self {
        Self {
            offsets: array.offsets().clone(),
            values: array.values().clone(),
        }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        let arrow_type = array.data_type();
        if arrow_type == &ArrowType::Binary {
            let arr = array.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();
            let arr = binary_to_large_binary(arr, ArrowType::LargeBinary);
            return Self::new(arr);
        }

        if arrow_type == &ArrowType::Utf8 {
            let arr = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            let iter = arr.values_iter();
            return Self::new_from_iter(iter);
        }

        if arrow_type == &ArrowType::LargeUtf8 {
            let arr = array.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            let iter = arr.values_iter();
            return Self::new_from_iter(iter);
        }

        Self::new(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .clone(),
        )
    }

    /// construct StringColumn from unchecked data
    /// # Safety
    /// just like BinaryArray::from_data_unchecked, as follows
    /// * `offsets` MUST be monotonically increasing
    /// # Panics
    /// This function panics if:
    /// * The last element of `offsets` is different from `values.len()`.
    /// * The validity is not `None` and its length is different from `offsets.len() - 1`.
    pub unsafe fn from_data_unchecked(offsets: Buffer<i64>, values: Buffer<u8>) -> Self {
        Self { offsets, values }
    }

    /// Returns the element at index `i`
    /// # Safety
    /// Assumes that the `i < self.len`.
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> &[u8] {
        // soundness: the invariant of the function
        let start = self.offsets.get_unchecked(i).to_usize();
        let end = self.offsets.get_unchecked(i + 1).to_usize();
        // soundness: the invariant of the struct
        self.values.get_unchecked(start..end)
    }

    pub fn to_binary_array(&self) -> BinaryArray<i64> {
        unsafe {
            BinaryArray::from_data_unchecked(
                ArrowType::LargeBinary,
                self.offsets.clone(),
                self.values.clone(),
                None,
            )
        }
    }

    #[inline]
    pub fn size_at_index(&self, i: usize) -> usize {
        let offset = self.offsets[i];
        let offset_1 = self.offsets[i + 1];
        (offset_1 - offset).to_usize()
    }

    pub fn values(&self) -> &[u8] {
        self.values.as_slice()
    }

    pub fn offsets(&self) -> &[i64] {
        self.offsets.as_slice()
    }
}

impl Column for StringColumn {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> DataTypePtr {
        StringType::arc()
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn memory_size(&self) -> usize {
        self.values.len() + self.offsets.len() * std::mem::size_of::<i64>()
    }

    fn as_arrow_array(&self) -> ArrayRef {
        Arc::new(LargeBinaryArray::from_data(
            ArrowType::LargeBinary,
            self.offsets.clone(),
            self.values.clone(),
            None,
        ))
    }

    fn slice(&self, offset: usize, length: usize) -> ColumnRef {
        let offsets = unsafe { self.offsets.clone().slice_unchecked(offset, length + 1) };

        Arc::new(Self {
            offsets,
            values: self.values.clone(),
        })
    }

    fn replicate(&self, offsets: &[usize]) -> ColumnRef {
        debug_assert!(
            offsets.len() == self.len(),
            "Size of offsets must match size of column"
        );

        if offsets.is_empty() {
            return self.slice(0, 0);
        }

        let max_size = offsets.iter().max().unwrap();
        let mut builder = MutableStringColumn::with_capacity(*max_size);

        let mut previous_offset: usize = 0;

        (0..self.len()).for_each(|i| {
            let offset: usize = offsets[i];
            let data = unsafe { self.value_unchecked(i) };
            for _ in previous_offset..offset {
                builder.append_value(data);
            }
            previous_offset = offset;
        });
        builder.as_column()
    }

    fn convert_full_column(&self) -> ColumnRef {
        Arc::new(self.clone())
    }

    unsafe fn get_unchecked(&self, index: usize) -> DataValue {
        let start = self.offsets[index].to_usize();
        let end = self.offsets[index + 1].to_usize();

        // soundness: the invariant of the struct
        let str = self.values.get_unchecked(start..end);
        DataValue::String(str.to_vec())
    }
}

impl GetDatas2<[u8]> for StringColumn {
    fn get_data(&self, row: usize) -> &[u8] {
        unsafe {
            // soundness: the invariant of the function
            let start = self.offsets.get_unchecked(row).to_usize();
            let end = self.offsets.get_unchecked(row + 1).to_usize();
            // soundness: the invariant of the struct
            let x = self.values.get_unchecked(start..end);
            x
        }
    }
}
