// Copyright 2022 Datafuse Labs.
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

use std::ops::Range;

use super::number::NumberScalar;
use super::timestamp::Timestamp;
use crate::property::Domain;
use crate::types::string::StringColumn;
use crate::types::string::StringColumnBuilder;
use crate::types::string::StringIterator;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::values::Column;
use crate::values::Scalar;
use crate::values::ScalarRef;
use crate::ColumnBuilder;

/// JSONB bytes representation of `null`.
pub const JSONB_NULL: &[u8] = &[0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VariantType;

impl ValueType for VariantType {
    type Scalar = Vec<u8>;
    type ScalarRef<'a> = &'a [u8];
    type Column = StringColumn;
    type Domain = ();
    type ColumnIterator<'a> = StringIterator<'a>;
    type ColumnBuilder = StringColumnBuilder;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: &'long [u8]) -> &'short [u8] {
        long
    }

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        scalar.to_vec()
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        scalar.as_variant().cloned()
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        col.as_variant().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        if domain.is_undefined() {
            Some(())
        } else {
            None
        }
    }

    fn try_downcast_builder<'a>(
        builder: &'a mut ColumnBuilder,
    ) -> Option<&'a mut Self::ColumnBuilder> {
        match builder {
            crate::ColumnBuilder::Variant(builder) => Some(builder),
            _ => None,
        }
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Variant(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Variant(col)
    }

    fn upcast_domain(_domain: Self::Domain) -> Domain {
        Domain::Undefined
    }

    fn column_len<'a>(col: &'a Self::Column) -> usize {
        col.len()
    }

    fn index_column<'a>(col: &'a Self::Column, index: usize) -> Option<Self::ScalarRef<'a>> {
        col.index(index)
    }

    unsafe fn index_column_unchecked<'a>(
        col: &'a Self::Column,
        index: usize,
    ) -> Self::ScalarRef<'a> {
        col.index_unchecked(index)
    }

    fn slice_column<'a>(col: &'a Self::Column, range: Range<usize>) -> Self::Column {
        col.slice(range)
    }

    fn iter_column<'a>(col: &'a Self::Column) -> Self::ColumnIterator<'a> {
        col.iter()
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        StringColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.put_slice(item);
        builder.commit_row();
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.put_slice(JSONB_NULL);
        builder.commit_row();
    }

    fn append_builder(builder: &mut Self::ColumnBuilder, other_builder: &Self::ColumnBuilder) {
        builder.append(other_builder)
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }
}

impl ArgType for VariantType {
    fn data_type() -> DataType {
        DataType::Variant
    }

    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        StringColumnBuilder::with_capacity(capacity, 0)
    }
}

pub fn cast_scalar_to_variant(scalar: ScalarRef, buf: &mut Vec<u8>) {
    let value = match scalar {
        ScalarRef::Null => common_jsonb::Value::Null,
        ScalarRef::EmptyArray => common_jsonb::Value::Array(vec![]),
        ScalarRef::Number(n) => match n {
            NumberScalar::UInt8(n) => common_jsonb::Value::Number(n.into()),
            NumberScalar::UInt16(n) => common_jsonb::Value::Number(n.into()),
            NumberScalar::UInt32(n) => common_jsonb::Value::Number(n.into()),
            NumberScalar::UInt64(n) => common_jsonb::Value::Number(n.into()),
            NumberScalar::Int8(n) => common_jsonb::Value::Number(n.into()),
            NumberScalar::Int16(n) => common_jsonb::Value::Number(n.into()),
            NumberScalar::Int32(n) => common_jsonb::Value::Number(n.into()),
            NumberScalar::Int64(n) => common_jsonb::Value::Number(n.into()),
            // TODO(andylokandy): properly cast Nan and Inf.
            NumberScalar::Float32(n) => {
                n.0.try_into()
                    .map(common_jsonb::Value::Number)
                    .unwrap_or(common_jsonb::Value::Null)
            }
            NumberScalar::Float64(n) => {
                n.0.try_into()
                    .map(common_jsonb::Value::Number)
                    .unwrap_or(common_jsonb::Value::Null)
            }
        },
        ScalarRef::Boolean(b) => common_jsonb::Value::Bool(b),
        ScalarRef::String(s) => common_jsonb::Value::String(String::from_utf8_lossy(s)),
        ScalarRef::Timestamp(Timestamp { ts, .. }) => common_jsonb::Value::Number(ts.into()),
        ScalarRef::Array(col) => {
            let items = cast_scalars_to_variants(col.iter());
            common_jsonb::build_array(items.iter(), buf).expect("failed to build jsonb array");
            return;
        }
        ScalarRef::Tuple(fields) => {
            let values = cast_scalars_to_variants(fields);
            common_jsonb::build_object(
                values
                    .iter()
                    .enumerate()
                    .map(|(i, bytes)| (format!("{i}"), bytes)),
                buf,
            )
            .expect("failed to build jsonb object");
            return;
        }
        ScalarRef::Variant(bytes) => {
            buf.extend_from_slice(bytes);
            return;
        }
    };
    value.to_vec(buf);
}

pub fn cast_scalars_to_variants(scalars: impl IntoIterator<Item = ScalarRef>) -> StringColumn {
    let iter = scalars.into_iter();
    let mut builder = StringColumnBuilder::with_capacity(iter.size_hint().0, 0);
    for scalar in iter {
        cast_scalar_to_variant(scalar, &mut builder.data);
        builder.commit_row();
    }
    builder.build()
}
