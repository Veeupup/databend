
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::Result;

use crate::prelude::*;

/**
 * Scalar type
 * 
 * 
 */

pub trait ScalarType2 {
    type NativeType: ?Sized;
    type ColumnType: Column + 'static;
    // type MutableColumnType: MutableColumn<Self::NativeType, Self::ColumnType>;
}

impl ScalarType2 for StringType {
    type NativeType = [u8];
    type ColumnType = StringColumn;
}

impl ScalarType2 for PrimitiveDataType<u8> {
    type NativeType = u8;
    type ColumnType = PrimitiveColumn<u8>;
}

/**
 * MutableColumn
 */

 
/**
 * Viewer
 */

pub trait GetDatas2<E: ?Sized> {
    fn get_data(&self, row: usize) -> &E;
}


pub struct ColumnViewer2<'a, T: ScalarType2> {
    pub column: &'a T::ColumnType,
    // pub data: &'a [T],
    pub validity: Bitmap,               

    // for not nullable column, it's 0. we only need keep one sign bit to tell `null_at` that it's not null.
    // for nullable column, it's usize::max, validity will be cloned from nullable column.
    null_mask: usize,
    // for const column, it's 0, `value` function will fetch the first value of the column.
    // for not const column, it's usize::max, `value` function will fetch the value of the row in the column.
    non_const_mask: usize,
    size: usize,
}



impl<'a, T> ColumnViewer2<'a, T>
where
    T: ScalarType2 + Default,
    T::ColumnType: Clone + GetDatas2<T::NativeType> + 'static,
{
    pub fn create(column: &'a ColumnRef) -> Result<Self> {
        let null_mask = get_null_mask(column);
        let non_const_mask = non_const_mask(column);
        let size = column.len();

        let (column, validity) = if column.is_nullable() {
            let c: &NullableColumn = unsafe { Series::static_cast(column) };
            (c.inner(), c.ensure_validity().clone())
        } else {
            let mut bitmap = MutableBitmap::with_capacity(1);
            bitmap.push(true);

            if column.is_const() {
                let c: &ConstColumn = unsafe { Series::static_cast(column) };
                (c.inner(), bitmap.into())
            } else {
                (column, bitmap.into())
            }
        };

        let column: &T::ColumnType = Series::check_get(column)?;
        // let data = column.get_data();

        Ok(Self {
            column,
            // data,
            validity,
            null_mask,
            non_const_mask,
            size,
        })
    }

    #[inline]
    pub fn valid_at(&self, i: usize) -> bool {
        unsafe { self.validity.get_bit_unchecked(i & self.null_mask) }
    }

    #[inline]
    pub fn null_at(&self, i: usize) -> bool {
        !self.valid_at(i)
    }

    #[inline]
    pub fn value(&self, i: usize) -> &T::NativeType {
        // &self.data[i & self.non_const_mask]
        let x = self.column.get_data(i & self.non_const_mask);
        x
    }

    #[inline]
    pub fn column(&self) -> &T::ColumnType {
        self.column
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
}

#[inline]
fn get_null_mask(column: &ColumnRef) -> usize {
    if !column.is_const() && !column.only_null() && column.is_nullable() {
        usize::MAX
    } else {
        0
    }
}

#[inline]
fn non_const_mask(column: &ColumnRef) -> usize {
    if !column.is_const() && !column.only_null() {
        usize::MAX
    } else {
        0
    }
}

/**
 * MutableColumn for string and primitive
 */



/**
 * test
*/

#[test]
fn test_primitive_wrapper() -> Result<()> {
    let column = Series::from_data(vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let wrapper = ColumnViewer2::<PrimitiveDataType<u8>>::create(&column)?;

    assert_eq!(wrapper.len(), 10);
    assert!(!wrapper.null_at(0));
    for i in 0..wrapper.len() {
        assert_eq!(*wrapper.value(i), (i + 1) as u8);
    }
    Ok(())
}

#[test]
fn test_string_wrapper() -> Result<()> {
    let column = Series::from_data(vec!["aaa".to_string(), "bbb".to_string()]);
    let wrapper = ColumnViewer2::<StringType>::create(&column)?;

    assert_eq!(wrapper.len(), 2);
    assert!(!wrapper.null_at(0));
    assert_eq!(wrapper.value(0), "aaa".as_bytes());
    assert_eq!(wrapper.value(1), "bbb".as_bytes());
    Ok(())
}

