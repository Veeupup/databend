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

use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::{DataField, DataTypeAndNullable};
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct ToTypeNameFunction {
    _display_name: String,
}

impl ToTypeNameFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(ToTypeNameFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl Function for ToTypeNameFunction {
    fn name(&self) -> &str {
        "ToTypeNameFunction"
    }

    fn return_type(&self, args: &[DataTypeAndNullable]) -> Result<DataType> {
        Ok(DataType::String)
    }

    fn nullable(&self, _args: &[DataTypeAndNullable]) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let type_name = format!("{}", columns[0].data_type());
        Ok(DataColumn::Constant(
            DataValue::String(Some(type_name.into_bytes())),
            input_rows,
        ))
    }
}

impl fmt::Display for ToTypeNameFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "toTypeName")
    }
}
