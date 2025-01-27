// Copyright 2023 Datafuse Labs.
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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;

use crate::api::ExchangeSorting;
use crate::pipelines::processors::transforms::aggregator::AggregateInfo;

pub struct AggregateExchangeSorting {}

impl AggregateExchangeSorting {
    pub fn create() -> Arc<dyn ExchangeSorting> {
        Arc::new(AggregateExchangeSorting {})
    }
}

impl ExchangeSorting for AggregateExchangeSorting {
    fn block_number(&self, data_block: &DataBlock) -> Result<isize> {
        match data_block.get_meta() {
            None => Ok(-1),
            Some(block_meta_info) => match block_meta_info.as_any().downcast_ref::<AggregateInfo>()
            {
                None => Err(ErrorCode::Internal(
                    "Internal error, AggregateExchangeSorting only recv AggregateInfo",
                )),
                Some(meta_info) => Ok(meta_info.bucket),
            },
        }
    }
}
