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

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::marker::Sync; 
use std::marker::Send;

use common_datablocks::DataBlock;
use common_datavalues::Column;
use common_datavalues::ColumnRef;
use common_datavalues::ConstColumn;
use common_datavalues::DataSchemaRef;
use common_datavalues::PrimitiveType;
use common_exception::Result;
use common_planners::Expression;

use crate::common::ExpressionEvaluator;
use crate::common::HashMap;
use crate::common::HashTableKeyable;
use crate::pipelines::new::processors::transforms::hash_join::hash::HashUtil;
use crate::pipelines::new::processors::transforms::hash_join::hash::HashVector;
use crate::pipelines::new::processors::transforms::hash_join::row::compare_and_combine;
use crate::pipelines::new::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::new::processors::transforms::hash_join::row::RowSpace;
use crate::pipelines::new::processors::HashJoinState;
use crate::sessions::QueryContext;

pub struct NewHashTable<T: HashTableKeyable> {
    /// Reference count
    ref_count: Mutex<usize>,
    is_finished: Mutex<bool>,

    build_expressions: Vec<Expression>,
    probe_expressions: Vec<Expression>,

    ctx: Arc<QueryContext>,

    /// A shared big hash table stores all the rows from build side
    // hash_table: RwLock<Vec<Vec<RowPtr>>>,
    hash_table: RwLock<HashMap<T, usize>>,
    row_space: RowSpace,
}

// Hack for HashJoinState: Send + Sync
unsafe impl<T: HashTableKeyable + Send> Send for NewHashTable<T> {}

unsafe impl<T: HashTableKeyable + Sync> Sync for NewHashTable<T> {}

impl<T: HashTableKeyable> HashJoinState for NewHashTable<T>
where T: PrimitiveType {
    fn build(&self, input: DataBlock) -> Result<()> {
        todo!()
    }

    fn probe(&self, input: &DataBlock) -> Result<Vec<DataBlock>> {
        todo!()
    }

    fn attach(&self) -> Result<()> {
        todo!()
    }

    fn detach(&self) -> Result<()> {
        todo!()
    }

    fn is_finished(&self) -> Result<bool> {
        todo!()
    }

    fn finish(&self) -> Result<()> {
        todo!()
    }
}
