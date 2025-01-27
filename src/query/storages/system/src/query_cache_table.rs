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

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::types::UInt64Type;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_storages_result_cache::gen_result_cache_prefix;
use common_storages_result_cache::ResultCacheMetaManager;
use common_users::UserApiProvider;
use itertools::Itertools;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct QueryCacheTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for QueryCacheTable {
    const NAME: &'static str = "system.query_cache";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let meta_client = UserApiProvider::instance().get_meta_store_client();
        let result_cache_mgr = ResultCacheMetaManager::create(meta_client, 0);
        let tenant = ctx.get_tenant();
        let prefix = gen_result_cache_prefix(&tenant);

        let cached_values = result_cache_mgr.list(prefix.as_str()).await?;

        let mut sql_vec: Vec<&str> = Vec::with_capacity(cached_values.len());
        let mut result_size_vec = Vec::with_capacity(cached_values.len());
        let mut num_rows_vec = Vec::with_capacity(cached_values.len());
        let mut partitions_sha_vec = Vec::with_capacity(cached_values.len());
        let mut location_vec = Vec::with_capacity(cached_values.len());

        cached_values.iter().for_each(|x| {
            sql_vec.push(x.sql.as_str());
            result_size_vec.push(x.result_size as u64);
            num_rows_vec.push(x.num_rows as u64);
            partitions_sha_vec.push(x.partitions_shas.clone());
            location_vec.push(x.location.as_str());
        });

        let partitions_sha_vec: Vec<String> = partitions_sha_vec
            .into_iter()
            .map(|part| part.into_iter().join(", "))
            .collect();

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(sql_vec),
            UInt64Type::from_data(result_size_vec),
            UInt64Type::from_data(num_rows_vec),
            StringType::from_data(
                partitions_sha_vec
                    .iter()
                    .map(|part_sha| part_sha.as_str())
                    .collect::<Vec<_>>(),
            ),
            StringType::from_data(location_vec),
        ]))
    }
}

impl QueryCacheTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("sql", TableDataType::String),
            TableField::new("result_size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("num_rows", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "partitions_sha",
                TableDataType::Array(Box::new(TableDataType::String)),
            ),
            TableField::new("location", TableDataType::String),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'query_cache'".to_string(),
            name: "query_cache".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemQueryCache".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(QueryCacheTable { table_info })
    }
}
