// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::option::Option::Some;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::{
    DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_SUPER_USER_ID, PG_CATALOG_SCHEMA_NAME,
};
use risingwave_common::types::ParallelUnitId;
use risingwave_common::{bail, ensure};
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::{Database, Index, Schema, Sink, Source, Table};
use risingwave_pb::common::ParallelUnit;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use tokio::sync::{Mutex, MutexGuard};

use super::IdCategory;
use crate::manager::{MetaSrvEnv, NotificationVersion, Relation};
use crate::model::{MetadataModel, TableFragments, Transactional};
use crate::storage::{MetaStore, Transaction};
use crate::{MetaError, MetaResult};

pub type DatabaseId = u32;
pub type SchemaId = u32;
pub type TableId = u32;
pub type SourceId = u32;
pub type SinkId = u32;
pub type RelationId = u32;
pub type IndexId = u32;

pub type Catalog = (
    Vec<Database>,
    Vec<Schema>,
    Vec<Table>,
    Vec<Source>,
    Vec<Sink>,
    Vec<Index>,
);

pub struct CatalogManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    core: Mutex<CatalogManagerCore<S>>,
}

pub type CatalogManagerRef<S> = Arc<CatalogManager<S>>;

impl<S> CatalogManager<S>
where
    S: MetaStore,
{
    pub async fn new(env: MetaSrvEnv<S>) -> MetaResult<Self> {
        let catalog_manager = Self {
            core: Mutex::new(CatalogManagerCore::new(env.clone()).await?),
            env,
        };
        catalog_manager.init().await?;
        Ok(catalog_manager)
    }

    // Create default database and schema.
    async fn init(&self) -> MetaResult<()> {
        let mut database = Database {
            name: DEFAULT_DATABASE_NAME.to_string(),
            owner: DEFAULT_SUPER_USER_ID,
            ..Default::default()
        };
        if !self.core.lock().await.has_database(&database) {
            database.id = self
                .env
                .id_gen_manager()
                .generate::<{ IdCategory::Database }>()
                .await? as u32;
            self.create_database(&database).await?;
        }
        let databases = Database::list(self.env.meta_store())
            .await?
            .into_iter()
            .filter(|db| db.name == DEFAULT_DATABASE_NAME)
            .collect::<Vec<Database>>();
        assert_eq!(1, databases.len());

        for name in [DEFAULT_SCHEMA_NAME, PG_CATALOG_SCHEMA_NAME] {
            let mut schema = Schema {
                name: name.to_string(),
                database_id: databases[0].id,
                owner: DEFAULT_SUPER_USER_ID,
                ..Default::default()
            };
            if !self.core.lock().await.has_schema(&schema) {
                schema.id = self
                    .env
                    .id_gen_manager()
                    .generate::<{ IdCategory::Schema }>()
                    .await? as u32;
                self.create_schema(&schema).await?;
            }
        }
        Ok(())
    }

    /// Used in `NotificationService::subscribe`.
    /// Need to pay attention to the order of acquiring locks to prevent deadlock problems.
    pub async fn get_catalog_core_guard(&self) -> MutexGuard<'_, CatalogManagerCore<S>> {
        self.core.lock().await
    }

    pub async fn get_catalog(&self) -> MetaResult<Catalog> {
        let core = self.core.lock().await;
        core.get_catalog().await
    }

    pub async fn create_database(&self, database: &Database) -> MetaResult<NotificationVersion> {
        let mut core = self.core.lock().await;
        if !core.has_database(database) {
            let mut transaction = Transaction::default();
            database.upsert_in_transaction(&mut transaction)?;
            let mut schemas = vec![];
            for schema_name in [DEFAULT_SCHEMA_NAME, PG_CATALOG_SCHEMA_NAME] {
                let schema = Schema {
                    id: self
                        .env
                        .id_gen_manager()
                        .generate::<{ IdCategory::Schema }>()
                        .await? as u32,
                    database_id: database.id,
                    name: schema_name.to_string(),
                    owner: database.owner,
                };
                schema.upsert_in_transaction(&mut transaction)?;
                schemas.push(schema);
            }
            self.env.meta_store().txn(transaction).await?;

            core.add_database(database);
            let mut version = self
                .notify_frontend(Operation::Add, Info::Database(database.to_owned()))
                .await;
            for schema in schemas {
                core.add_schema(&schema);
                version = self
                    .env
                    .notification_manager()
                    .notify_frontend(Operation::Add, Info::Schema(schema))
                    .await;
            }

            Ok(version)
        } else {
            bail!("database already exists");
        }
    }

    pub async fn drop_database(&self, database_id: DatabaseId) -> MetaResult<NotificationVersion> {
        let mut core = self.core.lock().await;
        let database = Database::select(self.env.meta_store(), &database_id).await?;
        if let Some(database) = database {
            let schemas = Schema::list(self.env.meta_store()).await?;
            let schemas = schemas
                .iter()
                .filter(|schema| {
                    schema.database_id == database_id && schema.name == PG_CATALOG_SCHEMA_NAME
                })
                .collect_vec();
            assert_eq!(1, schemas.len());
            let mut transaction = Transaction::default();
            database.delete_in_transaction(&mut transaction)?;
            schemas[0].delete_in_transaction(&mut transaction)?;
            self.env.meta_store().txn(transaction).await?;
            core.drop_schema(schemas[0]);
            core.drop_database(&database);

            let version = self
                .notify_frontend(Operation::Delete, Info::Database(database))
                .await;

            Ok(version)
        } else {
            bail!("database doesn't exist");
        }
    }

    pub async fn create_schema(&self, schema: &Schema) -> MetaResult<NotificationVersion> {
        let mut core = self.core.lock().await;
        if !core.has_schema(schema) {
            schema.insert(self.env.meta_store()).await?;
            core.add_schema(schema);

            let version = self
                .notify_frontend(Operation::Add, Info::Schema(schema.to_owned()))
                .await;

            Ok(version)
        } else {
            bail!("schema already exists");
        }
    }

    pub async fn drop_schema(&self, schema_id: SchemaId) -> MetaResult<NotificationVersion> {
        let mut core = self.core.lock().await;
        let schema = Schema::select(self.env.meta_store(), &schema_id).await?;
        if let Some(schema) = schema {
            Schema::delete(self.env.meta_store(), &schema_id).await?;
            core.drop_schema(&schema);

            let version = self
                .notify_frontend(Operation::Delete, Info::Schema(schema))
                .await;

            Ok(version)
        } else {
            bail!("schema doesn't exist");
        }
    }

    pub async fn start_create_procedure(&self, relation: &Relation) -> MetaResult<()> {
        match relation {
            Relation::Table(table) => self.start_create_table_procedure(table).await,
            Relation::Sink(sink) => self.start_create_sink_procedure(sink).await,
            Relation::Index(index, index_table) => {
                self.start_create_index_procedure(index, index_table).await
            }
        }
    }

    pub async fn cancel_create_procedure(&self, relation: &Relation) -> MetaResult<()> {
        match relation {
            Relation::Table(table) => self.cancel_create_table_procedure(table).await,
            Relation::Sink(sink) => self.cancel_create_sink_procedure(sink).await,
            Relation::Index(index, index_table) => {
                self.cancel_create_index_procedure(index, index_table).await
            }
        }
    }

    pub async fn finish_create_procedure(
        &self,
        internal_tables: Option<Vec<Table>>,
        relation: &Relation,
    ) -> MetaResult<NotificationVersion> {
        match relation {
            Relation::Table(table) => {
                self.finish_create_table_procedure(internal_tables.unwrap(), table)
                    .await
            }
            Relation::Sink(sink) => self.finish_create_sink_procedure(sink).await,
            Relation::Index(index, index_table) => {
                self.finish_create_index_procedure(index, internal_tables.unwrap(), index_table)
                    .await
            }
        }
    }

    pub async fn start_create_table_procedure(&self, table: &Table) -> MetaResult<()> {
        let mut core = self.core.lock().await;
        let key = (table.database_id, table.schema_id, table.name.clone());
        if !core.has_table(table) && !core.has_in_progress_creation(&key) {
            core.mark_creating(&key);
            for &dependent_relation_id in &table.dependent_relations {
                core.increase_ref_count(dependent_relation_id);
            }
            Ok(())
        } else {
            bail!("table already exists or in creating procedure");
        }
    }

    pub async fn finish_create_table_procedure(
        &self,
        internal_tables: Vec<Table>,
        table: &Table,
    ) -> MetaResult<NotificationVersion> {
        let mut core = self.core.lock().await;
        let key = (table.database_id, table.schema_id, table.name.clone());
        if !core.has_table(table) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            let mut transaction = Transaction::default();
            for table in &internal_tables {
                table.upsert_in_transaction(&mut transaction)?;
            }
            table.upsert_in_transaction(&mut transaction)?;
            core.env.meta_store().txn(transaction).await?;

            for internal_table in internal_tables {
                core.add_table(&internal_table);

                self.notify_frontend(Operation::Add, Info::Table(internal_table.to_owned()))
                    .await;
            }
            core.add_table(table);
            let version = self
                .broadcast_info_op(Operation::Add, Info::Table(table.to_owned()))
                .await;

            Ok(version)
        } else {
            bail!("table already exist or not in creating procedure");
        }
    }

    pub async fn cancel_create_table_procedure(&self, table: &Table) -> MetaResult<()> {
        let mut core = self.core.lock().await;
        let key = (table.database_id, table.schema_id, table.name.clone());
        if !core.has_table(table) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            for &dependent_relation_id in &table.dependent_relations {
                core.decrease_ref_count(dependent_relation_id);
            }
            Ok(())
        } else {
            bail!("table already exist or not in creating procedure");
        }
    }

    pub async fn drop_table(
        &self,
        table_id: TableId,
        internal_table_ids: Vec<TableId>,
    ) -> MetaResult<NotificationVersion> {
        let mut core = self.core.lock().await;
        let table = Table::select(self.env.meta_store(), &table_id).await?;
        if let Some(table) = table {
            match core.get_ref_count(table_id) {
                Some(ref_count) => Err(MetaError::permission_denied(format!(
                    "Fail to delete table `{}` because {} other relation(s) depend on it",
                    table.name, ref_count
                ))),
                None => {
                    let mut transaction = Transaction::default();
                    table.delete_in_transaction(&mut transaction)?;
                    let mut tables_to_drop = vec![];
                    for internal_table_id in internal_table_ids {
                        let internal_table =
                            Table::select(self.env.meta_store(), &internal_table_id).await?;
                        if let Some(internal_table) = internal_table {
                            internal_table.delete_in_transaction(&mut transaction)?;
                            tables_to_drop.push(internal_table);
                        }
                    }
                    core.env.meta_store().txn(transaction).await?;
                    for table in tables_to_drop {
                        core.drop_table(&table);
                        self.broadcast_info_op(Operation::Delete, Info::Table(table))
                            .await;
                    }
                    core.drop_table(&table);
                    for &dependent_relation_id in &table.dependent_relations {
                        core.decrease_ref_count(dependent_relation_id);
                    }

                    let version = self
                        .broadcast_info_op(Operation::Delete, Info::Table(table.to_owned()))
                        .await;

                    Ok(version)
                }
            }
        } else {
            bail!("table doesn't exist");
        }
    }

    pub async fn get_index_table(&self, index_id: IndexId) -> MetaResult<TableId> {
        let index = Index::select(self.env.meta_store(), &index_id).await?;
        if let Some(index) = index {
            Ok(index.index_table_id)
        } else {
            bail!("index doesn't exist");
        }
    }

    pub async fn drop_index(
        &self,
        index_id: IndexId,
        index_table_id: TableId,
        internal_table_ids: Vec<TableId>,
    ) -> MetaResult<NotificationVersion> {
        let mut core = self.core.lock().await;
        let index = Index::select(self.env.meta_store(), &index_id).await?;
        if let Some(index) = index {
            let mut transaction = Transaction::default();
            index.delete_in_transaction(&mut transaction)?;
            let mut tables_to_drop = vec![];
            assert_eq!(index_table_id, index.index_table_id);

            // drop index table
            let table = Table::select(self.env.meta_store(), &index_table_id).await?;
            if let Some(table) = table {
                match core.get_ref_count(index_table_id) {
                    Some(ref_count) => Err(MetaError::permission_denied(format!(
                        "Fail to delete table `{}` because {} other relation(s) depend on it",
                        table.name, ref_count
                    ))),
                    None => {
                        table.delete_in_transaction(&mut transaction)?;
                        for internal_table_id in internal_table_ids {
                            let internal_table =
                                Table::select(self.env.meta_store(), &internal_table_id).await?;
                            if let Some(internal_table) = internal_table {
                                internal_table.delete_in_transaction(&mut transaction)?;
                                tables_to_drop.push(internal_table);
                            }
                        }

                        core.env.meta_store().txn(transaction).await?;
                        core.drop_index(&index);
                        core.drop_table(&table);
                        for table in tables_to_drop {
                            core.drop_table(&table);
                            self.broadcast_info_op(Operation::Delete, Info::Table(table))
                                .await;
                        }
                        for &dependent_relation_id in &table.dependent_relations {
                            core.decrease_ref_count(dependent_relation_id);
                        }

                        self.env
                            .notification_manager()
                            .notify_frontend(Operation::Delete, Info::Index(index.to_owned()))
                            .await;

                        let version = self
                            .broadcast_info_op(Operation::Delete, Info::Table(table.to_owned()))
                            .await;

                        Ok(version)
                    }
                }
            } else {
                bail!("index table doesn't exist",)
            }
        } else {
            bail!("index doesn't exist",)
        }
    }

    pub async fn start_create_source_procedure(&self, source: &Source) -> MetaResult<()> {
        let mut core = self.core.lock().await;
        let key = (source.database_id, source.schema_id, source.name.clone());
        if !core.has_source(source) && !core.has_in_progress_creation(&key) {
            core.mark_creating(&key);
            Ok(())
        } else {
            bail!("source already exists or in creating procedure");
        }
    }

    pub async fn finish_create_source_procedure(
        &self,
        source: &Source,
    ) -> MetaResult<NotificationVersion> {
        let mut core = self.core.lock().await;
        let key = (source.database_id, source.schema_id, source.name.clone());
        if !core.has_source(source) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            source.insert(self.env.meta_store()).await?;
            core.add_source(source);

            let version = self
                .broadcast_info_op(Operation::Add, Info::Source(source.to_owned()))
                .await;

            Ok(version)
        } else {
            bail!("source already exist or not in creating procedure");
        }
    }

    pub async fn cancel_create_source_procedure(&self, source: &Source) -> MetaResult<()> {
        let mut core = self.core.lock().await;
        let key = (source.database_id, source.schema_id, source.name.clone());
        if !core.has_source(source) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            Ok(())
        } else {
            bail!("source already exist or not in creating procedure");
        }
    }

    pub async fn update_table_mapping(
        &self,
        fragments: &Vec<TableFragments>,
        migrate_map: &HashMap<ParallelUnitId, ParallelUnit>,
    ) -> MetaResult<()> {
        let mut core = self.core.lock().await;
        let mut transaction = Transaction::default();
        let mut tables = Vec::new();
        for fragment in fragments {
            let table_id = fragment.table_id().table_id();
            let internal_tables = fragment.internal_table_ids();
            let mut table_to_updates = vec![table_id];
            table_to_updates.extend(internal_tables);
            for table_id in table_to_updates {
                let table = Table::select(self.env.meta_store(), &table_id).await?;
                if let Some(mut table) = table {
                    if let Some(ref mut mapping) = table.mapping {
                        let mut migrated = false;
                        mapping.data.iter_mut().for_each(|id| {
                            if migrate_map.contains_key(id) {
                                migrated = true;
                                *id = migrate_map.get(id).unwrap().id;
                            }
                        });
                        if migrated {
                            table.upsert_in_transaction(&mut transaction)?;
                            tables.push(table);
                        }
                    }
                }
            }
        }
        core.env.meta_store().txn(transaction).await?;
        for table in &tables {
            self.broadcast_info_op(Operation::Update, Info::Table(table.to_owned()))
                .await;
            core.add_table(table);
        }
        Ok(())
    }

    pub async fn drop_source(&self, source_id: SourceId) -> MetaResult<NotificationVersion> {
        let mut core = self.core.lock().await;
        let source = Source::select(self.env.meta_store(), &source_id).await?;
        if let Some(source) = source {
            match core.get_ref_count(source_id) {
                Some(ref_count) => Err(MetaError::permission_denied(format!(
                    "Fail to delete source `{}` because {} other relation(s) depend on it",
                    source.name, ref_count
                ))),
                None => {
                    Source::delete(self.env.meta_store(), &source_id).await?;
                    core.drop_source(&source);

                    let version = self
                        .broadcast_info_op(Operation::Delete, Info::Source(source))
                        .await;

                    Ok(version)
                }
            }
        } else {
            bail!("source doesn't exist");
        }
    }

    pub async fn start_create_materialized_source_procedure(
        &self,
        source: &Source,
        mview: &Table,
    ) -> MetaResult<()> {
        let mut core = self.core.lock().await;
        let source_key = (source.database_id, source.schema_id, source.name.clone());
        let mview_key = (mview.database_id, mview.schema_id, mview.name.clone());
        if !core.has_source(source)
            && !core.has_table(mview)
            && !core.has_in_progress_creation(&source_key)
            && !core.has_in_progress_creation(&mview_key)
        {
            core.mark_creating(&source_key);
            core.mark_creating(&mview_key);
            ensure!(mview.dependent_relations.is_empty());
            Ok(())
        } else {
            bail!("source or table already exist");
        }
    }

    pub async fn finish_create_materialized_source_procedure(
        &self,
        source: &Source,
        mview: &Table,
        tables: Vec<Table>,
    ) -> MetaResult<NotificationVersion> {
        let mut core = self.core.lock().await;
        let source_key = (source.database_id, source.schema_id, source.name.clone());
        let mview_key = (mview.database_id, mview.schema_id, mview.name.clone());
        if !core.has_source(source)
            && !core.has_table(mview)
            && core.has_in_progress_creation(&source_key)
            && core.has_in_progress_creation(&mview_key)
        {
            core.unmark_creating(&source_key);
            core.unmark_creating(&mview_key);

            let mut transaction = Transaction::default();
            source.upsert_in_transaction(&mut transaction)?;
            mview.upsert_in_transaction(&mut transaction)?;
            for table in &tables {
                table.upsert_in_transaction(&mut transaction)?;
            }
            core.env.meta_store().txn(transaction).await?;

            core.add_source(source);
            core.add_table(mview);

            for table in tables {
                core.add_table(&table);
                self.notify_frontend(Operation::Add, Info::Table(table.to_owned()))
                    .await;
            }
            self.broadcast_info_op(Operation::Add, Info::Table(mview.to_owned()))
                .await;

            // Currently frontend uses source's version
            let version = self
                .broadcast_info_op(Operation::Add, Info::Source(source.to_owned()))
                .await;
            Ok(version)
        } else {
            bail!("source already exist or not in creating procedure");
        }
    }

    pub async fn cancel_create_materialized_source_procedure(
        &self,
        source: &Source,
        mview: &Table,
    ) -> MetaResult<()> {
        let mut core = self.core.lock().await;
        let source_key = (source.database_id, source.schema_id, source.name.clone());
        let mview_key = (mview.database_id, mview.schema_id, mview.name.clone());
        if !core.has_source(source)
            && !core.has_table(mview)
            && core.has_in_progress_creation(&source_key)
            && core.has_in_progress_creation(&mview_key)
        {
            core.unmark_creating(&source_key);
            core.unmark_creating(&mview_key);
            Ok(())
        } else {
            bail!("source already exist or not in creating procedure");
        }
    }

    pub async fn drop_materialized_source(
        &self,
        source_id: SourceId,
        mview_id: TableId,
    ) -> MetaResult<NotificationVersion> {
        let mut core = self.core.lock().await;
        let mview = Table::select(self.env.meta_store(), &mview_id).await?;
        let source = Source::select(self.env.meta_store(), &source_id).await?;
        match (mview, source) {
            (Some(mview), Some(source)) => {
                // decrease associated source's ref count first to avoid deadlock
                if let Some(OptionalAssociatedSourceId::AssociatedSourceId(associated_source_id)) =
                    mview.optional_associated_source_id
                {
                    if associated_source_id != source_id {
                        bail!("mview's associated source id doesn't match source id");
                    }
                } else {
                    bail!("mview do not have associated source id");
                }
                // check ref count
                if let Some(ref_count) = core.get_ref_count(mview_id) {
                    return Err(MetaError::permission_denied(format!(
                        "Fail to delete table `{}` because {} other relation(s) depend on it",
                        mview.name, ref_count
                    )));
                }
                if let Some(ref_count) = core.get_ref_count(source_id) {
                    return Err(MetaError::permission_denied(format!(
                        "Fail to delete source `{}` because {} other relation(s) depend on it",
                        source.name, ref_count
                    )));
                }

                // now is safe to delete both mview and source
                let mut transaction = Transaction::default();
                mview.delete_in_transaction(&mut transaction)?;
                source.delete_in_transaction(&mut transaction)?;
                core.env.meta_store().txn(transaction).await?;
                core.drop_table(&mview);
                core.drop_source(&source);
                for &dependent_relation_id in &mview.dependent_relations {
                    core.decrease_ref_count(dependent_relation_id);
                }

                self.notify_frontend(Operation::Delete, Info::Table(mview.to_owned()))
                    .await;

                let version = self
                    .notify_frontend(Operation::Delete, Info::Source(source))
                    .await;

                Ok(version)
            }

            _ => bail!("table or source doesn't exist"),
        }
    }

    pub async fn start_create_index_procedure(
        &self,
        index: &Index,
        index_table: &Table,
    ) -> MetaResult<()> {
        let mut core = self.core.lock().await;
        let key = (index.database_id, index.schema_id, index.name.clone());
        if !core.has_index(index) && !core.has_in_progress_creation(&key) {
            core.mark_creating(&key);
            for &dependent_relation_id in &index_table.dependent_relations {
                core.increase_ref_count(dependent_relation_id);
            }
            Ok(())
        } else {
            bail!("index already exists or in creating procedure".to_string(),)
        }
    }

    pub async fn cancel_create_index_procedure(
        &self,
        index: &Index,
        index_table: &Table,
    ) -> MetaResult<()> {
        let mut core = self.core.lock().await;
        let key = (index.database_id, index.schema_id, index.name.clone());
        if !core.has_index(index) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            for &dependent_relation_id in &index_table.dependent_relations {
                core.decrease_ref_count(dependent_relation_id);
            }
            Ok(())
        } else {
            bail!("index already exist or not in creating procedure",)
        }
    }

    pub async fn finish_create_index_procedure(
        &self,
        index: &Index,
        internal_tables: Vec<Table>,
        table: &Table,
    ) -> MetaResult<NotificationVersion> {
        let mut core = self.core.lock().await;
        let key = (table.database_id, table.schema_id, table.name.clone());
        if !core.has_index(index) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            let mut transaction = Transaction::default();

            index.upsert_in_transaction(&mut transaction)?;

            for table in &internal_tables {
                table.upsert_in_transaction(&mut transaction)?;
            }
            table.upsert_in_transaction(&mut transaction)?;
            core.env.meta_store().txn(transaction).await?;

            for internal_table in internal_tables {
                core.add_table(&internal_table);

                self.broadcast_info_op(Operation::Add, Info::Table(internal_table.to_owned()))
                    .await;
            }
            core.add_table(table);
            core.add_index(index);

            self.broadcast_info_op(Operation::Add, Info::Table(table.to_owned()))
                .await;

            let version = self
                .env
                .notification_manager()
                .notify_frontend(Operation::Add, Info::Index(index.to_owned()))
                .await;

            Ok(version)
        } else {
            bail!("table already exist or not in creating procedure",)
        }
    }

    pub async fn start_create_sink_procedure(&self, sink: &Sink) -> MetaResult<()> {
        let mut core = self.core.lock().await;
        let key = (sink.database_id, sink.schema_id, sink.name.clone());
        if !core.has_sink(sink) && !core.has_in_progress_creation(&key) {
            core.mark_creating(&key);
            for &dependent_relation_id in &sink.dependent_relations {
                core.increase_ref_count(dependent_relation_id);
            }
            Ok(())
        } else {
            bail!("sink already exists or in creating procedure");
        }
    }

    pub async fn finish_create_sink_procedure(
        &self,
        sink: &Sink,
    ) -> MetaResult<NotificationVersion> {
        let mut core = self.core.lock().await;
        let key = (sink.database_id, sink.schema_id, sink.name.clone());
        if !core.has_sink(sink) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            sink.insert(self.env.meta_store()).await?;
            core.add_sink(sink);

            let version = self
                .notify_frontend(Operation::Add, Info::Sink(sink.to_owned()))
                .await;

            Ok(version)
        } else {
            bail!("sink already exist or not in creating procedure");
        }
    }

    pub async fn cancel_create_sink_procedure(&self, sink: &Sink) -> MetaResult<()> {
        let mut core = self.core.lock().await;
        let key = (sink.database_id, sink.schema_id, sink.name.clone());
        if !core.has_sink(sink) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            Ok(())
        } else {
            bail!("sink already exist or not in creating procedure");
        }
    }

    pub async fn create_sink(&self, sink: &Sink) -> MetaResult<NotificationVersion> {
        let mut core = self.core.lock().await;
        if !core.has_sink(sink) {
            sink.insert(self.env.meta_store()).await?;
            core.add_sink(sink);

            let version = self
                .notify_frontend(Operation::Add, Info::Sink(sink.to_owned()))
                .await;

            Ok(version)
        } else {
            bail!("sink already exists");
        }
    }

    pub async fn drop_sink(&self, sink_id: SinkId) -> MetaResult<NotificationVersion> {
        let mut core = self.core.lock().await;
        let sink = Sink::select(self.env.meta_store(), &sink_id).await?;
        if let Some(sink) = sink {
            Sink::delete(self.env.meta_store(), &sink_id).await?;
            core.drop_sink(&sink);
            for &dependent_relation_id in &sink.dependent_relations {
                core.decrease_ref_count(dependent_relation_id);
            }

            let version = self
                .notify_frontend(Operation::Delete, Info::Sink(sink))
                .await;

            Ok(version)
        } else {
            bail!("sink doesn't exist");
        }
    }

    pub async fn list_tables(&self, schema_id: SchemaId) -> MetaResult<Vec<TableId>> {
        let core = self.core.lock().await;
        let tables = Table::list(core.env.meta_store()).await?;
        Ok(tables
            .iter()
            .filter(|t| t.schema_id == schema_id)
            .map(|t| t.id)
            .collect())
    }

    pub async fn list_sources(&self, schema_id: SchemaId) -> MetaResult<Vec<SourceId>> {
        let core = self.core.lock().await;
        let sources = Source::list(core.env.meta_store()).await?;
        Ok(sources
            .iter()
            .filter(|s| s.schema_id == schema_id)
            .map(|s| s.id)
            .collect())
    }

    async fn notify_frontend(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.env
            .notification_manager()
            .notify_frontend(operation, info)
            .await
    }

    async fn broadcast_info_op(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.env
            .notification_manager()
            .notify_all_node(operation, info)
            .await
    }
}

type DatabaseKey = String;
type SchemaKey = (DatabaseId, String);
type TableKey = (DatabaseId, SchemaId, String);
type SourceKey = (DatabaseId, SchemaId, String);
type SinkKey = (DatabaseId, SchemaId, String);
type IndexKey = (DatabaseId, SchemaId, String);
type RelationKey = (DatabaseId, SchemaId, String);

/// [`CatalogManagerCore`] caches meta catalog information and maintains dependent relationship
/// between tables.
pub struct CatalogManagerCore<S: MetaStore> {
    env: MetaSrvEnv<S>,
    /// Cached database key information.
    databases: HashSet<DatabaseKey>,
    /// Cached schema key information.
    schemas: HashSet<SchemaKey>,
    /// Cached source key information.
    sources: HashSet<SourceKey>,
    /// Cached sink key information.
    sinks: HashSet<SinkKey>,
    /// Cached table key information.
    tables: HashSet<TableKey>,
    /// Cached index key information.
    indexes: HashSet<IndexKey>,
    /// Relation refer count mapping.
    relation_ref_count: HashMap<RelationId, usize>,

    // In-progress creation tracker
    in_progress_creation_tracker: HashSet<RelationKey>,
}

impl<S> CatalogManagerCore<S>
where
    S: MetaStore,
{
    async fn new(env: MetaSrvEnv<S>) -> MetaResult<Self> {
        let databases = Database::list(env.meta_store()).await?;
        let schemas = Schema::list(env.meta_store()).await?;
        let sources = Source::list(env.meta_store()).await?;
        let sinks = Sink::list(env.meta_store()).await?;
        let tables = Table::list(env.meta_store()).await?;
        let indexes = Index::list(env.meta_store()).await?;

        let mut relation_ref_count = HashMap::new();

        let databases = HashSet::from_iter(databases.into_iter().map(|database| (database.name)));
        let schemas = HashSet::from_iter(
            schemas
                .into_iter()
                .map(|schema| (schema.database_id, schema.name)),
        );
        let sources = HashSet::from_iter(
            sources
                .into_iter()
                .map(|source| (source.database_id, source.schema_id, source.name)),
        );
        let sinks = HashSet::from_iter(
            sinks
                .into_iter()
                .map(|sink| (sink.database_id, sink.schema_id, sink.name)),
        );
        let indexes = HashSet::from_iter(
            indexes
                .into_iter()
                .map(|index| (index.database_id, index.schema_id, index.name)),
        );
        let tables = HashSet::from_iter(tables.into_iter().map(|table| {
            for depend_relation_id in &table.dependent_relations {
                relation_ref_count.entry(*depend_relation_id).or_insert(0);
            }
            (table.database_id, table.schema_id, table.name)
        }));

        let in_progress_creation_tracker = HashSet::new();

        Ok(Self {
            env,
            databases,
            schemas,
            sources,
            sinks,
            tables,
            indexes,
            relation_ref_count,
            in_progress_creation_tracker,
        })
    }

    pub async fn get_catalog(&self) -> MetaResult<Catalog> {
        Ok((
            Database::list(self.env.meta_store()).await?,
            Schema::list(self.env.meta_store()).await?,
            Table::list(self.env.meta_store()).await?,
            Source::list(self.env.meta_store()).await?,
            Sink::list(self.env.meta_store()).await?,
            Index::list(self.env.meta_store()).await?,
        ))
    }

    pub async fn list_sources(&self) -> MetaResult<Vec<Source>> {
        Source::list(self.env.meta_store())
            .await
            .map_err(Into::into)
    }

    fn has_database(&self, database: &Database) -> bool {
        self.databases.contains(database.get_name())
    }

    fn add_database(&mut self, database: &Database) {
        self.databases.insert(database.name.clone());
    }

    fn drop_database(&mut self, database: &Database) -> bool {
        self.databases.remove(database.get_name())
    }

    fn has_schema(&self, schema: &Schema) -> bool {
        self.schemas
            .contains(&(schema.database_id, schema.name.clone()))
    }

    fn add_schema(&mut self, schema: &Schema) {
        self.schemas
            .insert((schema.database_id, schema.name.clone()));
    }

    fn drop_schema(&mut self, schema: &Schema) -> bool {
        self.schemas
            .remove(&(schema.database_id, schema.name.clone()))
    }

    fn has_table(&self, table: &Table) -> bool {
        self.tables
            .contains(&(table.database_id, table.schema_id, table.name.clone()))
    }

    fn add_table(&mut self, table: &Table) {
        self.tables
            .insert((table.database_id, table.schema_id, table.name.clone()));
    }

    fn drop_table(&mut self, table: &Table) -> bool {
        self.tables
            .remove(&(table.database_id, table.schema_id, table.name.clone()))
    }

    fn has_source(&self, source: &Source) -> bool {
        self.sources
            .contains(&(source.database_id, source.schema_id, source.name.clone()))
    }

    fn add_source(&mut self, source: &Source) {
        self.sources
            .insert((source.database_id, source.schema_id, source.name.clone()));
    }

    fn drop_source(&mut self, source: &Source) -> bool {
        self.sources
            .remove(&(source.database_id, source.schema_id, source.name.clone()))
    }

    pub async fn get_source(&self, id: SourceId) -> MetaResult<Option<Source>> {
        Source::select(self.env.meta_store(), &id)
            .await
            .map_err(Into::into)
    }

    fn has_sink(&self, sink: &Sink) -> bool {
        self.sinks
            .contains(&(sink.database_id, sink.schema_id, sink.name.clone()))
    }

    fn add_sink(&mut self, sink: &Sink) {
        self.sinks
            .insert((sink.database_id, sink.schema_id, sink.name.clone()));
    }

    fn drop_sink(&mut self, sink: &Sink) -> bool {
        self.sinks
            .remove(&(sink.database_id, sink.schema_id, sink.name.clone()))
    }

    pub async fn get_sink(&self, id: SinkId) -> MetaResult<Option<Sink>> {
        Sink::select(self.env.meta_store(), &id)
            .await
            .map_err(Into::into)
    }

    fn has_index(&self, index: &Index) -> bool {
        self.indexes
            .contains(&(index.database_id, index.schema_id, index.name.clone()))
    }

    fn add_index(&mut self, index: &Index) {
        self.indexes
            .insert((index.database_id, index.schema_id, index.name.clone()));
    }

    fn drop_index(&mut self, index: &Index) -> bool {
        self.indexes
            .remove(&(index.database_id, index.schema_id, index.name.clone()))
    }

    fn get_ref_count(&self, relation_id: RelationId) -> Option<usize> {
        self.relation_ref_count.get(&relation_id).cloned()
    }

    fn increase_ref_count(&mut self, relation_id: RelationId) {
        *self.relation_ref_count.entry(relation_id).or_insert(0) += 1;
    }

    fn decrease_ref_count(&mut self, relation_id: RelationId) {
        match self.relation_ref_count.entry(relation_id) {
            Entry::Occupied(mut o) => {
                *o.get_mut() -= 1;
                if *o.get() == 0 {
                    o.remove_entry();
                }
            }
            Entry::Vacant(_) => unreachable!(),
        }
    }

    fn has_in_progress_creation(&self, relation: &RelationKey) -> bool {
        self.in_progress_creation_tracker
            .contains(&relation.clone())
    }

    fn mark_creating(&mut self, relation: &RelationKey) {
        self.in_progress_creation_tracker.insert(relation.clone());
    }

    fn unmark_creating(&mut self, relation: &RelationKey) {
        self.in_progress_creation_tracker.remove(&relation.clone());
    }
}
