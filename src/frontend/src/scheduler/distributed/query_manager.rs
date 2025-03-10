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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_batch::executor::BoxedDataChunkStream;
use risingwave_common::array::DataChunk;
use risingwave_common::error::RwError;
use risingwave_pb::batch_plan::TaskOutputId;
use risingwave_pb::common::HostAddress;
use risingwave_rpc_client::ComputeClientPoolRef;
use tracing::debug;

use super::QueryExecution;
use crate::catalog::catalog_service::CatalogReader;
use crate::handler::query::QueryResultSet;
use crate::handler::util::to_pg_rows;
use crate::scheduler::plan_fragmenter::Query;
use crate::scheduler::worker_node_manager::WorkerNodeManagerRef;
use crate::scheduler::{ExecutionContextRef, HummockSnapshotManagerRef, SchedulerResult};

pub struct QueryResultFetcher {
    // TODO: Remove these after implemented worker node level snapshot pinnning
    epoch: u64,
    hummock_snapshot_manager: HummockSnapshotManagerRef,

    task_output_id: TaskOutputId,
    task_host: HostAddress,
    compute_client_pool: ComputeClientPoolRef,

    chunk_rx: tokio::sync::mpsc::Receiver<SchedulerResult<DataChunk>>,
}

/// Manages execution of distributed batch queries.
#[derive(Clone)]
pub struct QueryManager {
    worker_node_manager: WorkerNodeManagerRef,
    hummock_snapshot_manager: HummockSnapshotManagerRef,
    compute_client_pool: ComputeClientPoolRef,
    catalog_reader: CatalogReader,
}

type QueryManagerRef = Arc<QueryManager>;

impl QueryManager {
    pub fn new(
        worker_node_manager: WorkerNodeManagerRef,
        hummock_snapshot_manager: HummockSnapshotManagerRef,
        compute_client_pool: ComputeClientPoolRef,
        catalog_reader: CatalogReader,
    ) -> Self {
        Self {
            worker_node_manager,
            hummock_snapshot_manager,
            compute_client_pool,
            catalog_reader,
        }
    }

    pub async fn schedule(
        &self,
        context: ExecutionContextRef,
        query: Query,
        format: bool,
    ) -> SchedulerResult<QueryResultSet> {
        let query_id = query.query_id().clone();
        let epoch = self
            .hummock_snapshot_manager
            .get_epoch(query_id.clone())
            .await?
            .committed_epoch;

        let query_execution = QueryExecution::new(
            context.clone(),
            query,
            epoch,
            self.worker_node_manager.clone(),
            self.hummock_snapshot_manager.clone(),
            self.compute_client_pool.clone(),
            self.catalog_reader.clone(),
        )
        .await;

        // Create a oneshot channel for QueryResultFetcher to get failed event.
        let query_result_fetcher = match query_execution.start().await {
            Ok(query_result_fetcher) => query_result_fetcher,
            Err(e) => {
                self.hummock_snapshot_manager
                    .unpin_snapshot(epoch, &query_id)
                    .await;
                return Err(e);
            }
        };

        query_result_fetcher.collect_rows_from_channel(format).await
    }
}

impl QueryResultFetcher {
    pub fn new(
        epoch: u64,
        hummock_snapshot_manager: HummockSnapshotManagerRef,
        task_output_id: TaskOutputId,
        task_host: HostAddress,
        compute_client_pool: ComputeClientPoolRef,
        chunk_rx: tokio::sync::mpsc::Receiver<SchedulerResult<DataChunk>>,
    ) -> Self {
        Self {
            epoch,
            hummock_snapshot_manager,
            task_output_id,
            task_host,
            compute_client_pool,
            chunk_rx,
        }
    }

    #[try_stream(ok = DataChunk, error = RwError)]
    async fn run_inner(self) {
        debug!(
            "Starting to run query result fetcher, task output id: {:?}, task_host: {:?}",
            self.task_output_id, self.task_host
        );
        let compute_client = self
            .compute_client_pool
            .get_by_addr((&self.task_host).into())
            .await?;
        let mut stream = compute_client.get_data(self.task_output_id.clone()).await?;
        while let Some(response) = stream.next().await {
            yield DataChunk::from_protobuf(response?.get_record_batch()?)?;
        }
    }

    fn run(self) -> BoxedDataChunkStream {
        Box::pin(self.run_inner())
    }

    async fn collect_rows_from_channel(mut self, format: bool) -> SchedulerResult<QueryResultSet> {
        let mut result_sets = vec![];
        while let Some(chunk_inner) = self.chunk_rx.recv().await {
            let chunk = chunk_inner?;
            result_sets.extend(to_pg_rows(chunk, format));
        }
        Ok(result_sets)
    }
}

impl Debug for QueryResultFetcher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResultFetcher")
            .field("epoch", &self.epoch)
            .field("task_output_id", &self.task_output_id)
            .field("task_host", &self.task_host)
            .finish()
    }
}
