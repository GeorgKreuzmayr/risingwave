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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use fail::fail_point;
use parking_lot::RwLock;
use risingwave_hummock_sdk::HummockContextId;
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::{
    CancelCompactTask, CompactTask, CompactTaskAssignment, CompactTaskProgress,
    SubscribeCompactTasksResponse,
};
use tokio::sync::mpsc::{Receiver, Sender};

use super::compaction_schedule_policy::{CompactionSchedulePolicy, RoundRobinPolicy, ScoredPolicy};
use crate::hummock::error::Result;
use crate::manager::MetaSrvEnv;
use crate::model::MetadataModel;
use crate::storage::MetaStore;
use crate::MetaResult;

pub type CompactorManagerRef = Arc<CompactorManager>;
type TaskId = u64;

/// Wraps the stream between meta node and compactor node.
/// Compactor node will re-establish the stream when the previous one fails.
pub struct Compactor {
    context_id: HummockContextId,
    sender: Sender<MetaResult<SubscribeCompactTasksResponse>>,
    max_concurrent_task_number: u64,
}

struct TaskHeartbeat {
    task: CompactTask,
    num_ssts_sealed: u32,
    num_ssts_uploaded: u32,
    expire_at: u64,
}

impl Compactor {
    pub fn new(
        context_id: HummockContextId,
        sender: Sender<MetaResult<SubscribeCompactTasksResponse>>,
        max_concurrent_task_number: u64,
    ) -> Self {
        Self {
            context_id,
            sender,
            max_concurrent_task_number,
        }
    }

    pub async fn send_task(&self, task: Task) -> MetaResult<()> {
        fail_point!("compaction_send_task_fail", |_| Err(anyhow::anyhow!(
            "compaction_send_task_fail"
        )
        .into()));
        self.sender
            .send(Ok(SubscribeCompactTasksResponse { task: Some(task) }))
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }

    pub async fn cancel_task(&self, task_id: u64) -> MetaResult<()> {
        self.sender
            .send(Ok(SubscribeCompactTasksResponse {
                task: Some(Task::CancelCompactTask(CancelCompactTask {
                    context_id: self.context_id,
                    task_id,
                })),
            }))
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }

    pub fn context_id(&self) -> HummockContextId {
        self.context_id
    }

    pub fn max_concurrent_task_number(&self) -> u64 {
        self.max_concurrent_task_number
    }
}

/// `CompactorManager` maintains compactors which can process compact task.
/// A compact task is tracked in `HummockManager::Compaction` via both `CompactStatus` and
/// `CompactTaskAssignment`.
///
/// A compact task can be in one of these states:
/// - 1. Pending: a compact task is picked but not assigned to a compactor.
///   Pending-->Success/Failed/Canceled.
/// - 2. Success: an assigned task is reported as success via `CompactStatus::report_compact_task`.
///   It's the final state.
/// - 3. Failed: an Failed task is reported as success via `CompactStatus::report_compact_task`.
///   It's the final state.
/// - 4. Cancelled: a task is reported as cancelled via `CompactStatus::report_compact_task`. It's
///   the final state.
/// We omit Assigned state because there's nothing to be done about this state currently.
///
/// Furthermore, the compactor for a compaction task must be picked with `CompactorManager`,
/// or its internal states might not be correctly maintained.
pub struct CompactorManager {
    // `policy` must be locked before `compactor_assigned_task_num`.
    policy: RwLock<Box<dyn CompactionSchedulePolicy>>,

    pub task_expiry_seconds: u64,
    // A map: { context_id -> { task_id -> heartbeat } }
    task_heartbeats: RwLock<HashMap<HummockContextId, HashMap<TaskId, TaskHeartbeat>>>,
}

impl CompactorManager {
    pub async fn new_with_meta<S: MetaStore>(
        env: MetaSrvEnv<S>,
        task_expiry_seconds: u64,
    ) -> MetaResult<Self> {
        // Retrieve the existing task assignments from metastore.
        let task_assignment = CompactTaskAssignment::list(env.meta_store()).await?;
        // Initialize the number of tasks assigned to each comapctor.
        let mut compactor_assigned_task_num = HashMap::new();
        task_assignment.iter().for_each(|assignment| {
            compactor_assigned_task_num
                .entry(assignment.context_id)
                .and_modify(|n| *n += 1)
                .or_insert(1);
        });
        let manager = Self {
            policy: RwLock::new(Box::new(ScoredPolicy::new_with_task_assignment(
                &task_assignment,
            ))),
            task_expiry_seconds,
            task_heartbeats: Default::default(),
        };
        // Initialize heartbeat for existing tasks.
        task_assignment.into_iter().for_each(|assignment| {
            manager
                .initiate_task_heartbeat(assignment.context_id, assignment.compact_task.unwrap());
        });
        Ok(manager)
    }

    /// Only used for unit test.
    pub fn new_for_test() -> Self {
        Self {
            policy: RwLock::new(Box::new(RoundRobinPolicy::new())),
            task_expiry_seconds: 1,
            task_heartbeats: Default::default(),
        }
    }

    /// Only used for unit test.
    pub fn new_with_policy(policy: Box<dyn CompactionSchedulePolicy>) -> Self {
        Self {
            policy: RwLock::new(policy),
            task_expiry_seconds: 1,
            task_heartbeats: Default::default(),
        }
    }

    /// Gets next idle compactor to assign task.
    pub fn next_idle_compactor(
        &self,
        compactor_assigned_task_num: &HashMap<HummockContextId, u64>,
    ) -> Option<Arc<Compactor>> {
        let policy = self.policy.read();
        policy.next_idle_compactor(compactor_assigned_task_num)
    }

    /// Gets next compactor to assign task.
    pub fn next_compactor(&self) -> Option<Arc<Compactor>> {
        let policy = self.policy.read();
        policy.next_compactor()
    }

    /// Retrieve a receiver of tasks for the compactor identified by `context_id`. The sender should
    /// be obtained by calling one of the compactor getters.
    ///
    ///  If `add_compactor` is called with the same `context_id` more than once, the only cause
    /// would be compactor re-subscription, as `context_id` is a monotonically increasing
    /// sequence.
    pub fn add_compactor(
        &self,
        context_id: HummockContextId,
        max_concurrent_task_number: u64,
    ) -> Receiver<MetaResult<SubscribeCompactTasksResponse>> {
        let mut policy = self.policy.write();
        let rx = policy.add_compactor(context_id, max_concurrent_task_number);
        tracing::info!("Added compactor session {}", context_id);
        rx
    }

    pub fn remove_compactor(&self, context_id: HummockContextId) {
        let mut policy = self.policy.write();
        policy.remove_compactor(context_id);

        // To remove the heartbeats, they need to be forcefully purged,
        // which is only safe when the context has been completely removed from meta.
        tracing::info!("Removed compactor session {}", context_id);
    }

    pub fn get_compactor(&self, context_id: HummockContextId) -> Option<Arc<Compactor>> {
        self.policy.read().get_compactor(context_id)
    }

    pub fn assign_compact_task(
        &self,
        context_id: HummockContextId,
        compact_task: &CompactTask,
    ) -> Result<()> {
        self.policy
            .write()
            .assign_compact_task(context_id, compact_task)
    }

    // Report the completion of a compaction task to adjust the compaction schedule policy.
    pub fn report_compact_task(&self, context_id: HummockContextId, compact_task: &CompactTask) {
        self.policy
            .write()
            .report_compact_task(context_id, compact_task)
    }

    /// Forcefully purging the heartbeats for a task is only safe when the
    /// context has been completely removed from meta.
    /// Returns true if there were remaining heartbeats for the task.
    pub fn purge_heartbeats_for_context(&self, context_id: HummockContextId) -> bool {
        self.task_heartbeats.write().remove(&context_id).is_some()
    }

    pub fn get_expired_tasks(&self) -> Vec<(HummockContextId, CompactTask)> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        let mut cancellable_tasks = vec![];
        {
            let guard = self.task_heartbeats.read();
            for (context_id, heartbeats) in guard.iter() {
                {
                    for TaskHeartbeat {
                        expire_at, task, ..
                    } in heartbeats.values()
                    {
                        if *expire_at < now {
                            cancellable_tasks.push((*context_id, task.clone()));
                        }
                    }
                }
            }
        }
        cancellable_tasks
    }

    pub fn initiate_task_heartbeat(&self, context_id: HummockContextId, task: CompactTask) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        let mut guard = self.task_heartbeats.write();
        let entry = guard.entry(context_id).or_insert_with(HashMap::new);
        entry.insert(
            task.task_id,
            TaskHeartbeat {
                task,
                num_ssts_sealed: 0,
                num_ssts_uploaded: 0,
                expire_at: now + self.task_expiry_seconds,
            },
        );
    }

    pub fn remove_task_heartbeat(&self, context_id: HummockContextId, task_id: u64) {
        let mut guard = self.task_heartbeats.write();
        let mut garbage_collect = false;
        if let Some(heartbeats) = guard.get_mut(&context_id) {
            heartbeats.remove(&task_id);
            if heartbeats.is_empty() {
                garbage_collect = true;
            }
        }
        if garbage_collect {
            guard.remove(&context_id);
        }
    }

    pub fn update_task_heartbeats(
        &self,
        context_id: HummockContextId,
        progress_list: &Vec<CompactTaskProgress>,
    ) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        let mut guard = self.task_heartbeats.write();
        if let Some(heartbeats) = guard.get_mut(&context_id) {
            for progress in progress_list {
                if let Some(task_ref) = heartbeats.get_mut(&progress.task_id) {
                    if task_ref.num_ssts_sealed < progress.num_ssts_sealed
                        || task_ref.num_ssts_uploaded < progress.num_ssts_uploaded
                    {
                        // Refresh the expiry of the task as it is showing progress.
                        task_ref.expire_at = now + self.task_expiry_seconds;
                        // Update the task state to the latest state.
                        task_ref.num_ssts_sealed = progress.num_ssts_sealed;
                        task_ref.num_ssts_uploaded = progress.num_ssts_uploaded;
                    }
                }
            }
        }
    }

    pub fn compactor_num(&self) -> usize {
        self.policy.read().compactor_num()
    }
}
