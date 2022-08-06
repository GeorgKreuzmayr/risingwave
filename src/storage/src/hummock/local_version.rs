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

use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeBounds;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use parking_lot::lock_api::ArcRwLockReadGuard;
use parking_lot::{RawRwLock, RwLock};
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch, HummockVersionId};
use risingwave_pb::hummock::{HummockVersion, Level};
use tokio::sync::mpsc::UnboundedSender;

use super::shared_buffer::SharedBuffer;
use crate::hummock::shared_buffer::{KeyIndexedUncommittedData, UncommittedData};

#[derive(Debug, Clone)]
pub struct LocalVersion {
    shared_buffer: BTreeMap<HummockEpoch, Arc<RwLock<SharedBuffer>>>,
    pinned_version: Arc<PinnedVersion>,
    pub version_ids_in_use: BTreeSet<HummockVersionId>,
    pub sync_uncommitted_sst_vec: Vec<(HummockEpoch, SyncUncommittedSstState)>,
}
#[derive(Debug, Clone)]
pub enum SyncUncommittedSstState {
    SyncTask(KeyIndexedUncommittedData),
    SyncSst(Vec<UncommittedData>),
    NoneData,
}

impl LocalVersion {
    pub fn new(
        version: HummockVersion,
        unpin_worker_tx: UnboundedSender<HummockVersionId>,
    ) -> Self {
        let mut version_ids_in_use = BTreeSet::new();
        version_ids_in_use.insert(version.id);
        Self {
            shared_buffer: BTreeMap::default(),
            pinned_version: Arc::new(PinnedVersion::new(version, unpin_worker_tx)),
            version_ids_in_use,
            sync_uncommitted_sst_vec: Default::default(),
        }
    }

    pub fn pinned_version(&self) -> &Arc<PinnedVersion> {
        &self.pinned_version
    }

    pub fn get_shared_buffer(&self, epoch: HummockEpoch) -> Option<&Arc<RwLock<SharedBuffer>>> {
        self.shared_buffer.get(&epoch)
    }

    pub fn add_sync_state(
        &mut self,
        sync_epoch: HummockEpoch,
        uncommitted_sst_state: SyncUncommittedSstState,
    ) {
        let node = self
            .sync_uncommitted_sst_vec
            .iter_mut()
            .find(|(epoch, _)| epoch == &sync_epoch);
        match &node {
            None => {
                assert!(matches!(
                    uncommitted_sst_state,
                    SyncUncommittedSstState::NoneData
                ));
                if let Some(last) = self.sync_uncommitted_sst_vec.last() {
                    assert!(
                        last.0 < sync_epoch
                    )
                }
                self.sync_uncommitted_sst_vec
                    .push((sync_epoch, uncommitted_sst_state));
                return;
            }
            Some((_, SyncUncommittedSstState::NoneData)) => {
                assert!(matches!(
                    uncommitted_sst_state,
                    SyncUncommittedSstState::SyncTask(_)
                ));
            }
            Some((_, SyncUncommittedSstState::SyncTask(_))) => {
                assert!(matches!(
                    uncommitted_sst_state,
                    SyncUncommittedSstState::SyncSst(_)
                ));
            }
            Some((_, SyncUncommittedSstState::SyncSst(_))) => {
                panic!("sync over, can't modify uncommitted sst state");
            }
        }
        *node.unwrap() = (sync_epoch, uncommitted_sst_state);
    }

    pub fn iter_shared_buffer(
        &self,
    ) -> impl Iterator<Item = (&HummockEpoch, &Arc<RwLock<SharedBuffer>>)> {
        self.shared_buffer.iter()
    }

    pub fn new_shared_buffer(
        &mut self,
        epoch: HummockEpoch,
        global_upload_task_size: Arc<AtomicUsize>,
    ) -> Arc<RwLock<SharedBuffer>> {
        self.shared_buffer
            .entry(epoch)
            .or_insert_with(|| Arc::new(RwLock::new(SharedBuffer::new(global_upload_task_size))))
            .clone()
    }

    pub fn set_pinned_version(&mut self, new_pinned_version: HummockVersion) {
        // Clean shared buffer and uncommitted ssts below (<=) new max committed epoch
        if self.pinned_version.max_committed_epoch() < new_pinned_version.max_committed_epoch {
            self.shared_buffer
                .retain(|epoch, _| epoch > &new_pinned_version.max_committed_epoch);
            self.sync_uncommitted_sst_vec
                .retain(|(epoch, _)| epoch > &new_pinned_version.max_committed_epoch);
        }

        self.version_ids_in_use.insert(new_pinned_version.id);

        // update pinned version
        self.pinned_version = Arc::new(PinnedVersion {
            version: new_pinned_version,
            unpin_worker_tx: self.pinned_version.unpin_worker_tx.clone(),
        });
    }

    pub fn read_version(this: &RwLock<Self>, read_epoch: HummockEpoch) -> ReadVersion {
        use parking_lot::RwLockReadGuard;
        let (pinned_version, (shared_buffer, sync_uncommitted_state)) = {
            let guard = this.read();
            let smallest_uncommitted_epoch = guard.pinned_version.max_committed_epoch() + 1;
            let pinned_version = guard.pinned_version.clone();
            (
                pinned_version,
                if read_epoch >= smallest_uncommitted_epoch {
                    let result = guard
                        .shared_buffer
                        .range(smallest_uncommitted_epoch..=read_epoch)
                        .rev() // Important: order by epoch descendingly
                        .map(|(_, shared_buffer)| shared_buffer.clone())
                        .collect();
                    let result_sync: Vec<SyncUncommittedSstState> = guard
                        .sync_uncommitted_sst_vec
                        .iter()
                        .filter(|&node| node.0 <= read_epoch && node.0 > smallest_uncommitted_epoch)
                        .map(|(_, value)| value.clone())
                        .collect();
                    RwLockReadGuard::unlock_fair(guard);
                    (result, result_sync)
                } else {
                    RwLockReadGuard::unlock_fair(guard);
                    (Vec::new(), Vec::new())
                },
            )
        };

        ReadVersion {
            shared_buffer: shared_buffer.into_iter().map(|x| x.read_arc()).collect(),
            pinned_version,
            sync_uncommitted_state,
        }
    }

    pub fn clear_shared_buffer(&mut self) {
        self.shared_buffer.clear();
    }
}

#[derive(Debug)]
pub struct PinnedVersion {
    version: HummockVersion,
    unpin_worker_tx: UnboundedSender<HummockVersionId>,
}

impl Drop for PinnedVersion {
    fn drop(&mut self) {
        self.unpin_worker_tx.send(self.version.id).ok();
    }
}

impl PinnedVersion {
    fn new(
        version: HummockVersion,
        unpin_worker_tx: UnboundedSender<HummockVersionId>,
    ) -> PinnedVersion {
        PinnedVersion {
            version,
            unpin_worker_tx,
        }
    }

    pub fn id(&self) -> HummockVersionId {
        self.version.id
    }

    pub fn levels(&self, compaction_group_id: Option<CompactionGroupId>) -> Vec<&Level> {
        match compaction_group_id {
            None => self.version.get_combined_levels(),
            Some(compaction_group_id) => {
                let levels = self
                    .version
                    .get_compaction_group_levels(compaction_group_id);
                let mut ret = vec![];
                ret.extend(levels.l0.as_ref().unwrap().sub_levels.iter().rev());
                ret.extend(levels.levels.iter());
                ret
            }
        }
    }

    pub fn max_committed_epoch(&self) -> u64 {
        self.version.max_committed_epoch
    }

    pub fn safe_epoch(&self) -> u64 {
        self.version.safe_epoch
    }

    /// ret value can't be used as `HummockVersion`. it must be modified with delta
    pub fn version(&self) -> HummockVersion {
        self.version.clone()
    }
}

pub struct ReadVersion {
    /// The shared buffer is sorted by epoch descendingly
    pub shared_buffer: Vec<ArcRwLockReadGuard<RawRwLock, SharedBuffer>>,
    pub pinned_version: Arc<PinnedVersion>,
    pub sync_uncommitted_state: Vec<SyncUncommittedSstState>,
}
