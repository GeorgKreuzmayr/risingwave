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

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

use async_stack_trace::StackTrace;
use enum_as_inner::EnumAsInner;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use minitrace::prelude::*;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayImpl, DataChunk, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_common::types::DataType;
use risingwave_connector::source::{ConnectorState, SplitImpl};
use risingwave_pb::data::Epoch as ProstEpoch;
use risingwave_pb::stream_plan::add_mutation::Dispatchers;
use risingwave_pb::stream_plan::barrier::Mutation as ProstMutation;
use risingwave_pb::stream_plan::stream_message::StreamMessage;
use risingwave_pb::stream_plan::update_mutation::{DispatcherUpdate, MergeUpdate};
use risingwave_pb::stream_plan::{
    AddMutation, Barrier as ProstBarrier, Dispatcher as ProstDispatcher, PauseMutation,
    ResumeMutation, SourceChangeSplitMutation, StopMutation, StreamMessage as ProstStreamMessage,
    UpdateMutation,
};
use smallvec::SmallVec;

use crate::task::ActorId;

mod actor;
mod barrier_align;
pub mod exchange;
pub mod monitor;

pub mod aggregation;
mod batch_query;
mod chain;
mod dispatch;
mod dynamic_filter;
mod error;
mod expand;
mod filter;
mod global_simple_agg;
mod group_top_n;
mod hash_agg;
pub mod hash_join;
mod hop_window;
mod local_simple_agg;
mod lookup;
mod lookup_union;
mod managed_state;
mod merge;
mod mview;
mod project;
mod project_set;
mod rearranged_chain;
mod receiver;
mod simple;
mod sink;
mod source;
mod top_n;
mod top_n_appendonly;
mod top_n_executor;
mod union;
mod wrapper;

#[cfg(test)]
mod integration_tests;
#[cfg(test)]
mod test_utils;

pub use actor::{Actor, ActorContext, ActorContextRef};
pub use batch_query::BatchQueryExecutor;
pub use chain::ChainExecutor;
pub use dispatch::{DispatchExecutor, DispatcherImpl};
pub use dynamic_filter::DynamicFilterExecutor;
pub use error::StreamExecutorResult;
pub use expand::ExpandExecutor;
pub use filter::FilterExecutor;
pub use global_simple_agg::GlobalSimpleAggExecutor;
pub use group_top_n::GroupTopNExecutor;
pub use hash_agg::HashAggExecutor;
pub use hash_join::*;
pub use hop_window::HopWindowExecutor;
pub use local_simple_agg::LocalSimpleAggExecutor;
pub use lookup::*;
pub use lookup_union::LookupUnionExecutor;
pub use merge::MergeExecutor;
pub use mview::*;
pub use project::ProjectExecutor;
pub use project_set::*;
pub use rearranged_chain::RearrangedChainExecutor;
pub use receiver::ReceiverExecutor;
use risingwave_pb::source::{ConnectorSplit, ConnectorSplits};
use simple::{SimpleExecutor, SimpleExecutorWrapper};
pub use sink::SinkExecutor;
pub use source::*;
pub use top_n::TopNExecutor;
pub use top_n_appendonly::AppendOnlyTopNExecutor;
pub use union::UnionExecutor;
pub use wrapper::WrapperExecutor;

pub type BoxedExecutor = Box<dyn Executor>;
pub type BoxedMessageStream = BoxStream<'static, StreamExecutorResult<Message>>;
pub type MessageStreamItem = StreamExecutorResult<Message>;

pub trait MessageStream = futures::Stream<Item = MessageStreamItem> + Send;

/// The maximum chunk length produced by executor at a time.
const PROCESSING_WINDOW_SIZE: usize = 1024;

/// Static information of an executor.
#[derive(Debug, Default)]
pub struct ExecutorInfo {
    /// See [`Executor::schema`].
    pub schema: Schema,

    /// See [`Executor::pk_indices`].
    pub pk_indices: PkIndices,

    /// See [`Executor::identity`].
    pub identity: String,
}

/// `Executor` supports handling of control messages.
pub trait Executor: Send + 'static {
    fn execute(self: Box<Self>) -> BoxedMessageStream;

    /// Return the schema of the OUTPUT of the executor.
    fn schema(&self) -> &Schema;

    /// Return the primary key indices of the OUTPUT of the executor.
    /// Schema is used by both OLAP and streaming, therefore
    /// pk indices are maintained independently.
    fn pk_indices(&self) -> PkIndicesRef;

    /// Identity of the executor.
    fn identity(&self) -> &str;

    fn execute_with_epoch(self: Box<Self>, _epoch: u64) -> BoxedMessageStream {
        self.execute()
    }

    #[inline(always)]
    fn info(&self) -> ExecutorInfo {
        let schema = self.schema().to_owned();
        let pk_indices = self.pk_indices().to_owned();
        let identity = self.identity().to_owned();
        ExecutorInfo {
            schema,
            pk_indices,
            identity,
        }
    }

    fn boxed(self) -> BoxedExecutor
    where
        Self: Sized + Send + 'static,
    {
        Box::new(self)
    }
}

impl std::fmt::Debug for BoxedExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.identity())
    }
}

pub const INVALID_EPOCH: u64 = 0;

pub trait ExprFn = Fn(&DataChunk) -> Result<Bitmap> + Send + Sync + 'static;

/// See [`risingwave_pb::stream_plan::barrier::Mutation`] for the semantics of each mutation.
#[derive(Debug, Clone, PartialEq, EnumAsInner)]
pub enum Mutation {
    Stop(HashSet<ActorId>),
    Update {
        dispatchers: HashMap<ActorId, DispatcherUpdate>,
        merges: HashMap<ActorId, MergeUpdate>,
        vnode_bitmaps: HashMap<ActorId, Arc<Bitmap>>,
        dropped_actors: HashSet<ActorId>,
    },
    Add {
        adds: HashMap<ActorId, Vec<ProstDispatcher>>,
        // TODO: remove this and use `SourceChangesSplit` after we support multiple mutations.
        splits: HashMap<ActorId, Vec<SplitImpl>>,
    },
    SourceChangeSplit(HashMap<ActorId, ConnectorState>),
    Pause,
    Resume,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Epoch {
    pub curr: u64,
    pub prev: u64,
}

impl Epoch {
    pub fn new(curr: u64, prev: u64) -> Self {
        assert!(curr > prev);
        Self { curr, prev }
    }

    #[cfg(test)]
    pub fn inc(&self) -> Self {
        Self {
            curr: self.curr + 1,
            prev: self.prev + 1,
        }
    }

    pub fn new_test_epoch(curr: u64) -> Self {
        assert!(curr > 0);
        Self::new(curr, curr - 1)
    }
}

impl Default for Epoch {
    fn default() -> Self {
        Self {
            curr: 1,
            prev: INVALID_EPOCH,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Barrier {
    pub epoch: Epoch,
    pub mutation: Option<Arc<Mutation>>,
    pub checkpoint: bool,

    /// The actors that this barrier has passed locally. Used for debugging only.
    pub passed_actors: Vec<ActorId>,
}

impl Barrier {
    /// Create a plain barrier.
    pub fn new_test_barrier(epoch: u64) -> Self {
        Self {
            epoch: Epoch::new_test_epoch(epoch),
            checkpoint: true,
            ..Default::default()
        }
    }

    #[must_use]
    pub fn with_mutation(self, mutation: Mutation) -> Self {
        Self {
            mutation: Some(Arc::new(mutation)),
            ..self
        }
    }

    #[must_use]
    pub fn with_stop(self) -> Self {
        self.with_mutation(Mutation::Stop(HashSet::default()))
    }

    /// Whether this barrier carries stop mutation.
    pub fn is_with_stop_mutation(&self) -> bool {
        matches!(self.mutation.as_deref(), Some(Mutation::Stop(_)))
    }

    /// Whether this barrier is to stop the actor with `actor_id`.
    pub fn is_stop_or_update_drop_actor(&self, actor_id: ActorId) -> bool {
        self.all_stop_actors()
            .map_or(false, |actors| actors.contains(&actor_id))
    }

    /// Get all actors that to be stopped (dropped) by this barrier.
    pub fn all_stop_actors(&self) -> Option<&HashSet<ActorId>> {
        match self.mutation.as_deref() {
            Some(Mutation::Stop(actors)) => Some(actors),
            Some(Mutation::Update { dropped_actors, .. }) => Some(dropped_actors),
            _ => None,
        }
    }

    /// Whether this barrier is to add new dispatchers for the actor with `actor_id`.
    pub fn is_add_dispatcher(&self, actor_id: ActorId) -> bool {
        matches!(
            self.mutation.as_deref(),
            Some(Mutation::Add {adds, ..}) if adds
                .values()
                .flatten()
                .any(|dispatcher| dispatcher.downstream_actor_id.contains(&actor_id))
        )
    }

    /// Whether this barrier is for configuration change. Used for source executor initialization.
    pub fn is_update(&self) -> bool {
        matches!(self.mutation.as_deref(), Some(Mutation::Update { .. }))
    }

    /// Returns the [`MergeUpdate`] if this barrier is to update the merge executors for the actor
    /// with `actor_id`.
    pub fn as_update_merge(&self, actor_id: ActorId) -> Option<&MergeUpdate> {
        self.mutation
            .as_deref()
            .and_then(|mutation| match mutation {
                Mutation::Update { merges, .. } => merges.get(&actor_id),
                _ => None,
            })
    }

    /// Returns the new vnode bitmap if this barrier is to update the vnode bitmap for the actor
    /// with `actor_id`.
    ///
    /// Actually, this vnode bitmap update is only useful for the record accessing validation for
    /// distributed executors, since the read/write pattern will never be across multiple vnodes.
    pub fn as_update_vnode_bitmap(&self, actor_id: ActorId) -> Option<Arc<Bitmap>> {
        self.mutation
            .as_deref()
            .and_then(|mutation| match mutation {
                Mutation::Update { vnode_bitmaps, .. } => vnode_bitmaps.get(&actor_id).cloned(),
                _ => None,
            })
    }
}

impl PartialEq for Barrier {
    fn eq(&self, other: &Self) -> bool {
        self.epoch == other.epoch && self.mutation == other.mutation
    }
}

impl Mutation {
    /// Return true if the mutation is stop.
    ///
    /// Note that this does not mean we will stop the current actor.
    #[cfg(test)]
    pub fn is_stop(&self) -> bool {
        matches!(self, Mutation::Stop(_))
    }

    fn to_protobuf(&self) -> ProstMutation {
        match self {
            Mutation::Stop(actors) => ProstMutation::Stop(StopMutation {
                actors: actors.iter().copied().collect::<Vec<_>>(),
            }),
            Mutation::Update {
                dispatchers,
                merges,
                vnode_bitmaps,
                dropped_actors,
            } => ProstMutation::Update(UpdateMutation {
                actor_dispatcher_update: dispatchers.clone(),
                actor_merge_update: merges.clone(),
                actor_vnode_bitmap_update: vnode_bitmaps
                    .iter()
                    .map(|(&actor_id, bitmap)| (actor_id, bitmap.to_protobuf()))
                    .collect(),
                dropped_actors: dropped_actors.iter().cloned().collect(),
            }),
            Mutation::Add { adds, .. } => ProstMutation::Add(AddMutation {
                actor_dispatchers: adds
                    .iter()
                    .map(|(&actor_id, dispatchers)| {
                        (
                            actor_id,
                            Dispatchers {
                                dispatchers: dispatchers.clone(),
                            },
                        )
                    })
                    .collect(),
                ..Default::default()
            }),
            Mutation::SourceChangeSplit(changes) => {
                ProstMutation::Splits(SourceChangeSplitMutation {
                    actor_splits: changes
                        .iter()
                        .map(|(&actor_id, splits)| {
                            (
                                actor_id,
                                ConnectorSplits {
                                    splits: splits
                                        .clone()
                                        .unwrap_or_default()
                                        .iter()
                                        .map(ConnectorSplit::from)
                                        .collect(),
                                },
                            )
                        })
                        .collect(),
                })
            }
            Mutation::Pause => ProstMutation::Pause(PauseMutation {}),
            Mutation::Resume => ProstMutation::Resume(ResumeMutation {}),
        }
    }

    fn from_protobuf(prost: &ProstMutation) -> Result<Self> {
        let mutation = match prost {
            ProstMutation::Stop(stop) => {
                Mutation::Stop(HashSet::from_iter(stop.get_actors().clone()))
            }

            ProstMutation::Update(update) => Mutation::Update {
                dispatchers: update.actor_dispatcher_update.clone(),
                merges: update.actor_merge_update.clone(),
                vnode_bitmaps: update
                    .actor_vnode_bitmap_update
                    .iter()
                    .map(|(&actor_id, bitmap)| (actor_id, Arc::new(bitmap.into())))
                    .collect(),
                dropped_actors: update.dropped_actors.iter().cloned().collect(),
            },

            ProstMutation::Add(add) => Mutation::Add {
                adds: add
                    .actor_dispatchers
                    .iter()
                    .map(|(&actor_id, dispatchers)| (actor_id, dispatchers.dispatchers.clone()))
                    .collect(),
                // TODO: remove this and use `SourceChangesSplit` after we support multiple
                // mutations.
                splits: add
                    .actor_splits
                    .iter()
                    .map(|(&actor_id, splits)| {
                        (
                            actor_id,
                            splits
                                .splits
                                .iter()
                                .map(|split| split.try_into().unwrap())
                                .collect(),
                        )
                    })
                    .collect(),
            },

            ProstMutation::Splits(s) => {
                let mut change_splits: Vec<(ActorId, ConnectorState)> =
                    Vec::with_capacity(s.actor_splits.len());
                for (&actor_id, splits) in &s.actor_splits {
                    if splits.splits.is_empty() {
                        change_splits.push((actor_id, None));
                    } else {
                        change_splits.push((
                            actor_id,
                            Some(
                                splits
                                    .splits
                                    .iter()
                                    .map(SplitImpl::try_from)
                                    .collect::<anyhow::Result<Vec<SplitImpl>>>()
                                    .to_rw_result()?,
                            ),
                        ));
                    }
                }
                Mutation::SourceChangeSplit(
                    change_splits
                        .into_iter()
                        .collect::<HashMap<ActorId, ConnectorState>>(),
                )
            }
            ProstMutation::Pause(_) => Mutation::Pause,
            ProstMutation::Resume(_) => Mutation::Resume,
        };
        Ok(mutation)
    }
}

impl Barrier {
    pub fn to_protobuf(&self) -> ProstBarrier {
        let Barrier {
            epoch,
            mutation,
            checkpoint,
            passed_actors,
            ..
        }: Barrier = self.clone();
        ProstBarrier {
            epoch: Some(ProstEpoch {
                curr: epoch.curr,
                prev: epoch.prev,
            }),
            mutation: mutation.map(|mutation| mutation.to_protobuf()),
            span: vec![],
            checkpoint,
            passed_actors,
        }
    }

    pub fn from_protobuf(prost: &ProstBarrier) -> Result<Self> {
        let mutation = prost
            .mutation
            .as_ref()
            .map(Mutation::from_protobuf)
            .transpose()?
            .map(Arc::new);
        let epoch = prost.get_epoch().unwrap();
        Ok(Barrier {
            checkpoint: prost.checkpoint,
            epoch: Epoch::new(epoch.curr, epoch.prev),
            mutation,
            passed_actors: prost.get_passed_actors().clone(),
        })
    }
}

#[derive(Debug, EnumAsInner, PartialEq)]
pub enum Message {
    Chunk(StreamChunk),
    Barrier(Barrier),
}

impl<'a> TryFrom<&'a Message> for &'a Barrier {
    type Error = ();

    fn try_from(m: &'a Message) -> std::result::Result<Self, Self::Error> {
        match m {
            Message::Chunk(_) => Err(()),
            Message::Barrier(b) => Ok(b),
        }
    }
}

impl Message {
    /// Return true if the message is a stop barrier, meaning the stream
    /// will not continue, false otherwise.
    ///
    /// Note that this does not mean we will stop the current actor.
    #[cfg(test)]
    pub fn is_stop(&self) -> bool {
        matches!(
            self,
            Message::Barrier(Barrier {
                mutation,
                ..
            }) if mutation.as_ref().unwrap().is_stop()
        )
    }

    pub fn to_protobuf(&self) -> Result<ProstStreamMessage> {
        let prost = match self {
            Self::Chunk(stream_chunk) => {
                let prost_stream_chunk = stream_chunk.to_protobuf();
                StreamMessage::StreamChunk(prost_stream_chunk)
            }
            Self::Barrier(barrier) => StreamMessage::Barrier(barrier.clone().to_protobuf()),
        };
        let prost_stream_msg = ProstStreamMessage {
            stream_message: Some(prost),
        };
        Ok(prost_stream_msg)
    }

    pub fn from_protobuf(prost: &ProstStreamMessage) -> Result<Self> {
        let res = match prost.get_stream_message()? {
            StreamMessage::StreamChunk(ref stream_chunk) => {
                Message::Chunk(StreamChunk::from_protobuf(stream_chunk)?)
            }
            StreamMessage::Barrier(ref barrier) => {
                Message::Barrier(Barrier::from_protobuf(barrier)?)
            }
        };
        Ok(res)
    }

    pub fn get_encoded_len(msg: &impl ::prost::Message) -> usize {
        ::prost::Message::encoded_len(msg)
    }
}

pub type PkIndices = Vec<usize>;
pub type PkIndicesRef<'a> = &'a [usize];
pub type PkDataTypes = SmallVec<[DataType; 1]>;

/// Expect the first message of the given `stream` as a barrier.
#[track_caller]
pub async fn expect_first_barrier(
    stream: &mut (impl MessageStream + Unpin),
) -> StreamExecutorResult<Barrier> {
    let message = stream
        .next()
        .stack_trace("expect_first_barrier")
        .await
        .expect("failed to extract the first message: stream closed unexpectedly")?;
    let barrier = message
        .into_barrier()
        .expect("the first message must be a barrier");
    Ok(barrier)
}

/// `StreamConsumer` is the last step in an actor.
pub trait StreamConsumer: Send + 'static {
    type BarrierStream: Stream<Item = Result<Barrier>> + Send;

    fn execute(self: Box<Self>) -> Self::BarrierStream;
}
