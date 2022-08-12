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

use async_trait::async_trait;
use futures::future;

use super::error::SourceResult;
use crate::source::{Column, ConnectorState, SourceMessage, SplitReader};

/// [`DummySplitReader`] is a placeholder for source executor that is assigned no split. It will
/// wait forever when calling `next`.
#[derive(Clone, Debug)]
pub struct DummySplitReader;

#[async_trait]
impl SplitReader for DummySplitReader {
    type Properties = ();

    async fn new(
        _properties: Self::Properties,
        _state: ConnectorState,
        _columns: Option<Vec<Column>>,
    ) -> SourceResult<Self> {
        Ok(Self {})
    }

    async fn next(&mut self) -> SourceResult<Option<Vec<SourceMessage>>> {
        let pending = future::pending();
        let () = pending.await;

        unreachable!()
    }
}
