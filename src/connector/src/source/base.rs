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

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use enum_as_inner::EnumAsInner;
use futures::{pin_mut, Stream, StreamExt};
use itertools::Itertools;
use prost::Message;
use risingwave_common::error::ErrorCode;
use risingwave_pb::source::ConnectorSplit;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::source::datagen::{
    DatagenProperties, DatagenSplit, DatagenSplitEnumerator, DatagenSplitReader, DATAGEN_CONNECTOR,
};
use crate::source::dummy_connector::DummySplitReader;
use crate::source::filesystem::s3::{S3Properties, S3_CONNECTOR};
use crate::source::kafka::enumerator::KafkaSplitEnumerator;
use crate::source::kafka::source::KafkaSplitReader;
use crate::source::kafka::{KafkaProperties, KafkaSplit, KAFKA_CONNECTOR};
use crate::source::kinesis::enumerator::client::KinesisSplitEnumerator;
use crate::source::kinesis::source::reader::KinesisMultiSplitReader;
use crate::source::kinesis::split::KinesisSplit;
use crate::source::kinesis::{KinesisProperties, KINESIS_CONNECTOR};
use crate::source::nexmark::source::reader::NexmarkSplitReader;
use crate::source::nexmark::{
    NexmarkProperties, NexmarkSplit, NexmarkSplitEnumerator, NEXMARK_CONNECTOR,
};
use crate::source::pulsar::source::reader::PulsarSplitReader;
use crate::source::pulsar::{
    PulsarProperties, PulsarSplit, PulsarSplitEnumerator, PULSAR_CONNECTOR,
};
use crate::{impl_connector_properties, impl_split, impl_split_enumerator, impl_split_reader};

/// [`SplitEnumerator`] fetches the split metadata from the external source service.
/// NOTE: It runs in the meta server, so probably it should be moved to the `meta` crate.
#[async_trait]
pub trait SplitEnumerator: Sized {
    type Split: SplitMetaData + Send + Sync;
    type Properties;

    async fn new(properties: Self::Properties) -> Result<Self>;
    async fn list_splits(&mut self) -> Result<Vec<Self::Split>>;
}

/// [`SplitReader`] is an abstraction of the external connector read interface,
/// used to read messages from the outside and transform them into source-oriented
/// [`SourceMessage`], in order to improve throughput, it is recommended to return a batch of
/// messages at a time, [`Option`] is used to be compatible with the Stream API, but the stream of a
/// Streaming system should not end
#[async_trait]
pub trait SplitReader: Sized {
    type Properties;

    async fn new(
        properties: Self::Properties,
        state: ConnectorState,
        columns: Option<Vec<Column>>,
    ) -> Result<Self>;

    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>>;
}

#[derive(Debug, Clone, Serialize, Deserialize, EnumAsInner, PartialEq, Hash)]
pub enum SplitImpl {
    Kafka(KafkaSplit),
    Pulsar(PulsarSplit),
    Kinesis(KinesisSplit),
    Nexmark(NexmarkSplit),
    Datagen(DatagenSplit),
}

pub enum SplitReaderImpl {
    Kinesis(Box<KinesisMultiSplitReader>),
    Kafka(Box<KafkaSplitReader>),
    Dummy(Box<DummySplitReader>),
    Nexmark(Box<NexmarkSplitReader>),
    Pulsar(Box<PulsarSplitReader>),
    Datagen(Box<DatagenSplitReader>),
}

pub enum SplitEnumeratorImpl {
    Kafka(KafkaSplitEnumerator),
    Pulsar(PulsarSplitEnumerator),
    Kinesis(KinesisSplitEnumerator),
    Nexmark(NexmarkSplitEnumerator),
    Datagen(DatagenSplitEnumerator),
}

#[derive(Clone, Debug, Deserialize)]
pub enum ConnectorProperties {
    Kafka(KafkaProperties),
    Pulsar(PulsarProperties),
    Kinesis(KinesisProperties),
    Nexmark(NexmarkProperties),
    Datagen(DatagenProperties),
    S3(S3Properties),
    Dummy(()),
}

impl_connector_properties! {
    [ ] ,
    { Kafka, KAFKA_CONNECTOR },
    { Pulsar, PULSAR_CONNECTOR },
    { Kinesis, KINESIS_CONNECTOR },
    { Nexmark, NEXMARK_CONNECTOR },
    { Datagen, DATAGEN_CONNECTOR },
    { S3, S3_CONNECTOR }
}

impl_split_enumerator! {
    [ ] ,
    { Kafka, KafkaSplitEnumerator },
    { Pulsar, PulsarSplitEnumerator },
    { Kinesis, KinesisSplitEnumerator },
    { Nexmark, NexmarkSplitEnumerator },
    { Datagen, DatagenSplitEnumerator }
}

impl_split! {
    [ ] ,
    { Kafka, KAFKA_CONNECTOR, KafkaSplit },
    { Pulsar, PULSAR_CONNECTOR, PulsarSplit },
    { Kinesis, KINESIS_CONNECTOR, KinesisSplit },
    { Nexmark, NEXMARK_CONNECTOR, NexmarkSplit },
    { Datagen, DATAGEN_CONNECTOR, DatagenSplit }
}

impl_split_reader! {
    [ ] ,
    { Kafka, KafkaSplitReader },
    { Pulsar, PulsarSplitReader },
    { Kinesis, KinesisMultiSplitReader },
    { Nexmark, NexmarkSplitReader },
    { Datagen, DatagenSplitReader },
    { Dummy, DummySplitReader }
}

pub type DataType = risingwave_common::types::DataType;

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

/// Split id resides in every source message, use `Arc` to avoid copying.
pub type SplitId = Arc<str>;

/// The message pumped from the external source service.
/// The third-party message structs will eventually be transformed into this struct.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SourceMessage {
    pub payload: Option<Bytes>,
    pub offset: String,
    pub split_id: SplitId,
}

/// The metadata of a split.
pub trait SplitMetaData: Sized {
    fn id(&self) -> SplitId;
    fn encode_to_bytes(&self) -> Bytes;
    fn restore_from_bytes(bytes: &[u8]) -> Result<Self>;
}

/// [`ConnectorState`] maintains the consuming splits' info. In specific split readers,
/// `ConnectorState` cannot be [`None`] and only contains one [`SplitImpl`]. If no split is assigned
/// to source executor, `ConnectorState` is [`None`] and [`DummySplitReader`] is up instead of other
/// split readers.
pub type ConnectorState = Option<Vec<SplitImpl>>;

/// Used for acquiring the generated data for [`spawn_data_generation_stream`].
pub type DataGenerationReceiver = mpsc::Receiver<Result<Vec<SourceMessage>>>;

/// Spawn a **new runtime** in a new thread to run the data generator, returns a channel receiver
/// for acquiring the generated data. This is used for the [`DatagenSplitReader`] and
/// [`NexmarkSplitReader`] in case that they are CPU intensive and may block the streaming actors.
pub fn spawn_data_generation_stream(
    stream: impl Stream<Item = Result<Vec<SourceMessage>>> + Send + 'static,
) -> DataGenerationReceiver {
    const GENERATION_BUFFER: usize = 4;

    let (generation_tx, generation_rx) = mpsc::channel(GENERATION_BUFFER);
    let future = async move {
        pin_mut!(stream);
        while let Some(result) = stream.next().await {
            if generation_tx.send(result).await.is_err() {
                tracing::warn!("failed to send next event to reader, exit");
                break;
            }
        }
    };

    #[cfg(not(madsim))]
    std::thread::Builder::new()
        .name("risingwave-data-generation".to_string())
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(future);
        })
        .unwrap();

    // Note: madsim does not support creating multiple runtime, so we just run it in current
    // runtime.
    #[cfg(madsim)]
    tokio::spawn(future);

    generation_rx
}

#[cfg(test)]
mod tests {
    use maplit::*;

    use super::*;

    #[test]
    fn test_split_impl_get_fn() -> Result<()> {
        let split = KafkaSplit::new(0, Some(0), Some(0), "demo".to_string());
        let split_impl = SplitImpl::Kafka(split.clone());
        let get_value = split_impl.into_kafka().unwrap();
        println!("{:?}", get_value);
        assert_eq!(split.encode_to_bytes(), get_value.encode_to_bytes());

        Ok(())
    }

    #[test]
    fn test_extract_nexmark_config() {
        let props: HashMap<String, String> = convert_args!(hashmap!(
            "connector" => "nexmark",
            "nexmark.table.type" => "Person",
            "nexmark.split.num" => "1",
        ));

        let props = ConnectorProperties::extract(props).unwrap();

        if let ConnectorProperties::Nexmark(props) = props {
            assert_eq!(props.table_type, "Person");
            assert_eq!(props.split_num, 1);
        } else {
            panic!("extract nexmark config failed");
        }
    }
}
