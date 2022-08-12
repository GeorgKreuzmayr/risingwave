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

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_object_store::object::{ObjectStore, ObjectStoreImpl, S3ObjectStore};
use risingwave_storage::hummock::multi_builder::{
    get_sst_writer_and_sealer_for_batch_upload, get_sst_writer_and_sealer_for_streaming_upload,
    CapacitySplitTableBuilder, LocalTableBuilderFactory, SstableBuilderSealer, TableBuilderFactory,
};
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{
    CachePolicy, CompressionAlgorithm, SstableBuilderOptions, SstableStore, SstableWriterBuilder,
    TieredCache,
};
use risingwave_storage::monitor::ObjectStoreMetrics;

const RANGE: Range<u64> = 0..2500000;
const VALUE: &[u8] = &[0; 400];
const SAMPLE_COUNT: usize = 10;
const ESTIMATED_MEASUREMENT_TIME: Duration = Duration::from_secs(50);

fn get_builder_options(capacity_mb: usize) -> SstableBuilderOptions {
    SstableBuilderOptions {
        capacity: capacity_mb * 1024 * 1024,
        block_capacity: 1 * 1024 * 1024,
        restart_interval: 16,
        bloom_false_positive: 0.01,
        compression_algorithm: CompressionAlgorithm::None,
        estimate_bloom_filter_capacity: 1024 * 1024,
    }
}

fn get_builder<B, S>(
    writer_builder: B,
    builder_sealer: S,
    options: SstableBuilderOptions,
) -> CapacitySplitTableBuilder<LocalTableBuilderFactory<B>, B, S>
where
    B: SstableWriterBuilder,
    S: SstableBuilderSealer<B::Writer>,
{
    CapacitySplitTableBuilder::new(
        LocalTableBuilderFactory::new(1, writer_builder, options),
        builder_sealer,
    )
}

async fn build_tables<F, B, S>(mut builder: CapacitySplitTableBuilder<F, B, S>)
where
    F: TableBuilderFactory<B>,
    B: SstableWriterBuilder,
    S: SstableBuilderSealer<B::Writer>,
{
    for i in RANGE {
        builder
            .add_user_key(i.to_be_bytes().to_vec(), HummockValue::put(VALUE), 1)
            .await
            .unwrap();
    }
    let sealed_builders = builder.finish().await.unwrap();
    let join_handles = sealed_builders
        .into_iter()
        .map(|b| b.upload_join_handle)
        .collect_vec();
    try_join_all(join_handles).await.unwrap();
}

fn bench_builder(c: &mut Criterion, capacity_mb: usize, enable_streaming_upload: bool) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let options = get_builder_options(capacity_mb);
    let object_store = Arc::new(ObjectStoreImpl::S3(runtime.block_on(async {
        S3ObjectStore::new("zhidong-s3-bench".to_string())
            .await
            .monitored(Arc::new(ObjectStoreMetrics::unused()))
    })));
    let sstable_store = Arc::new(SstableStore::new(
        object_store,
        "test".to_string(),
        64 << 20,
        128 << 20,
        TieredCache::none(),
    ));
    let policy = CachePolicy::NotFill;

    let mut group = c.benchmark_group("bench_multi_builder");
    group
        .sample_size(SAMPLE_COUNT)
        .measurement_time(ESTIMATED_MEASUREMENT_TIME);
    if enable_streaming_upload {
        group.bench_function(format!("bench_streaming_upload_{}mb", capacity_mb), |b| {
            b.to_async(&runtime).iter(|| {
                let (writer_builder, builder_sealer) =
                    get_sst_writer_and_sealer_for_streaming_upload(sstable_store.clone(), policy);
                build_tables(get_builder(writer_builder, builder_sealer, options.clone()))
            })
        });
    } else {
        group.bench_function(format!("bench_batch_upload_{}mb", capacity_mb), |b| {
            b.to_async(&runtime).iter(|| {
                let (writer_builder, builder_sealer) = get_sst_writer_and_sealer_for_batch_upload(
                    &options,
                    sstable_store.clone(),
                    policy,
                );
                build_tables(get_builder(writer_builder, builder_sealer, options.clone()))
            })
        });
    }
    group.finish();
}

// SST size: 32, 64, 128, 256MiB
fn bench_multi_builder(c: &mut Criterion) {
    let sst_capacities = vec![32, 64, 128, 256];
    for capacity in sst_capacities {
        bench_builder(c, capacity, false);
        bench_builder(c, capacity, true);
    }
}

criterion_group!(benches, bench_multi_builder);
criterion_main!(benches);
