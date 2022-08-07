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

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use aws_sdk_s3::model::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::output::UploadPartOutput;
use aws_sdk_s3::{Client, Endpoint, Region};
use fail::fail_point;
use futures::future::try_join_all;
use itertools::Itertools;
use tokio::sync::Mutex;

use crate::object::multipart::{
    MultipartUploadHandle, MultipartUploadHandleImpl, PartId, PartIdGenerator, PartIdGeneratorImpl,
};
use crate::object::{
    BlockLocation, Bytes, MultipartUpload, ObjectError, ObjectMetadata, ObjectResult, ObjectStore,
};

const MIN_PART_ID: i32 = 1;
const MAX_PART_ID: i32 = 10000;

/// Part number generator for S3 multipart upload.
pub struct S3PartIdGenerator {
    next_id: AtomicI32,
}

impl S3PartIdGenerator {
    fn new() -> S3PartIdGenerator {
        Self {
            next_id: AtomicI32::new(MIN_PART_ID),
        }
    }
}

impl PartIdGenerator for S3PartIdGenerator {
    fn gen(&self) -> ObjectResult<PartId> {
        let part_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if part_id > MAX_PART_ID {
            Err(ObjectError::internal(format!(
                "reached maximum S3 part number {}",
                MAX_PART_ID
            )))
        } else {
            Ok(part_id.try_into().unwrap())
        }
    }
}

/// S3 multipart upload handle.
/// Reference: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html>
pub struct S3MultipartUploadHandle {
    client: Arc<Client>,
    bucket: String,
    /// The key of the object.
    key: String,
    /// The identifier of multipart upload task for S3.
    upload_id: String,
    /// Parts that are already uploaded to S3.
    /// The bool member indicates whether the upload is finished.
    uploaded_parts: Mutex<(bool, BTreeMap<i32, UploadPartOutput>)>,
}

#[async_trait::async_trait]
impl MultipartUploadHandle for S3MultipartUploadHandle {
    async fn upload_part(&self, part_id: PartId, part: Bytes) -> ObjectResult<()> {
        fail_point!("s3_upload_part_err", |_| Err(ObjectError::internal(
            "s3 upload part error"
        )));
        let mut upload_guard = self.uploaded_parts.lock().await;
        if !upload_guard.0 {
            let part_id = part_id as i32;
            let upload_output = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(&self.key)
                .upload_id(&self.upload_id)
                .part_number(part_id)
                .body(aws_sdk_s3::types::ByteStream::from(part))
                .send()
                .await?;
            upload_guard.1.insert(part_id, upload_output);
        } else {
            panic!("an upload task must be created first before the parts are uploaded");
        }
        Ok(())
    }

    async fn finish(self) -> ObjectResult<()> {
        fail_point!("s3_finish_multipart_upload_err", |_| Err(
            ObjectError::internal("s3 finish multipart upload error")
        ));
        let mut upload_guard = self.uploaded_parts.lock().await;
        if !upload_guard.0 {
            self.client
                .complete_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .upload_id(&self.upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(
                            upload_guard
                                .1
                                .iter()
                                .map(|(part_id, output)| {
                                    CompletedPart::builder()
                                        .set_e_tag(output.e_tag.clone())
                                        .set_part_number(Some(*part_id))
                                        .build()
                                })
                                .collect_vec(),
                        ))
                        .build(),
                )
                .send()
                .await?;
            upload_guard.0 = true;
        } else {
            panic!("an upload task must be created first before finishing it");
        }
        Ok(())
    }
}
/// Object store with S3 backend
pub struct S3ObjectStore {
    client: Arc<Client>,
    bucket: String,
}

#[async_trait::async_trait]
impl ObjectStore for S3ObjectStore {
    async fn upload(&self, path: &str, obj: Bytes) -> ObjectResult<()> {
        fail_point!("s3_upload_err", |_| Err(ObjectError::internal(
            "s3 upload error"
        )));
        self.client
            .put_object()
            .bucket(&self.bucket)
            .body(aws_sdk_s3::types::ByteStream::from(obj))
            .key(path)
            .send()
            .await?;
        Ok(())
    }

    /// Amazon S3 doesn't support retrieving multiple ranges of data per GET request.
    async fn read(&self, path: &str, block_loc: Option<BlockLocation>) -> ObjectResult<Bytes> {
        fail_point!("s3_read_err", |_| Err(ObjectError::internal(
            "s3 read error"
        )));
        let req = self.client.get_object().bucket(&self.bucket).key(path);

        let range = match block_loc.as_ref() {
            None => None,
            Some(block_location) => block_location.byte_range_specifier(),
        };

        let req = if let Some(range) = range {
            req.range(range)
        } else {
            req
        };

        let resp = req.send().await?;
        let val = resp.body.collect().await?.into_bytes();

        if block_loc.is_some() && block_loc.as_ref().unwrap().size != val.len() {
            return Err(ObjectError::internal(format!(
                "mismatched size: expected {}, found {} when reading {} at {:?}",
                block_loc.as_ref().unwrap().size,
                val.len(),
                path,
                block_loc.as_ref().unwrap()
            )));
        }
        Ok(val)
    }

    async fn readv(&self, path: &str, block_locs: &[BlockLocation]) -> ObjectResult<Vec<Bytes>> {
        let futures = block_locs
            .iter()
            .map(|block_loc| self.read(path, Some(*block_loc)))
            .collect_vec();
        try_join_all(futures).await
    }

    async fn metadata(&self, path: &str) -> ObjectResult<ObjectMetadata> {
        fail_point!("s3_metadata_err", |_| Err(ObjectError::internal(
            "s3 metadata error"
        )));
        let resp = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await?;
        Ok(ObjectMetadata {
            key: path.to_owned(),
            last_modified: resp
                .last_modified()
                .expect("last_modified required")
                .as_secs_f64(),
            total_size: resp.content_length as usize,
        })
    }

    /// Permanently deletes the whole object.
    /// According to Amazon S3, this will simply return Ok if the object does not exist.
    async fn delete(&self, path: &str) -> ObjectResult<()> {
        fail_point!("s3_delete_err", |_| Err(ObjectError::internal(
            "s3 delete error"
        )));
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await?;
        Ok(())
    }

    async fn list(&self, prefix: &str) -> ObjectResult<Vec<ObjectMetadata>> {
        let mut ret: Vec<ObjectMetadata> = vec![];
        let mut next_continuation_token = None;
        // list_objects_v2 returns up to 1000 keys and truncated the exceeded parts.
        // Use `continuation_token` given by last response to fetch more parts of the result,
        // until result is no longer truncated.
        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(continuation_token) = next_continuation_token.take() {
                request = request.continuation_token(continuation_token);
            }
            let result = request.send().await?;
            let is_truncated = result.is_truncated;
            ret.append(
                &mut result
                    .contents()
                    .unwrap_or_default()
                    .iter()
                    .map(|obj| ObjectMetadata {
                        key: obj.key().expect("key required").to_owned(),
                        last_modified: obj
                            .last_modified()
                            .expect("last_modified required")
                            .as_secs_f64(),
                        total_size: obj.size() as usize,
                    })
                    .collect_vec(),
            );
            next_continuation_token = result.next_continuation_token;
            if !is_truncated {
                break;
            }
        }
        Ok(ret)
    }
}

#[async_trait::async_trait]
impl MultipartUpload for S3ObjectStore {
    type Handle = MultipartUploadHandleImpl;
    type IdGen = PartIdGeneratorImpl;

    async fn create_multipart_upload(
        &self,
        path: &str,
    ) -> ObjectResult<(Self::Handle, Self::IdGen)> {
        fail_point!("s3_create_multipart_upload_err", |_| Err(
            ObjectError::internal("s3 create multipart upload error")
        ));
        let resp = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await?;
        Ok((
            Self::Handle::S3(S3MultipartUploadHandle {
                client: self.client.clone(),
                bucket: self.bucket.clone(),
                key: path.to_string(),
                upload_id: resp.upload_id.unwrap(),
                uploaded_parts: Default::default(),
            }),
            Self::IdGen::S3(S3PartIdGenerator::new()),
        ))
    }
}

impl S3ObjectStore {
    /// Creates an S3 object store from environment variable.
    ///
    /// See [AWS Docs](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html) on how to provide credentials and region from env variable. If you are running compute-node on EC2, no configuration is required.
    pub async fn new(bucket: String) -> Self {
        let shared_config = aws_config::load_from_env().await;
        let client = Arc::new(Client::new(&shared_config));

        Self { client, bucket }
    }

    /// Creates a minio client. The server should be like `minio://key:secret@address:port/bucket`.
    pub async fn with_minio(server: &str) -> Self {
        let server = server.strip_prefix("minio://").unwrap();
        let (access_key_id, rest) = server.split_once(':').unwrap();
        let (secret_access_key, rest) = rest.split_once('@').unwrap();
        let (address, bucket) = rest.split_once('/').unwrap();

        let loader = aws_config::ConfigLoader::default();
        let builder = aws_sdk_s3::config::Builder::from(&loader.load().await);
        let builder = builder.region(Region::new("custom"));
        let builder = builder.endpoint_resolver(Endpoint::immutable(
            format!("http://{}", address).try_into().unwrap(),
        ));
        let builder = builder.credentials_provider(aws_sdk_s3::Credentials::from_keys(
            access_key_id,
            secret_access_key,
            None,
        ));
        let config = builder.build();
        let client = Arc::new(Client::from_conf(config));
        Self {
            client,
            bucket: bucket.to_string(),
        }
    }
}
