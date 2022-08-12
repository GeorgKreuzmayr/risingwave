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

use std::cmp;
use std::ops::{Add, DerefMut};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Notify;

// Token bucket rate limiter.
pub struct RateLimiter {
    // The three locks never overlap.
    current_token_num: parking_lot::Mutex<u64>,
    notifier: parking_lot::Mutex<Option<Arc<Notify>>>,
    next_add_time: parking_lot::Mutex<Instant>,
    token_add_per_interval: u64,
    token_add_interval: Duration,
}

impl RateLimiter {
    pub fn new(token_add_per_interval: u64, token_add_interval: Duration) -> Self {
        Self {
            current_token_num: parking_lot::Mutex::new(0),
            notifier: parking_lot::Mutex::new(None),
            token_add_per_interval,
            token_add_interval,
            next_add_time: parking_lot::Mutex::new(Instant::now()),
        }
    }

    pub async fn remove_token(&self, mut require_token_num: u64) {
        loop {
            {
                let mut guard = self.current_token_num.lock();
                let current_token = guard.deref_mut();
                if *current_token >= require_token_num {
                    *current_token -= require_token_num;
                    return;
                }
                require_token_num -= *current_token;
                *current_token = 0;
            }
            let (notify, is_leader) = {
                let mut guard = self.notifier.lock();
                match guard.deref_mut() {
                    None => {
                        let notify = Arc::new(Notify::new());
                        *guard = Some(notify.clone());
                        (notify, true)
                    }
                    Some(notify) => (notify.clone(), false),
                }
            };
            if !is_leader {
                notify.notified().await;
                notify.notify_one();
                continue;
            }
            let now = Instant::now();
            let time_to_wait = self.next_add_time.lock().duration_since(now);
            tokio::time::sleep(time_to_wait).await;
            *self.next_add_time.lock() = now.add(self.token_add_interval);
            *self.current_token_num.lock() += self.token_add_per_interval;
            self.notifier.lock().take().unwrap().notify_one();
        }
    }
}
