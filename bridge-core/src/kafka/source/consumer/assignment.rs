use std::sync::{Arc, Mutex};

use rdkafka::{TopicPartitionList, consumer::Consumer};

use crate::kafka::source::StreamConsumer;

#[derive(Clone)]
/// Automatically clears topic/partition assignments from a consumer when dropped.
///
/// May be cloned. The first clone dropped will clear the assignment.
pub struct AssignmentGuard {
    inner: Arc<Mutex<Option<AssignmentGuardInner>>>,
}

impl AssignmentGuard {
    pub fn new(consumer: Arc<StreamConsumer>, assignment: TopicPartitionList) -> Self {
        let inner = AssignmentGuardInner {
            consumer,
            assignment,
        };

        Self {
            inner: Arc::new(Mutex::new(Some(inner))),
        }
    }
}

struct AssignmentGuardInner {
    consumer: Arc<StreamConsumer>,
    assignment: TopicPartitionList,
}

impl Drop for AssignmentGuard {
    fn drop(&mut self) {
        // The lock is only held to perform `Option::take()`. Therefore it cannot be poisoned.
        let maybe_inner = self.inner.lock().expect("Can't panic").take();

        if let Some(inner) = maybe_inner {
            tracing::debug!("Unassigning {:?}", inner.assignment);
            if let Err(err) = inner.consumer.incremental_unassign(&inner.assignment) {
                tracing::error!(
                    "Couldn't unassign partitions {:?}. Error: {}",
                    inner.assignment,
                    err
                );
            }
        }
    }
}
