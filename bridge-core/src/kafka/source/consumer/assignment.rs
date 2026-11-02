use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use rdkafka::error::KafkaError;
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

    /// A handle to change the assignment while the guard holds it. Dropping the handle does not clear it.
    pub fn handle(&self) -> AssignmentHandle {
        AssignmentHandle {
            inner: Arc::clone(&self.inner),
        }
    }
}

struct AssignmentGuardInner {
    consumer: Arc<StreamConsumer>,
    assignment: TopicPartitionList,
}

fn lock(
    inner: &Mutex<Option<AssignmentGuardInner>>,
) -> MutexGuard<'_, Option<AssignmentGuardInner>> {
    // The guarded data is never left half-updated, so a poisoned lock is still usable.
    inner.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Changes the assignment of an [`AssignmentGuard`] without owning it.
pub struct AssignmentHandle {
    inner: Arc<Mutex<Option<AssignmentGuardInner>>>,
}

pub enum Reassigned {
    /// The partitions were assigned again.
    Yes,
    /// The guard has already cleared the assignment.
    Released,
    /// The partitions could not be unassigned. The assignment is unchanged.
    NotUnassigned(KafkaError),
    /// The partitions were unassigned but could not be assigned again.
    /// The guard no longer holds them, so it will not unassign them again.
    NotAssigned(KafkaError),
}

impl AssignmentHandle {
    /// Unassigns the guarded partitions and assigns them again at the offsets in `target`.
    ///
    /// Holds the guard's lock throughout, so the guard can't clear the assignment halfway
    /// and the partitions are never assigned again after the guard has let go of them.
    pub fn reassign(&self, target: &TopicPartitionList) -> Reassigned {
        let mut inner = lock(&self.inner);
        let Some(guarded) = inner.as_ref() else {
            return Reassigned::Released;
        };

        if let Err(err) = guarded.consumer.incremental_unassign(target) {
            return Reassigned::NotUnassigned(err);
        }

        if let Err(err) = guarded.consumer.incremental_assign(target) {
            *inner = None;
            return Reassigned::NotAssigned(err);
        }

        Reassigned::Yes
    }
}

impl Drop for AssignmentGuard {
    fn drop(&mut self) {
        // Taking the assignment waits for a running `AssignmentHandle::reassign`,
        // and a later one sees the assignment released.
        let taken = lock(&self.inner).take();

        if let Some(inner) = taken {
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
