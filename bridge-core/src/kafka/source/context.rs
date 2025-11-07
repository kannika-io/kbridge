use crate::prelude::*;

use std::{
    collections::HashMap,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

use futures::Stream;
use futures::{StreamExt, channel::mpsc};

use lazy_static::lazy_static;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::ConsumerContext;
use regex::Regex;

use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, thiserror::Error)]
pub enum OutOfBandError {
    #[error("Partitions count shrunk")]
    PartitionCountShrunk,
    #[error("Other out-of-band error: {0}")]
    Other(String),
}

/// A consumer context that forwards out-of-band errors to clients that have subscribed to it.
#[derive(Debug, Default)]
pub struct CustomConsumerContext {
    err_clients: Mutex<HashMap<TopicName, Vec<mpsc::UnboundedSender<OutOfBandError>>>>,
}

impl ConsumerContext for CustomConsumerContext {}

impl rdkafka::ClientContext for CustomConsumerContext {
    #[tracing::instrument(skip_all)]
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => {
                error!("{fac}: {log_message}");
            }
            RDKafkaLogLevel::Warning => {
                warn!("{fac}: {log_message}");
            }
            RDKafkaLogLevel::Notice | RDKafkaLogLevel::Info => {
                info!("{fac}: {log_message}");
            }
            RDKafkaLogLevel::Debug => {
                debug!("{fac}: {log_message}");
            }
        }
        match fac {
            "OFFSET" => {
                if let Some(topic) = parse_offset_warning_message(log_message) {
                    warn!(
                        topic,
                        "Got an Offset Reset warning. Some records may have been missed."
                    );
                } else {
                    warn!("Couldn't parse OFFSET log message `{log_message}`");
                }
            }
            "PARTCNT" => match parse_partcnt_warning_message(log_message) {
                Some((topic, from, to)) if from > to => {
                    self.broadcast_notice(&topic, OutOfBandError::PartitionCountShrunk);
                }
                Some(_) => {
                    // Partition count increased. Should be fine.
                }
                None => {
                    warn!("Couldn't parse PARTCNT log message `{log_message}`");
                } // TODO: stop the task if the number of partition decreases
            },
            _ => {}
        }
    }

    #[tracing::instrument(skip_all)]
    fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
        error!("{error}: '{reason}'");
    }
}

impl CustomConsumerContext {
    /// Creates a channel for out-of-band errors for a specific topic.
    pub fn subscribe_to_out_of_band_errors(&self, topic: TopicName) -> TopicOutOfBandErrorStream {
        let (tx, rx) = mpsc::unbounded();
        self.err_clients
            .lock()
            .unwrap()
            .entry(topic)
            .or_default()
            .push(tx);
        TopicOutOfBandErrorStream { inner: rx }
    }

    /// Broadcast the error to all subscribed clients interested in errors for a particular topic
    fn broadcast_notice(&self, topic: &TopicName, notice: OutOfBandError) {
        let mut err_clients = self.err_clients.lock().unwrap();

        for client in err_clients.get(topic).into_iter().flatten() {
            client.unbounded_send(notice.clone()).ok();
        }

        // Cleanup clients that are gone
        for (_topic, clients) in err_clients.iter_mut() {
            clients.retain(|cl| !cl.is_closed());
        }
    }
}

pub struct TopicOutOfBandErrorStream {
    inner: mpsc::UnboundedReceiver<OutOfBandError>,
}

impl Stream for TopicOutOfBandErrorStream {
    type Item = OutOfBandError;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

/// Extract the topic name from an "OFFSET" warning message.
fn parse_offset_warning_message(err: &str) -> Option<TopicName> {
    let (_thread, rest) = err.split_once(' ')?;
    let (topic, _) = rest.split_once(' ')?;
    Some(topic.to_owned())
}

/// Extract the topic and the from/to partition counts from a "PARTCNT" warning message.
fn parse_partcnt_warning_message(err: &str) -> Option<(TopicName, u32, u32)> {
    lazy_static! {
        static ref RE: Regex =
            Regex::new(r"^Topic ([^ ]+) partition count changed from (\d+) to (\d+)").unwrap();
    }

    let (_thread, rest) = err.split_once(' ')?;
    let captures = RE.captures(rest)?;

    let topic = captures.get(1).unwrap().as_str().to_owned();
    let from = captures.get(2).unwrap().as_str().parse().unwrap();
    let to = captures.get(3).unwrap().as_str().parse().unwrap();

    Some((topic, from, to))
}
