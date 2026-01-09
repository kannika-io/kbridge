// #[cfg(test)]
// mod mock {
//     use std::{
//         pin::Pin,
//         sync::{Arc, Mutex},
//         task::{Context, Poll},
//     };

//     use futures::StreamExt;

//     use crate::partition::seek::SeekablePartition;

//     use super::*;

//     #[derive(Debug, Clone, PartialEq, Eq)]
//     pub struct MockMessage {
//         pub offset: i64,
//         pub timestamp: i64,
//         pub headers: HashMap<String, Vec<u8>>,
//     }

//     pub struct MockPartition {
//         messages: Vec<MockMessage>,
//         position: Arc<Mutex<ConsumerPosition>>,
//     }

//     struct ConsumerPosition {
//         pub index: usize,
//         pub offset: i64,
//     }

//     pub struct MockStream<'a> {
//         partition: &'a MockPartition,
//         position: Arc<Mutex<ConsumerPosition>>,
//     }

//     impl PartialOrd for MockMessage {
//         fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//             Some(self.offset.cmp(&other.offset))
//         }
//     }

//     impl Message for MockMessage {
//         fn offset(&self) -> i64 {
//             self.offset
//         }
//         fn timestamp(&self) -> i64 {
//             self.timestamp
//         }

//         fn headers(&self) -> HashMap<String, Vec<u8>> {
//             self.headers.clone()
//         }

//         fn header(&self, _key: impl Into<String>) -> Option<Vec<u8>> {
//             self.headers.get(&_key.into()).cloned()
//         }
//     }

// impl MockPartition {
//     pub fn new(messages: Vec<(i64, i64)>) -> Self {
//         let mut messages: Vec<_> = messages
//             .into_iter()
//             .map(|(offset, timestamp)| MockMessage {
//                 offset,
//                 timestamp,
//                 headers: HashMap::new(),
//             })
//             .collect();

//         messages.sort_by(|a, b| a.offset.cmp(&b.offset));

//         // Time should only increase with offset
//         assert!(
//             messages
//                 .windows(2)
//                 .all(|w| w[0].timestamp <= w[1].timestamp)
//         );

//         Self {
//             position: Arc::new(Mutex::new(ConsumerPosition {
//                 offset: 0,
//                 index: 0,
//             })),
//             messages,
//         }
//     }

//     pub fn stream(&self) -> MockStream {
//         MockStream::new(self.position.clone(), self.messages.clone())
//     }
// }

// impl Partition for MockPartition {
//     type Error = String;
//     type Message = MockMessage;

//     fn stream(&self) -> impl Stream<Item = Result<Self::Message, Self::Error>> {
//         MockStream::new(self.position.clone(), self.messages.clone())
//     }

// fn seek(&mut self, offset: i64) -> Result<(), String> {
//     let mut pos = self.position.lock().unwrap();
//     // scan messages
//     if self.messages.iter().any(|msg| msg.offset >= offset) {
//         pos.offset = offset;
//     } else {
//         return Err(format!("No message with offset >= {}", offset));
//     }
//     Ok(())
// }

// fn offset_for_timestamp(&mut self, timestamp: i64) -> Result<i64, String> {
//     self.messages
//         .iter()
//         .find(|msg| msg.timestamp >= timestamp)
//         .map(|msg| msg.offset)
//         .ok_or_else(|| format!("No offset for timestamp {}", timestamp))
// }
// }

// impl MockStream {
//     pub fn new(position: Arc<Mutex<ConsumerPosition>>, messages: Vec<MockMessage>) -> Self {
//         Self {
//             position,
//             messages: messages.into_iter().map(Ok).collect(),
//         }
//     }
// }

// impl Stream for MockStream {
//     type Item = Result<MockMessage, String>;

//     fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         // Clone and move position
//         if self.messages.is_empty() {
//             Poll::Ready(None)
//         } else {
//             let mut pos = self.position.lock().unwrap();
//             let current_offset = pos.offset;
//             let current_index = pos.index;
//             // Find the first message with offset >= current_offset, starting from current_index

//             let msg: Option<(Result<MockMessage, String>, usize)> = self
//                 .messages
//                 .iter()
//                 .enumerate()
//                 .skip(current_index)
//                 .find_map(|(i, msg)| match msg {
//                     Ok(m) if m.offset >= current_offset => Some((msg.clone(), i)),
//                     _ => None,
//                 });

//             match msg {
//                 Some((Ok(m), i)) => {
//                     // Update position
//                     pos.index = i;
//                     pos.offset = m.offset + 1; // next offset
//                     Poll::
//                 }
//                 Some((Err(e), i)) => Poll::Ready(Some(Err(e.clone()))),
//                 None => Poll::Pending,
//             }
//         }
//     }
// }

// impl Unpin for MockStream {}

// pub struct MockSeekableConsumer {
//     inner: SeekablePartition<MockPartition>,
// }

// impl MockSeekableConsumer {
//     pub fn new(messages: Vec<(i64, i64)>) -> Self {
//         let backend = MockPartition::new(messages);
//         let inner = SeekablePartition::new(backend);
//         Self { inner }
//     }
// }

// #[test]
// fn test_mock_seekable_consumer() {
//     let messages = vec![(0, 1000), (1, 2000), (2, 3000), (3, 4000)];
//     let mut consumer = MockSeekableConsumer::new(messages);

//     let stream = consumer.stream();

//     // assert we get first message when consuming the strea
//     assert_eq!(stream.next().unwrap().unwrap().offset(), 0);
//     // Seek offset to third
//     // assert we get third

//     // Test seeking to timestamp
//     let offset = consumer.inner.seek_from_timestamp(2500).unwrap();
//     assert_eq!(offset, 2);
//     assert_eq!(consumer.inner.seek_offset(), Some(2));
// }
// }
