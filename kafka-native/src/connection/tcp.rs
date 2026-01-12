//! TCP connection handling with Kafka protocol framing.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::SinkExt;
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use kafka_protocol::messages::{RequestHeader, ResponseHeader};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tokio_util::codec::{Decoder, Encoder, Framed};

use crate::error::{ConnectionError, Error, ProtocolError};

/// Kafka protocol framing codec.
///
/// Kafka uses a simple length-prefixed framing where each message is preceded
/// by a 4-byte big-endian length field.
#[derive(Debug, Clone, Copy, Default)]
pub struct KafkaCodec;

impl Decoder for KafkaCodec {
    type Item = BytesMut;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        // Read length prefix (4 bytes, big-endian)
        let length = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        // Check if we have the full message
        if src.len() < 4 + length {
            // Reserve space for the rest of the message
            src.reserve(4 + length - src.len());
            return Ok(None);
        }

        // Remove the length prefix
        src.advance(4);

        // Extract the message
        Ok(Some(src.split_to(length)))
    }
}

impl Encoder<Bytes> for KafkaCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Write length prefix
        dst.reserve(4 + item.len());
        dst.put_u32(item.len() as u32);
        dst.extend_from_slice(&item);
        Ok(())
    }
}

/// Pending request waiting for a response.
struct PendingRequest {
    response_tx: oneshot::Sender<Result<BytesMut, Error>>,
}

/// Internal message for the connection task.
enum ConnectionMessage {
    Request {
        data: Bytes,
        correlation_id: i32,
        response_tx: oneshot::Sender<Result<BytesMut, Error>>,
    },
    Shutdown,
}

/// A connection to a Kafka broker.
///
/// This handles the low-level protocol framing and correlation ID tracking.
pub struct BrokerConnection {
    /// Sender to the connection task.
    tx: mpsc::Sender<ConnectionMessage>,
    /// Next correlation ID.
    correlation_id: AtomicI32,
    /// Client ID for request headers.
    client_id: Option<StrBytes>,
    /// Whether the connection is still alive.
    alive: Arc<std::sync::atomic::AtomicBool>,
}

impl BrokerConnection {
    /// Connect to a broker at the given address.
    pub async fn connect(addr: SocketAddr, client_id: Option<String>) -> Result<Self, Error> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(ConnectionError::Io)?;

        Self::from_stream(stream, client_id)
    }

    /// Create a connection from an existing stream.
    pub fn from_stream<S>(stream: S, client_id: Option<String>) -> Result<Self, Error>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let framed = Framed::new(stream, KafkaCodec);
        let (tx, rx) = mpsc::channel(64);
        let alive = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let alive_clone = alive.clone();

        // Spawn the connection task
        tokio::spawn(async move {
            connection_task(framed, rx, alive_clone).await;
        });

        Ok(Self {
            tx,
            correlation_id: AtomicI32::new(0),
            client_id: client_id.map(|s| StrBytes::from_string(s)),
            alive,
        })
    }

    /// Check if the connection is still alive.
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    /// Get the next correlation ID.
    fn next_correlation_id(&self) -> i32 {
        self.correlation_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Send a request and wait for the response.
    pub async fn send_request<R: Request>(
        &self,
        request: R,
        timeout_duration: Duration,
    ) -> Result<R::Response, Error> {
        let api_key = R::KEY;
        // Use the highest supported version from VERSIONS range
        let api_version = R::VERSIONS.max;
        let correlation_id = self.next_correlation_id();

        // Build request header
        let header_version = R::header_version(api_version);
        let mut header = RequestHeader::default();
        header.request_api_key = api_key as i16;
        header.request_api_version = api_version;
        header.correlation_id = correlation_id;
        header.client_id = self.client_id.clone();

        // Encode the request
        let mut buf = BytesMut::new();
        header
            .encode(&mut buf, header_version)
            .map_err(|e| ProtocolError::Encode(e.to_string()))?;
        request
            .encode(&mut buf, api_version)
            .map_err(|e| ProtocolError::Encode(e.to_string()))?;

        // Create response channel
        let (response_tx, response_rx) = oneshot::channel();

        // Send the request
        self.tx
            .send(ConnectionMessage::Request {
                data: buf.freeze(),
                correlation_id,
                response_tx,
            })
            .await
            .map_err(|_| ConnectionError::Closed)?;

        // Wait for response with timeout
        let response_data = timeout(timeout_duration, response_rx)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| ConnectionError::Closed)??;

        // Decode response header
        let response_header_version = R::Response::header_version(api_version);
        let mut cursor = response_data.as_ref();
        let response_header = ResponseHeader::decode(&mut cursor, response_header_version)
            .map_err(|e| ProtocolError::Decode(e.to_string()))?;

        // Verify correlation ID
        if response_header.correlation_id != correlation_id {
            return Err(ProtocolError::CorrelationMismatch {
                expected: correlation_id,
                actual: response_header.correlation_id,
            }
            .into());
        }

        // Decode response body
        let response = R::Response::decode(&mut cursor, api_version)
            .map_err(|e| ProtocolError::Decode(e.to_string()))?;

        Ok(response)
    }

    /// Close the connection.
    pub async fn close(&self) {
        let _ = self.tx.send(ConnectionMessage::Shutdown).await;
    }
}

/// Background task that handles sending and receiving messages.
async fn connection_task<S>(
    mut framed: Framed<S, KafkaCodec>,
    mut rx: mpsc::Receiver<ConnectionMessage>,
    alive: Arc<std::sync::atomic::AtomicBool>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use futures::StreamExt;

    let mut pending: HashMap<i32, PendingRequest> = HashMap::new();

    loop {
        tokio::select! {
            // Handle incoming messages from the channel
            msg = rx.recv() => {
                match msg {
                    Some(ConnectionMessage::Request { data, correlation_id, response_tx }) => {
                        // Send the request
                        if let Err(e) = framed.send(data).await {
                            let _ = response_tx.send(Err(ConnectionError::Io(e).into()));
                            continue;
                        }

                        // Store the pending request
                        pending.insert(correlation_id, PendingRequest { response_tx });
                    }
                    Some(ConnectionMessage::Shutdown) | None => {
                        break;
                    }
                }
            }

            // Handle incoming responses from the socket
            response = framed.next() => {
                match response {
                    Some(Ok(data)) => {
                        // Parse correlation ID from response header
                        if data.len() >= 4 {
                            let correlation_id = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);

                            if let Some(pending_req) = pending.remove(&correlation_id) {
                                let _ = pending_req.response_tx.send(Ok(data));
                            } else {
                                tracing::warn!(correlation_id, "Received response for unknown correlation ID");
                            }
                        }
                    }
                    Some(Err(e)) => {
                        tracing::error!(error = %e, "Connection read error");
                        // Fail all pending requests
                        for (_, pending_req) in pending.drain() {
                            let _ = pending_req.response_tx.send(Err(ConnectionError::Io(e.kind().into()).into()));
                        }
                        break;
                    }
                    None => {
                        tracing::debug!("Connection closed by peer");
                        break;
                    }
                }
            }
        }
    }

    // Mark connection as dead
    alive.store(false, Ordering::SeqCst);

    // Fail any remaining pending requests
    for (_, pending_req) in pending {
        let _ = pending_req.response_tx.send(Err(ConnectionError::Closed.into()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_codec_decode() {
        let mut codec = KafkaCodec;
        let mut buf = BytesMut::new();

        // Write a message with length prefix
        buf.put_u32(5); // length
        buf.extend_from_slice(b"hello");

        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(result, Some(BytesMut::from(&b"hello"[..])));
    }

    #[test]
    fn test_kafka_codec_decode_partial() {
        let mut codec = KafkaCodec;
        let mut buf = BytesMut::new();

        // Write incomplete message
        buf.put_u32(10); // length
        buf.extend_from_slice(b"hello"); // only 5 bytes

        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_kafka_codec_encode() {
        let mut codec = KafkaCodec;
        let mut buf = BytesMut::new();

        codec.encode(Bytes::from("hello"), &mut buf).unwrap();

        assert_eq!(&buf[0..4], &[0, 0, 0, 5]); // length prefix
        assert_eq!(&buf[4..], b"hello");
    }
}
