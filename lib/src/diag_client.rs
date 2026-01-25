//! DIAG client for connecting to orbicd's multiplexer
//!
//! This module provides a client that connects to orbicd's Unix socket
//! and receives DIAG data. It can be used as an alternative to direct
//! /dev/diag access when orbicd is managing the device.

use std::path::Path;
use std::time::Duration;
use futures::TryStream;
use log::{info, warn};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::UnixStream;
use tokio::time::timeout;

use crate::diag::MessagesContainer;
use deku::prelude::*;

/// Default socket path for orbicd DIAG server
pub const DEFAULT_SOCKET_PATH: &str = "/var/run/orbicd/diag.sock";

/// Protocol constants (must match orbicd)
const PROTOCOL_MAGIC: u8 = 0xD1;
const PROTOCOL_VERSION: u8 = 0x01;
const MSG_TYPE_SUBSCRIBE: u8 = 0x01;
const MSG_TYPE_PING: u8 = 0x04;
const MSG_TYPE_CONTAINER: u8 = 0x10;
const MSG_TYPE_ACK: u8 = 0x12;
const MSG_TYPE_ERROR: u8 = 0x13;

/// Maximum payload size
const MAX_PAYLOAD_SIZE: u32 = 256 * 1024;

/// DIAG client errors
#[derive(Error, Debug)]
pub enum DiagClientError {
    #[error("Failed to connect to socket: {0}")]
    ConnectionFailed(std::io::Error),
    #[error("Socket not found at {0}")]
    SocketNotFound(String),
    #[error("Protocol error: {0}")]
    ProtocolError(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Server error: code={0}, message={1}")]
    ServerError(u32, String),
    #[error("Connection closed")]
    ConnectionClosed,
    #[error("Timeout waiting for response")]
    Timeout,
    #[error("Failed to parse container: {0}")]
    ParseError(deku::DekuError),
}

/// DIAG client that connects to orbicd
pub struct DiagClient {
    reader: BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: BufWriter<tokio::net::unix::OwnedWriteHalf>,
    subscribed: bool,
}

impl DiagClient {
    /// Check if the orbicd socket exists
    pub fn socket_exists(path: &str) -> bool {
        Path::new(path).exists()
    }

    /// Check if the default socket exists
    pub fn default_socket_exists() -> bool {
        Self::socket_exists(DEFAULT_SOCKET_PATH)
    }

    /// Connect to orbicd DIAG server at the default path
    pub async fn connect() -> Result<Self, DiagClientError> {
        Self::connect_to(DEFAULT_SOCKET_PATH).await
    }

    /// Connect to orbicd DIAG server at a specific path
    pub async fn connect_to(path: &str) -> Result<Self, DiagClientError> {
        if !Path::new(path).exists() {
            return Err(DiagClientError::SocketNotFound(path.to_string()));
        }

        info!("Connecting to orbicd DIAG socket at {}", path);

        let stream = UnixStream::connect(path).await
            .map_err(DiagClientError::ConnectionFailed)?;

        let (read_half, write_half) = stream.into_split();
        let reader = BufReader::new(read_half);
        let writer = BufWriter::new(write_half);

        Ok(Self {
            reader,
            writer,
            subscribed: false,
        })
    }

    /// Subscribe to DIAG data
    pub async fn subscribe(&mut self, client_name: &str) -> Result<(), DiagClientError> {
        info!("Subscribing to DIAG data as '{}'", client_name);

        // Build subscribe message
        let name_bytes = client_name.as_bytes();
        let name_len = name_bytes.len().min(255) as u8;

        let mut payload = Vec::with_capacity(2 + name_len as usize);
        payload.push(name_len);
        payload.extend_from_slice(&name_bytes[..name_len as usize]);
        payload.push(0); // No log code filters

        // Write header + payload
        self.write_message(MSG_TYPE_SUBSCRIBE, &payload).await?;

        // Wait for ack
        let (msg_type, _) = self.read_message().await?;
        match msg_type {
            MSG_TYPE_ACK => {
                self.subscribed = true;
                info!("Successfully subscribed to DIAG data");
                Ok(())
            }
            MSG_TYPE_ERROR => {
                Err(DiagClientError::ProtocolError("Subscribe rejected".into()))
            }
            _ => {
                Err(DiagClientError::ProtocolError(format!(
                    "Unexpected response type: 0x{:02x}", msg_type
                )))
            }
        }
    }

    /// Read the next container from the server
    pub async fn read_container(&mut self) -> Result<MessagesContainer, DiagClientError> {
        if !self.subscribed {
            return Err(DiagClientError::ProtocolError("Not subscribed".into()));
        }

        loop {
            let (msg_type, payload) = self.read_message().await?;

            match msg_type {
                MSG_TYPE_CONTAINER => {
                    // Container format: seq(8) + timestamp_us(8) + raw_data
                    if payload.len() < 16 {
                        return Err(DiagClientError::ProtocolError("Container too short".into()));
                    }

                    let raw_data = &payload[16..];

                    // Parse as MessagesContainer
                    let container = MessagesContainer::from_bytes((raw_data, 0))
                        .map_err(|e| DiagClientError::ParseError(e))?
                        .1;

                    return Ok(container);
                }
                MSG_TYPE_ERROR => {
                    if payload.len() >= 6 {
                        let code = u32::from_le_bytes([
                            payload[0], payload[1], payload[2], payload[3]
                        ]);
                        let msg_len = u16::from_le_bytes([payload[4], payload[5]]) as usize;
                        let msg = String::from_utf8_lossy(
                            &payload[6..6 + msg_len.min(payload.len() - 6)]
                        ).into_owned();
                        return Err(DiagClientError::ServerError(code, msg));
                    }
                    return Err(DiagClientError::ServerError(0, "Unknown error".into()));
                }
                MSG_TYPE_ACK => {
                    // Ignore ACKs when waiting for containers
                    continue;
                }
                _ => {
                    warn!("Unexpected message type: 0x{:02x}", msg_type);
                    continue;
                }
            }
        }
    }

    /// Get a stream of containers
    pub fn as_stream(
        &mut self,
    ) -> impl TryStream<Ok = MessagesContainer, Error = DiagClientError> + '_ {
        futures::stream::try_unfold(self, |client| async {
            let container = client.read_container().await?;
            Ok(Some((container, client)))
        })
    }

    /// Send a ping to check connection
    pub async fn ping(&mut self) -> Result<(), DiagClientError> {
        self.write_message(MSG_TYPE_PING, &[]).await?;

        let result = timeout(Duration::from_secs(5), self.read_message()).await;
        match result {
            Ok(Ok((MSG_TYPE_ACK, _))) => Ok(()),
            Ok(Ok((msg_type, _))) => {
                Err(DiagClientError::ProtocolError(format!(
                    "Expected ACK, got 0x{:02x}", msg_type
                )))
            }
            Ok(Err(e)) => Err(e),
            Err(_) => Err(DiagClientError::Timeout),
        }
    }

    /// Write a message to the server
    async fn write_message(&mut self, msg_type: u8, payload: &[u8]) -> Result<(), DiagClientError> {
        let mut header = [0u8; 8];
        header[0] = PROTOCOL_MAGIC;
        header[1] = PROTOCOL_VERSION;
        header[2] = msg_type;
        header[3] = 0; // flags
        header[4..8].copy_from_slice(&(payload.len() as u32).to_le_bytes());

        self.writer.write_all(&header).await?;
        if !payload.is_empty() {
            self.writer.write_all(payload).await?;
        }
        self.writer.flush().await?;

        Ok(())
    }

    /// Read a message from the server
    async fn read_message(&mut self) -> Result<(u8, Vec<u8>), DiagClientError> {
        // Read header
        let mut header = [0u8; 8];
        self.reader.read_exact(&mut header).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                DiagClientError::ConnectionClosed
            } else {
                DiagClientError::IoError(e)
            }
        })?;

        // Validate header
        if header[0] != PROTOCOL_MAGIC {
            return Err(DiagClientError::ProtocolError(format!(
                "Invalid magic: 0x{:02x}", header[0]
            )));
        }
        if header[1] != PROTOCOL_VERSION {
            return Err(DiagClientError::ProtocolError(format!(
                "Unsupported version: {}", header[1]
            )));
        }

        let msg_type = header[2];
        let length = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);

        if length > MAX_PAYLOAD_SIZE {
            return Err(DiagClientError::ProtocolError(format!(
                "Payload too large: {}", length
            )));
        }

        // Read payload
        let mut payload = vec![0u8; length as usize];
        if length > 0 {
            self.reader.read_exact(&mut payload).await.map_err(|e| {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    DiagClientError::ConnectionClosed
                } else {
                    DiagClientError::IoError(e)
                }
            })?;
        }

        Ok((msg_type, payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_socket_path() {
        assert!(DEFAULT_SOCKET_PATH.starts_with("/var/run/"));
    }

    #[test]
    fn test_protocol_constants() {
        // Ensure constants match orbicd
        assert_eq!(PROTOCOL_MAGIC, 0xD1);
        assert_eq!(PROTOCOL_VERSION, 0x01);
    }
}
