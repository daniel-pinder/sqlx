use crate::net::Socket;

use std::future;
use std::io::{self, Read, Write};
use std::task::{Context, Poll};

pub struct StdSocket<S> {
    pub socket: S,
    wants_read: bool,
    wants_write: bool,
    /// When ciphertext was last read from or written to `socket`. This is the
    /// transport-progress signal a RECEIVE wait is anchored to: a TLS record
    /// arriving in fragments moves this instant while the plaintext buffer
    /// stays empty.
    last_transport_progress: Option<std::time::Instant>,
    /// When ciphertext was last WRITTEN to `socket`. An OUTGOING wait anchors
    /// to this and nothing else: ciphertext arriving from the peer says
    /// nothing about whether our own write is being drained.
    last_send_progress: Option<std::time::Instant>,
    /// TLS record framing over the ciphertext consumed so far, so a prefix of
    /// a record pulled by a previous read is still visible to the next wait.
    records: RecordFramer,
}

/// Counts TLS record framing in the ciphertext stream: a five-byte header
/// (content type, version, length) followed by `length` body bytes. It never
/// decrypts — it only walks bytes — so it works for every backend above it.
///
/// The state is deliberately NOT cleared when a SQLx message is consumed: the
/// record and the message are different units, and consuming a message says
/// nothing about the next record's framing. Only a record COMPLETING retires
/// its own state.
#[derive(Default)]
struct RecordFramer {
    header: [u8; 5],
    header_len: usize,
    body_remaining: usize,
}

impl RecordFramer {
    fn observe(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            if self.body_remaining > 0 {
                let consumed = std::cmp::min(self.body_remaining, bytes.len());
                self.body_remaining -= consumed;
                bytes = &bytes[consumed..];
                continue;
            }

            self.header[self.header_len] = bytes[0];
            self.header_len += 1;
            bytes = &bytes[1..];

            if self.header_len == self.header.len() {
                self.body_remaining = u16::from_be_bytes([self.header[3], self.header[4]]) as usize;
                self.header_len = 0;
            }
        }
    }

    fn is_incomplete(&self) -> bool {
        self.header_len > 0 || self.body_remaining > 0
    }
}

impl<S: Socket> StdSocket<S> {
    pub fn new(socket: S) -> Self {
        Self {
            socket,
            wants_read: false,
            wants_write: false,
            last_transport_progress: None,
            last_send_progress: None,
            records: RecordFramer::default(),
        }
    }

    /// Returns `Ready` if a previously blocked read _or_ write may now proceed.
    ///
    /// If both a read and a write were attempted, to avoid deadlocks this returns `Ready`
    /// when _either_ direction is ready, not necessarily both.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Return `Ready` without waiting if the caller hasn't tried to do I/O in either direction.
        let mut ready = !(self.wants_read || self.wants_write);

        if self.wants_write && self.socket.poll_write_ready(cx)?.is_ready() {
            self.wants_write = false;
            ready |= true;
        }

        if self.wants_read && self.socket.poll_read_ready(cx)?.is_ready() {
            self.wants_read = false;
            ready |= true;
        }

        if ready {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    /// Returns successfully if a previously blocked read _or_ write may now proceed.
    ///
    /// If both a read and a write were attempted, to avoid deadlocks this returns when _either_
    /// direction is ready, not necessarily both.
    pub async fn ready(&mut self) -> io::Result<()> {
        future::poll_fn(|cx| self.poll_ready(cx)).await
    }

    /// When ciphertext was last moved in either direction, if ever.
    pub fn last_transport_progress(&self) -> Option<std::time::Instant> {
        self.last_transport_progress
    }

    /// When ciphertext was last moved OUTBOUND, if ever.
    pub fn last_send_progress(&self) -> Option<std::time::Instant> {
        self.last_send_progress
    }

    /// Whether the ciphertext consumed so far ends inside a TLS record.
    pub fn has_incomplete_incoming_record(&self) -> bool {
        self.records.is_incomplete()
    }
}

impl<S: Socket> Read for StdSocket<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.wants_read = true;
        // `BufMut` semantics advance the slice it is handed, so pass a
        // REBORROW: the bytes are written through it into `buf`, but `buf`
        // itself still starts at the first byte read.
        let read = self.socket.try_read(&mut &mut *buf)?;
        self.wants_read = false;
        if read > 0 {
            self.last_transport_progress = Some(std::time::Instant::now());
            self.records.observe(&buf[..read]);
        }

        Ok(read)
    }
}

impl<S: Socket> Write for StdSocket<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.wants_write = true;
        let written = self.socket.try_write(buf)?;
        self.wants_write = false;
        if written > 0 {
            let now = std::time::Instant::now();
            self.last_transport_progress = Some(now);
            self.last_send_progress = Some(now);
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        // NOTE: TCP sockets and unix sockets are both no-ops for flushes
        Ok(())
    }
}
