use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use tokio::io::AsyncWrite;
use tokio::net::TcpStream;

use crate::io::ReadBuf;
use crate::net::Socket;

/// A tokio `TcpStream` that records the instant of the last byte it moved.
///
/// Every transport reports transport progress through [`Socket`]: the last
/// byte moved in EITHER direction (read waits) and the last byte moved
/// OUTBOUND (write, flush and shutdown waits), so the inactivity bound is
/// anchored to real wire activity on EVERY socket — there is no default an
/// implementation can silently omit.
pub struct TrackedTcpStream {
    inner: TcpStream,
    last: Option<Instant>,
    last_send: Option<Instant>,
}

impl TrackedTcpStream {
    pub fn new(inner: TcpStream) -> Self {
        Self {
            inner,
            last: None,
            last_send: None,
        }
    }
}

impl Socket for TrackedTcpStream {
    fn try_read(&mut self, mut buf: &mut dyn ReadBuf) -> io::Result<usize> {
        // Requires `&mut impl BufMut`
        let read = self.inner.try_read_buf(&mut buf)?;
        if read > 0 {
            self.last = Some(Instant::now());
        }
        Ok(read)
    }

    fn try_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.try_write(buf)?;
        if written > 0 {
            let now = Instant::now();
            self.last = Some(now);
            self.last_send = Some(now);
        }
        Ok(written)
    }

    fn poll_read_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_read_ready(cx)
    }

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_write_ready(cx)
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn last_transport_progress(&self) -> Option<Instant> {
        self.last
    }

    fn last_send_progress(&self) -> Option<Instant> {
        self.last_send
    }

    fn has_incomplete_incoming_record(&self) -> bool {
        // A plaintext transport has no record framing to be part-way through.
        false
    }
}

/// A tokio `UnixStream` that records the instant of the last byte it moved;
/// see [`TrackedTcpStream`].
#[cfg(unix)]
pub struct TrackedUnixStream {
    inner: tokio::net::UnixStream,
    last: Option<Instant>,
    last_send: Option<Instant>,
}

#[cfg(unix)]
impl TrackedUnixStream {
    pub fn new(inner: tokio::net::UnixStream) -> Self {
        Self {
            inner,
            last: None,
            last_send: None,
        }
    }
}

#[cfg(unix)]
impl Socket for TrackedUnixStream {
    fn try_read(&mut self, mut buf: &mut dyn ReadBuf) -> io::Result<usize> {
        let read = self.inner.try_read_buf(&mut buf)?;
        if read > 0 {
            self.last = Some(Instant::now());
        }
        Ok(read)
    }

    fn try_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.try_write(buf)?;
        if written > 0 {
            let now = Instant::now();
            self.last = Some(now);
            self.last_send = Some(now);
        }
        Ok(written)
    }

    fn poll_read_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_read_ready(cx)
    }

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_write_ready(cx)
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn last_transport_progress(&self) -> Option<Instant> {
        self.last
    }

    fn last_send_progress(&self) -> Option<Instant> {
        self.last_send
    }

    fn has_incomplete_incoming_record(&self) -> bool {
        // A plaintext transport has no record framing to be part-way through.
        false
    }
}
