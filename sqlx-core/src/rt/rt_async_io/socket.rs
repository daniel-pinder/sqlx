use crate::net::Socket;

use std::io;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::task::{Context, Poll};
use std::time::Instant;

use async_io::Async;

use crate::io::ReadBuf;

/// An async-io `TcpStream` that records the instant of the last byte it moved;
/// see the tokio twin in `rt_tokio::socket`.
pub struct TrackedTcpStream {
    inner: Async<TcpStream>,
    last: Option<Instant>,
    last_send: Option<Instant>,
}

impl TrackedTcpStream {
    pub fn new(inner: Async<TcpStream>) -> Self {
        Self {
            inner,
            last: None,
            last_send: None,
        }
    }
}

impl Socket for TrackedTcpStream {
    fn try_read(&mut self, buf: &mut dyn ReadBuf) -> io::Result<usize> {
        let read = self.inner.get_ref().read(buf.init_mut())?;
        if read > 0 {
            self.last = Some(Instant::now());
        }
        Ok(read)
    }

    fn try_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.get_ref().write(buf)?;
        if written > 0 {
            let now = Instant::now();
            self.last = Some(now);
            self.last_send = Some(now);
        }
        Ok(written)
    }

    fn poll_read_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_readable(cx)
    }

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_writable(cx)
    }

    fn poll_shutdown(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.inner.get_ref().shutdown(Shutdown::Both))
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

/// An async-io `UnixStream` that records the instant of the last byte it
/// moved; see [`TrackedTcpStream`].
#[cfg(unix)]
pub struct TrackedUnixStream {
    inner: Async<std::os::unix::net::UnixStream>,
    last: Option<Instant>,
    last_send: Option<Instant>,
}

#[cfg(unix)]
impl TrackedUnixStream {
    pub fn new(inner: Async<std::os::unix::net::UnixStream>) -> Self {
        Self {
            inner,
            last: None,
            last_send: None,
        }
    }
}

#[cfg(unix)]
impl Socket for TrackedUnixStream {
    fn try_read(&mut self, buf: &mut dyn ReadBuf) -> io::Result<usize> {
        let read = self.inner.get_ref().read(buf.init_mut())?;
        if read > 0 {
            self.last = Some(Instant::now());
        }
        Ok(read)
    }

    fn try_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.get_ref().write(buf)?;
        if written > 0 {
            let now = Instant::now();
            self.last = Some(now);
            self.last_send = Some(now);
        }
        Ok(written)
    }

    fn poll_read_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_readable(cx)
    }

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_writable(cx)
    }

    fn poll_shutdown(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.inner.get_ref().shutdown(Shutdown::Both))
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
