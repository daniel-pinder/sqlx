use crate::error::{Error, SocketTimedOut};
use crate::net::Socket;
use bytes::BytesMut;
use std::future::Future;
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::{cmp, io};

use crate::io::{AsyncRead, AsyncReadExt, ProtocolDecode, ProtocolEncode};

// Tokio, async-std, and std all use this as the default capacity for their buffered I/O.
const DEFAULT_BUF_SIZE: usize = 8192;

static NEXT_SOCKET_ID: AtomicUsize = AtomicUsize::new(1);

pub struct BufferedSocket<S> {
    socket: S,
    write_buf: WriteBuffer,
    read_buf: ReadBuffer,
    io_timeout: Option<Duration>,
    poisoned: Option<Duration>,
    socket_id: usize,
    /// Instant of the transport progress observed at the last completed read;
    /// the idle read path arms its bound as soon as this is stale (see
    /// `ReadBuffer::read`).
    seen_transport_progress: Option<std::time::Instant>,
}

pub struct WriteBuffer {
    buf: Vec<u8>,
    bytes_written: usize,
    bytes_flushed: usize,
}

pub struct ReadBuffer {
    read: BytesMut,
    available: BytesMut,
}

impl<S: Socket> BufferedSocket<S> {
    pub fn new(socket: S) -> Self
    where
        S: Sized,
    {
        BufferedSocket {
            socket,
            io_timeout: None,
            poisoned: None,
            socket_id: NEXT_SOCKET_ID.fetch_add(1, Ordering::Relaxed),
            seen_transport_progress: None,
            write_buf: WriteBuffer {
                buf: Vec::with_capacity(DEFAULT_BUF_SIZE),
                bytes_written: 0,
                bytes_flushed: 0,
            },
            read_buf: ReadBuffer {
                read: BytesMut::new(),
                available: BytesMut::with_capacity(DEFAULT_BUF_SIZE),
            },
        }
    }

    /// Set the inactivity timeout for each socket read, write, flush and shutdown wait.
    ///
    /// `None` (the default) leaves I/O unbounded. A timed-out socket is permanently
    /// poisoned; changing this setting cannot make it usable again.
    pub fn set_io_timeout(&mut self, timeout: Option<Duration>) {
        self.io_timeout = timeout;
    }

    fn check_poisoned(&self) -> io::Result<()> {
        if let Some(after) = self.poisoned {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                SocketTimedOut { after },
            ));
        }
        Ok(())
    }

    fn poison_on_timeout(&mut self, error: io::Error) -> io::Error {
        if let Some(after) = socket_timed_out(&error) {
            if self.poisoned.is_none() {
                self.poisoned = Some(after);
                tracing::warn!(target: "sqlx::net::timeout", timeout = ?after,
                    socket_id = self.socket_id, "socket I/O timed out; connection poisoned");
            }
        }
        error
    }

    pub async fn read_buffered(&mut self, len: usize) -> Result<BytesMut, Error> {
        self.try_read(|buf| {
            Ok(if buf.len() < len {
                ControlFlow::Continue(len)
            } else {
                ControlFlow::Break(buf.split_to(len))
            })
        })
        .await
    }

    /// Retryable read operation.
    ///
    /// The callback should check the contents of the buffer passed to it and either:
    ///
    /// * Remove a full message from the buffer and return [`ControlFlow::Break`], or:
    /// * Return [`ControlFlow::Continue`] with the expected _total_ length of the buffer,
    ///   _without_ modifying it.
    ///
    /// Cancel-safe as long as the callback does not modify the passed `BytesMut`
    /// before returning [`ControlFlow::Continue`].
    pub async fn try_read<F, R>(&mut self, try_read: F) -> Result<R, Error>
    where
        F: FnMut(&mut BytesMut) -> Result<ControlFlow<R, usize>, Error>,
    {
        self.try_read_inner(try_read, false).await
    }

    /// Like [`Self::try_read`], but allow an indefinite wait for the first byte
    /// when the read buffer is empty. Once any bytes arrive, I/O is bounded again.
    /// This is intended for unsolicited messages on an otherwise idle connection.
    pub async fn try_read_idle<F, R>(&mut self, try_read: F) -> Result<R, Error>
    where
        F: FnMut(&mut BytesMut) -> Result<ControlFlow<R, usize>, Error>,
    {
        self.try_read_inner(try_read, true).await
    }

    async fn try_read_inner<F, R>(&mut self, mut try_read: F, idle: bool) -> Result<R, Error>
    where
        F: FnMut(&mut BytesMut) -> Result<ControlFlow<R, usize>, Error>,
    {
        self.check_poisoned()?;
        loop {
            let read_len = match try_read(&mut self.read_buf.read)? {
                ControlFlow::Continue(read_len) => read_len,
                ControlFlow::Break(ret) => return Ok(ret),
            };

            if let Err(error) = self
                .read_buf
                .read(
                    read_len,
                    &mut self.socket,
                    self.io_timeout,
                    idle,
                    &mut self.seen_transport_progress,
                )
                .await
            {
                return Err(self.poison_on_timeout(error).into());
            }
        }
    }

    pub fn write_buffer(&self) -> &WriteBuffer {
        &self.write_buf
    }

    pub fn write_buffer_mut(&mut self) -> &mut WriteBuffer {
        &mut self.write_buf
    }

    pub async fn read<'de, T>(&mut self, byte_len: usize) -> Result<T, Error>
    where
        T: ProtocolDecode<'de, ()>,
    {
        self.read_with(byte_len, ()).await
    }

    pub async fn read_with<'de, T, C>(&mut self, byte_len: usize, context: C) -> Result<T, Error>
    where
        T: ProtocolDecode<'de, C>,
    {
        T::decode_with(self.read_buffered(byte_len).await?.freeze(), context)
    }

    #[inline(always)]
    pub fn write<'en, T>(&mut self, value: T) -> Result<(), Error>
    where
        T: ProtocolEncode<'en, ()>,
    {
        self.write_with(value, ())
    }

    #[inline(always)]
    pub fn write_with<'en, T, C>(&mut self, value: T, context: C) -> Result<(), Error>
    where
        T: ProtocolEncode<'en, C>,
    {
        value.encode_with(self.write_buf.buf_mut(), context)?;
        self.write_buf.bytes_written = self.write_buf.buf.len();
        self.write_buf.sanity_check();

        Ok(())
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        self.check_poisoned()?;
        while !self.write_buf.is_empty() {
            let mut anchor = std::time::Instant::now();
            let written = loop {
                let before = self.socket.last_transport_progress();
                let window = self
                    .io_timeout
                    .map(|bound| bound.saturating_sub(anchor.elapsed()));
                match io_timeout_bound(window, self.io_timeout, async {
                    self.socket.write(self.write_buf.get()).await
                })
                .await
                {
                    Ok(written) => break written,
                    Err(error) if socket_timed_out(&error).is_some() => {
                        match self.socket.last_transport_progress() {
                            // The transport moved bytes while the wait had no
                            // plaintext progress to report (a TLS record being
                            // re-assembled or a partly accepted write): the
                            // deadline re-anchors to that byte, exactly as
                            // SO_SNDTIMEO would.
                            Some(last) if Some(last) != before => anchor = last,
                            _ => return Err(self.poison_on_timeout(error)),
                        }
                    }
                    Err(error) => return Err(self.poison_on_timeout(error)),
                }
            };
            self.write_buf.consume(written);
            self.write_buf.sanity_check();
        }

        let mut anchor = std::time::Instant::now();
        loop {
            let before = self.socket.last_transport_progress();
            let window = self
                .io_timeout
                .map(|bound| bound.saturating_sub(anchor.elapsed()));
            match io_timeout_bound(window, self.io_timeout, async { self.socket.flush().await })
                .await
            {
                Ok(()) => return Ok(()),
                Err(error) if socket_timed_out(&error).is_some() => {
                    match self.socket.last_transport_progress() {
                        Some(last) if Some(last) != before => anchor = last,
                        _ => return Err(self.poison_on_timeout(error)),
                    }
                }
                Err(error) => return Err(self.poison_on_timeout(error)),
            }
        }
    }

    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.check_poisoned()?;
        self.flush().await?;
        let mut anchor = std::time::Instant::now();
        loop {
            let before = self.socket.last_transport_progress();
            let window = self
                .io_timeout
                .map(|bound| bound.saturating_sub(anchor.elapsed()));
            match io_timeout_bound(window, self.io_timeout, async {
                self.socket.shutdown().await
            })
            .await
            {
                Ok(()) => return Ok(()),
                Err(error) if socket_timed_out(&error).is_some() => {
                    match self.socket.last_transport_progress() {
                        Some(last) if Some(last) != before => anchor = last,
                        _ => return Err(self.poison_on_timeout(error)),
                    }
                }
                Err(error) => return Err(self.poison_on_timeout(error)),
            }
        }
    }

    pub fn shrink_buffers(&mut self) {
        // Won't drop data still in the buffer.
        self.write_buf.shrink();
        self.read_buf.shrink();
    }

    pub fn into_inner(self) -> S {
        self.socket
    }

    pub fn boxed(self) -> BufferedSocket<Box<dyn Socket>> {
        BufferedSocket {
            socket: Box::new(self.socket),
            write_buf: self.write_buf,
            read_buf: self.read_buf,
            io_timeout: self.io_timeout,
            poisoned: self.poisoned,
            socket_id: self.socket_id,
            seen_transport_progress: self.seen_transport_progress,
        }
    }
}

impl WriteBuffer {
    fn sanity_check(&self) {
        assert_ne!(self.buf.capacity(), 0);
        assert!(self.bytes_written <= self.buf.len());
        assert!(self.bytes_flushed <= self.bytes_written);
    }

    pub fn buf_mut(&mut self) -> &mut Vec<u8> {
        self.buf.truncate(self.bytes_written);
        self.sanity_check();
        &mut self.buf
    }

    pub fn init_remaining_mut(&mut self) -> &mut [u8] {
        self.buf.resize(self.buf.capacity(), 0);
        self.sanity_check();
        &mut self.buf[self.bytes_written..]
    }

    pub fn put_slice(&mut self, slice: &[u8]) {
        // If we already have an initialized area that can fit the slice,
        // don't change `self.buf.len()`
        if let Some(dest) = self.buf[self.bytes_written..].get_mut(..slice.len()) {
            dest.copy_from_slice(slice);
        } else {
            self.buf.truncate(self.bytes_written);
            self.buf.extend_from_slice(slice);
        }
        self.advance(slice.len());
        self.sanity_check();
    }

    pub fn advance(&mut self, amt: usize) {
        let new_bytes_written = self
            .bytes_written
            .checked_add(amt)
            .expect("self.bytes_written + amt overflowed");

        assert!(new_bytes_written <= self.buf.len());

        self.bytes_written = new_bytes_written;

        self.sanity_check();
    }

    /// Read into the buffer from `source`, returning the number of bytes read.
    ///
    /// The buffer is automatically advanced by the number of bytes read.
    pub async fn read_from(&mut self, mut source: impl AsyncRead + Unpin) -> io::Result<usize> {
        let read = match () {
            // Tokio lets us read into the buffer without zeroing first
            #[cfg(feature = "_rt-tokio")]
            _ => source.read_buf(self.buf_mut()).await?,
            #[cfg(not(feature = "_rt-tokio"))]
            _ => source.read(self.init_remaining_mut()).await?,
        };

        if read > 0 {
            self.advance(read);
        }

        Ok(read)
    }

    pub fn is_empty(&self) -> bool {
        self.bytes_flushed >= self.bytes_written
    }

    pub fn is_full(&self) -> bool {
        self.bytes_written == self.buf.len()
    }

    pub fn get(&self) -> &[u8] {
        &self.buf[self.bytes_flushed..self.bytes_written]
    }

    pub fn get_mut(&mut self) -> &mut [u8] {
        &mut self.buf[self.bytes_flushed..self.bytes_written]
    }

    pub fn shrink(&mut self) {
        if self.bytes_flushed > 0 {
            // Move any data that remains to be flushed to the beginning of the buffer,
            // if necessary.
            self.buf
                .copy_within(self.bytes_flushed..self.bytes_written, 0);
            self.bytes_written -= self.bytes_flushed;
            self.bytes_flushed = 0
        }

        // Drop excess capacity.
        self.buf
            .truncate(cmp::max(self.bytes_written, DEFAULT_BUF_SIZE));
        self.buf.shrink_to_fit();
    }

    fn consume(&mut self, amt: usize) {
        let new_bytes_flushed = self
            .bytes_flushed
            .checked_add(amt)
            .expect("self.bytes_flushed + amt overflowed");

        assert!(new_bytes_flushed <= self.bytes_written);

        self.bytes_flushed = new_bytes_flushed;

        if self.bytes_flushed == self.bytes_written {
            // Reset cursors to zero if we've consumed the whole buffer
            self.bytes_flushed = 0;
            self.bytes_written = 0;
        }

        self.sanity_check();
    }
}

impl ReadBuffer {
    async fn read(
        &mut self,
        len: usize,
        socket: &mut impl Socket,
        timeout: Option<Duration>,
        idle: bool,
        seen_transport_progress: &mut Option<std::time::Instant>,
    ) -> io::Result<()> {
        // `try_read_idle` exempts a wait that has seen NO progress at all an
        // idle connection may wait indefinitely for its first byte. Partial
        // PLAINTEXT (a message header already in the buffer) and partial
        // TRANSPORT data (a TLS record the session has consumed but not
        // assembled) both arm the bound, so neither a half-delivered message
        // nor a half-delivered record can stall the wait forever.
        let mut armed =
            !self.read.is_empty() || socket.last_transport_progress() != *seen_transport_progress;
        // The deadline is anchored to the LAST transport byte (`last + bound`),
        // never to the start of a fresh window: a byte that arrives just
        // before a window expires buys one bound from ITS instant, so silence
        // cannot stretch to twice the bound.
        let mut anchor = std::time::Instant::now();
        // Because of how `BytesMut` works, we should only be shifting capacity back and forth
        // between `read` and `available` unless we have to read an oversize message.
        while self.read.len() < len {
            self.reserve(len - self.read.len());

            let before = socket.last_transport_progress();
            let window = timeout.map(|bound| bound.saturating_sub(anchor.elapsed()));
            let read = match io_timeout_bound(window, timeout, async {
                socket.read(&mut self.available).await
            })
            .await
            {
                Ok(read) => read,
                Err(error) if socket_timed_out(&error).is_some() => {
                    match socket.last_transport_progress() {
                        Some(last) if Some(last) != before => {
                            // Ciphertext moved while plaintext did not:
                            // the message is still arriving (a fragmented
                            // TLS record, for instance), so the deadline
                            // re-anchors to that byte instead of poisoning.
                            armed = true;
                            anchor = last;
                            continue;
                        }
                        _ if idle && !armed => {
                            // Genuinely idle: no transport activity yet,
                            // so the wait goes on rather than failing.
                            anchor = std::time::Instant::now();
                            continue;
                        }
                        _ => {
                            *seen_transport_progress = socket.last_transport_progress();
                            return Err(error);
                        }
                    }
                }
                Err(error) => {
                    *seen_transport_progress = socket.last_transport_progress();
                    return Err(error);
                }
            };

            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "expected to read {} bytes, got {} bytes at EOF",
                        len,
                        self.read.len()
                    ),
                ));
            }

            // Plaintext progress is progress: the next wait gets a full bound
            // measured from it, exactly as it did before the bound followed
            // the transport.
            armed = true;
            anchor = std::time::Instant::now();
            self.advance(read);
        }

        *seen_transport_progress = socket.last_transport_progress();
        Ok(())
    }

    fn reserve(&mut self, amt: usize) {
        if let Some(additional) = amt.checked_sub(self.available.capacity()) {
            self.available.reserve(additional);
        }
    }

    fn advance(&mut self, amt: usize) {
        self.read.unsplit(self.available.split_to(amt));
    }

    fn shrink(&mut self) {
        if self.available.capacity() > DEFAULT_BUF_SIZE {
            // `BytesMut` doesn't have a way to shrink its capacity,
            // but we only use `available` for spare capacity anyway so we can just replace it.
            //
            // If `self.read` still contains data on the next call to `advance` then this might
            // force a memcpy as they'll no longer be pointing to the same allocation,
            // but that's kind of unavoidable.
            //
            // The `async-std` impl of `Socket` will also need to re-zero the buffer,
            // but that's also kind of unavoidable.
            //
            // We should be warning the user not to call this often.
            self.available = BytesMut::with_capacity(DEFAULT_BUF_SIZE);
        }
    }
}

/// The configured bound carried by a timed-out wait, or `None` for any other
/// error. The payload is how the bound survives the TLS layer: it is built
/// below it and inspected here.
fn socket_timed_out(error: &io::Error) -> Option<Duration> {
    error
        .get_ref()
        .and_then(|error| error.downcast_ref::<SocketTimedOut>())
        .map(|timeout| timeout.after)
}

// Apply the bound to one wait, not a whole message: successful partial I/O resets it.
// `window` may be shorter than `after` when the deadline is anchored to the
// last transport byte; the error still reports the CONFIGURED bound, which is
// what the connection is poisoned for and what the log names.
// Callers keep raw Socket futures inside async blocks to preserve Send when
// this is nested in driver futures with a boxed socket.
/// Apply the bound to one wait. `window` is the wait actually armed and
/// `bound` is the configured bound the error reports.
async fn io_timeout_bound<T: Send>(
    window: Option<Duration>,
    bound: Option<Duration>,
    io: impl Future<Output = io::Result<T>> + Send,
) -> io::Result<T> {
    match window {
        Some(window) => crate::rt::timeout(window, io).await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                SocketTimedOut {
                    after: bound.unwrap_or(window),
                },
            )
        })?,
        None => io.await,
    }
}

#[cfg(all(test, feature = "_rt-tokio"))]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::task::{Context, Poll};
    use tracing::{span, Event, Metadata, Subscriber};

    #[derive(Clone, Copy)]
    enum Stall {
        Read,
        Write,
        Flush,
        Shutdown,
    }

    struct StalledSocket(Stall);
    impl Socket for StalledSocket {
        fn try_read(&mut self, _: &mut dyn crate::io::ReadBuf) -> io::Result<usize> {
            Err(io::ErrorKind::WouldBlock.into())
        }
        fn try_write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if matches!(self.0, Stall::Write) {
                Err(io::ErrorKind::WouldBlock.into())
            } else {
                Ok(buf.len())
            }
        }
        fn poll_read_ready(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
        fn poll_write_ready(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
        fn poll_flush(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            if matches!(self.0, Stall::Flush) {
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }
        fn poll_shutdown(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            if matches!(self.0, Stall::Shutdown) {
                Poll::Pending
            } else {
                panic!("poisoned shutdown must not touch the socket")
            }
        }
    }

    #[derive(Default)]
    struct Warnings(AtomicUsize);
    impl Subscriber for Warnings {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
            span::Id::from_u64(1)
        }
        fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
        fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
        fn event(&self, event: &Event<'_>) {
            if event.metadata().target() == "sqlx::net::timeout" {
                assert_eq!(event.metadata().level(), &tracing::Level::WARN);
                assert!(event.fields().any(|field| field.name() == "socket_id"));
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        fn enter(&self, _: &span::Id) {}
        fn exit(&self, _: &span::Id) {}
    }

    fn timed_out(error: io::Error) {
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(
            error
                .get_ref()
                .unwrap()
                .downcast_ref::<SocketTimedOut>()
                .unwrap()
                .after,
            Duration::from_millis(10)
        );
    }

    #[test]
    fn socket_io_remains_send_in_driver_futures() {
        fn assert_send(_: impl Send) {}
        let mut socket = BufferedSocket::new(StalledSocket(Stall::Read)).boxed();
        assert_send(async move {
            let _ = socket.read_buffered(1).await;
            let _ = socket.flush().await;
            let _ = socket.shutdown().await;
        });
    }

    #[test]
    fn read_write_and_flush_poison_once_and_shutdown_without_io() {
        let warnings = Arc::new(Warnings::default());
        tracing::subscriber::with_default(warnings.clone(), || {
            crate::rt::test_block_on(async {
                for stall in [Stall::Read, Stall::Write, Stall::Flush, Stall::Shutdown] {
                    let mut socket = BufferedSocket::new(StalledSocket(stall));
                    socket.set_io_timeout(Some(Duration::from_millis(10)));
                    let error = match stall {
                        Stall::Read => match crate::rt::timeout(
                            Duration::from_secs(1),
                            socket.read_buffered(1),
                        )
                        .await
                        .unwrap()
                        .unwrap_err()
                        {
                            Error::Io(error) => error,
                            error => panic!("unexpected error: {error}"),
                        },
                        Stall::Shutdown => {
                            crate::rt::timeout(Duration::from_secs(1), socket.shutdown())
                                .await
                                .expect("driver shutdown bound missing")
                                .unwrap_err()
                        }
                        Stall::Write | Stall::Flush => {
                            socket.write_buffer_mut().put_slice(b"pending");
                            crate::rt::timeout(Duration::from_secs(1), socket.flush())
                                .await
                                .unwrap()
                                .unwrap_err()
                        }
                    };
                    timed_out(error);
                    // Disabling the timeout or boxing cannot revive a desynchronized connection.
                    socket.set_io_timeout(None);
                    socket.set_io_timeout(Some(Duration::from_secs(5)));
                    let mut socket = socket.boxed();
                    timed_out(socket.flush().await.unwrap_err());
                    timed_out(socket.shutdown().await.unwrap_err());
                    assert!(matches!(socket.read_buffered(0).await, Err(Error::Io(_))));
                }
            })
        });
        assert_eq!(warnings.0.load(Ordering::Relaxed), 4);
    }
}
