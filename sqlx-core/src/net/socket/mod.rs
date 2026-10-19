use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

pub use buffered::{BufferedSocket, WriteBuffer};
use bytes::BufMut;
use cfg_if::cfg_if;

use crate::io::ReadBuf;

mod buffered;

pub trait Socket: Send + Sync + Unpin + 'static {
    fn try_read(&mut self, buf: &mut dyn ReadBuf) -> io::Result<usize>;

    fn try_write(&mut self, buf: &[u8]) -> io::Result<usize>;

    fn poll_read_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // `flush()` is a no-op for TCP/UDS
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    /// When this socket last moved a byte in EITHER direction, `None` before
    /// the first one.
    ///
    /// REQUIRED on purpose. [`BufferedSocket`] anchors its inactivity deadline
    /// to this instant (`last + bound`) and exempts only a wait that has seen
    /// no byte of the current message; a socket that cannot answer this cannot
    /// tell a fragmented message in flight from a stalled peer. Every
    /// transport in the tree — the raw TCP/UDS sockets (which record on
    /// `try_read`/`try_write`) and the TLS sessions above them — implements
    /// it, and a new transport has to make that decision instead of
    /// inheriting a silent default.
    fn last_transport_progress(&self) -> Option<std::time::Instant>;

    /// When this socket last RECEIVED a byte, `None` before the first one.
    ///
    /// This is the ARMING signal: only a received byte can establish that an
    /// incoming message has started arriving. A write moves
    /// [`Self::last_transport_progress`] — and so may extend a wait's
    /// deadline — but says nothing about the peer, so it must not arm one.
    ///
    /// REQUIRED on purpose, like [`Self::last_transport_progress`].
    fn last_receive_progress(&self) -> Option<std::time::Instant>;

    /// Whether this transport is holding a PREFIX OF AN INCOMPLETE incoming
    /// record that it has already consumed from the wire.
    ///
    /// A TLS session buffers ciphertext, so a read can complete with the
    /// SQLx plaintext buffer empty while the first half of the NEXT record
    /// already sits in the session. That is an incoming message in flight and
    /// must arm the inactivity bound; without this predicate the entry
    /// window — bytes pulled by the previous read — is invisible. Plaintext
    /// transports do not frame records and answer `false`.
    ///
    /// The predicate is deliberately CONSERVATIVE: an unfinished record can be
    /// shown unfinished, but not shown to carry application data (TLS 1.3
    /// ciphertext may decode to control traffic), so it arms the bound rather
    /// than claiming a message exists.
    ///
    /// REQUIRED on purpose: a new transport has to make that decision instead
    /// of inheriting a silent "nothing buffered".
    fn has_incomplete_incoming_record(&self) -> bool;

    fn read<'a, B: ReadBuf>(&'a mut self, buf: &'a mut B) -> Read<'a, Self, B>
    where
        Self: Sized,
    {
        Read { socket: self, buf }
    }

    fn write<'a>(&'a mut self, buf: &'a [u8]) -> Write<'a, Self>
    where
        Self: Sized,
    {
        Write { socket: self, buf }
    }

    fn flush(&mut self) -> Flush<'_, Self>
    where
        Self: Sized,
    {
        Flush { socket: self }
    }

    fn shutdown(&mut self) -> Shutdown<'_, Self>
    where
        Self: Sized,
    {
        Shutdown { socket: self }
    }
}

pub struct Read<'a, S: ?Sized, B> {
    socket: &'a mut S,
    buf: &'a mut B,
}

impl<S: ?Sized, B> Future for Read<'_, S, B>
where
    S: Socket,
    B: ReadBuf,
{
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        while this.buf.has_remaining_mut() {
            match this.socket.try_read(&mut *this.buf) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    ready!(this.socket.poll_read_ready(cx))?;
                }
                ready => return Poll::Ready(ready),
            }
        }

        Poll::Ready(Ok(0))
    }
}

pub struct Write<'a, S: ?Sized> {
    socket: &'a mut S,
    buf: &'a [u8],
}

impl<S: ?Sized> Future for Write<'_, S>
where
    S: Socket,
{
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        while !this.buf.is_empty() {
            match this.socket.try_write(this.buf) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    ready!(this.socket.poll_write_ready(cx))?;
                }
                ready => return Poll::Ready(ready),
            }
        }

        Poll::Ready(Ok(0))
    }
}

pub struct Flush<'a, S: ?Sized> {
    socket: &'a mut S,
}

impl<S: Socket + ?Sized> Future for Flush<'_, S> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.socket.poll_flush(cx)
    }
}

pub struct Shutdown<'a, S: ?Sized> {
    socket: &'a mut S,
}

impl<S: ?Sized> Future for Shutdown<'_, S>
where
    S: Socket,
{
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.socket.poll_shutdown(cx)
    }
}

pub trait WithSocket {
    type Output;

    fn with_socket<S: Socket>(self, socket: S) -> impl Future<Output = Self::Output> + Send;
}

pub struct SocketIntoBox;

impl WithSocket for SocketIntoBox {
    type Output = Box<dyn Socket>;

    async fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        Box::new(socket)
    }
}

impl<S: Socket + ?Sized> Socket for Box<S> {
    fn try_read(&mut self, buf: &mut dyn ReadBuf) -> io::Result<usize> {
        (**self).try_read(buf)
    }

    fn try_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (**self).try_write(buf)
    }

    fn poll_read_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        (**self).poll_read_ready(cx)
    }

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        (**self).poll_write_ready(cx)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        (**self).poll_flush(cx)
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        (**self).poll_shutdown(cx)
    }

    fn last_transport_progress(&self) -> Option<std::time::Instant> {
        (**self).last_transport_progress()
    }

    fn last_receive_progress(&self) -> Option<std::time::Instant> {
        (**self).last_receive_progress()
    }

    fn has_incomplete_incoming_record(&self) -> bool {
        (**self).has_incomplete_incoming_record()
    }
}

pub async fn connect_tcp<Ws: WithSocket>(
    host: &str,
    port: u16,
    with_socket: Ws,
) -> crate::Result<Ws::Output> {
    #[cfg(feature = "_rt-tokio")]
    if crate::rt::rt_tokio::available() {
        use crate::rt::rt_tokio::socket::TrackedTcpStream;

        return Ok(with_socket
            .with_socket(TrackedTcpStream::new(
                tokio::net::TcpStream::connect((host, port)).await?,
            ))
            .await);
    }

    cfg_if! {
        if #[cfg(feature = "_rt-async-io")] {
            use crate::rt::rt_async_io::socket::TrackedTcpStream;

            Ok(with_socket
                .with_socket(TrackedTcpStream::new(connect_tcp_async_io(host, port).await?))
                .await)
        } else {
            crate::rt::missing_rt((host, port, with_socket))
        }
    }
}

/// Open a TCP socket to `host` and `port`.
///
/// If `host` is a hostname, attempt to connect to each address it resolves to.
///
/// This implements the same behavior as [`tokio::net::TcpStream::connect()`].
///
/// The return type is CONCRETE on purpose: the caller wraps it in
/// [`crate::rt::rt_async_io::socket::TrackedTcpStream`], which is what
/// implements [`Socket`] and records transport progress. Returning
/// `impl Socket` here compiled only against upstream's blanket impl for
/// `async_io::Async<TcpStream>`; in the fork that impl is the tracked wrapper,
/// so an opaque `impl Socket` is neither constructible from the raw stream nor
/// acceptable to `TrackedTcpStream::new`. Naming the raw type keeps the
/// wrapping — and therefore the progress semantics — identical on both
/// runtime arms.
#[cfg(feature = "_rt-async-io")]
async fn connect_tcp_async_io(
    host: &str,
    port: u16,
) -> crate::Result<async_io::Async<std::net::TcpStream>> {
    use async_io::Async;
    use std::net::{IpAddr, TcpStream, ToSocketAddrs};

    // IPv6 addresses in URLs will be wrapped in brackets and the `url` crate doesn't trim those.
    let host = host.trim_matches(&['[', ']'][..]);

    if let Ok(addr) = host.parse::<IpAddr>() {
        return Ok(Async::<TcpStream>::connect((addr, port)).await?);
    }

    let host = host.to_string();

    let addresses = crate::rt::spawn_blocking(move || {
        let addr = (host.as_str(), port);
        ToSocketAddrs::to_socket_addrs(&addr)
    })
    .await?;

    let mut last_err = None;

    // Loop through all the Socket Addresses that the hostname resolves to
    for socket_addr in addresses {
        match Async::<TcpStream>::connect(socket_addr).await {
            Ok(stream) => return Ok(stream),
            Err(e) => last_err = Some(e),
        }
    }

    // If we reach this point, it means we failed to connect to any of the addresses.
    // Return the last error we encountered, or a custom error if the hostname didn't resolve to any address.
    Err(last_err
        .unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "Hostname did not resolve to any addresses",
            )
        })
        .into())
}

/// Connect a Unix Domain Socket at the given path.
///
/// Returns an error if Unix Domain Sockets are not supported on this platform.
pub async fn connect_uds<P: AsRef<Path>, Ws: WithSocket>(
    path: P,
    with_socket: Ws,
) -> crate::Result<Ws::Output> {
    #[cfg(unix)]
    {
        #[cfg(feature = "_rt-tokio")]
        if crate::rt::rt_tokio::available() {
            use crate::rt::rt_tokio::socket::TrackedUnixStream;
            use tokio::net::UnixStream;

            let stream = TrackedUnixStream::new(UnixStream::connect(path).await?);

            return Ok(with_socket.with_socket(stream).await);
        }

        cfg_if! {
            if #[cfg(feature = "_rt-async-io")] {
                use crate::rt::rt_async_io::socket::TrackedUnixStream;
                use async_io::Async;
                use std::os::unix::net::UnixStream;

                let stream = TrackedUnixStream::new(Async::<UnixStream>::connect(path).await?);

                Ok(with_socket.with_socket(stream).await)
            } else {
                crate::rt::missing_rt((path, with_socket))
            }
        }
    }

    #[cfg(not(unix))]
    {
        drop((path, with_socket));

        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Unix domain sockets are not supported on this platform",
        )
        .into())
    }
}
