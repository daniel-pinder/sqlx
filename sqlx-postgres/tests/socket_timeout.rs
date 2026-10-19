//! DB-free witnesses for stalled peers. Each outer deadline catches a missing driver bound.
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;
use tracing::{span, Event, Metadata, Subscriber};

use sqlx_core::connection::{ConnectOptions, Connection};
use sqlx_core::executor::Executor;
use sqlx_core::rt::{sleep, test_block_on, timeout};
use sqlx_postgres::{PgConnectOptions, PgSslMode};

const BOUND: Duration = Duration::from_millis(250);
const WATCHDOG: Duration = Duration::from_secs(5);

#[derive(Default)]
struct ConnectWarnings(AtomicUsize);
impl Subscriber for ConnectWarnings {
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
            let fields = event.fields().map(|field| field.name()).collect::<Vec<_>>();
            for field in ["timeout", "phase", "host", "port"] {
                assert!(
                    fields.contains(&field),
                    "missing correlation field: {field}"
                );
            }
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn enter(&self, _: &span::Id) {}
    fn exit(&self, _: &span::Id) {}
}

fn frame(socket: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0; 5];
    socket.read_exact(&mut header).unwrap();
    let len = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
    let mut body = vec![0; len - 4];
    socket.read_exact(&mut body).unwrap();
    (header[0], body)
}

fn message(socket: &mut TcpStream, tag: u8, body: &[u8]) {
    socket.write_all(&[tag]).unwrap();
    socket
        .write_all(&(u32::try_from(body.len()).unwrap() + 4).to_be_bytes())
        .unwrap();
    socket.write_all(body).unwrap();
}

fn startup(socket: &mut TcpStream) {
    socket.set_read_timeout(Some(WATCHDOG)).unwrap();
    socket.set_write_timeout(Some(WATCHDOG)).unwrap();
    let mut len = [0; 4];
    socket.read_exact(&mut len).unwrap();
    let mut body = vec![0; u32::from_be_bytes(len) as usize - 4];
    socket.read_exact(&mut body).unwrap();
    message(socket, b'R', &[0; 4]); // AuthenticationOk
    message(socket, b'Z', b"I"); // ReadyForQuery
}

fn server(
    script: impl FnOnce(TcpStream) + Send + 'static,
) -> (PgConnectOptions, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || script(listener.accept().unwrap().0));
    let options: PgConnectOptions =
        format!("postgres://test@127.0.0.1:{port}/test?sslmode=disable&socket-timeout=0.1")
            .parse()
            .unwrap();
    (options.socket_timeout(BOUND), handle)
}

fn assert_timeout(error: sqlx_core::Error) {
    match error {
        sqlx_core::Error::Io(error) => {
            assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
            assert!(error
                .get_ref()
                .unwrap()
                .is::<sqlx_core::error::SocketTimedOut>());
        }
        error => panic!("expected socket timeout, got {error:?}"),
    }
}

#[test]
fn silent_read_times_out_and_poisoned_ping_sends_nothing() {
    let (options, server) = server(|mut socket| {
        startup(&mut socket);
        assert_eq!(frame(&mut socket).0, b'Q');
        // The connection stays open. A repeated ping must not reach this peer.
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).unwrap(), 0);
    });
    test_block_on(async {
        let mut connection = options.connect().await.unwrap();
        let error = timeout(WATCHDOG, connection.execute("SELECT 1"))
            .await
            .expect("driver read bound missing")
            .unwrap_err();
        assert_timeout(error);
        assert_timeout(
            timeout(Duration::from_millis(50), connection.ping())
                .await
                .expect("poisoned ping must fail immediately")
                .unwrap_err(),
        );
        drop(connection);
    });
    server.join().unwrap();
}

// F2: keep the large-Bind protocol witness; the COPY witness below independently
// fills the actual socket window using bounded memory rather than a payload guess.
#[test]
fn filled_write_window_times_out_during_bind() {
    let (stop, stopped) = std::sync::mpsc::channel();
    let (options, server) = server(move |mut socket| {
        startup(&mut socket);
        assert_eq!(frame(&mut socket).0, b'P');
        assert_eq!(frame(&mut socket).0, b'D');
        assert_eq!(frame(&mut socket).0, b'S');
        message(&mut socket, b'1', b""); // ParseComplete
        message(&mut socket, b't', &[0, 1, 0, 0, 0, 17]); // one BYTEA parameter
        message(&mut socket, b'n', b""); // NoData
        message(&mut socket, b'Z', b"I");
        stopped.recv_timeout(WATCHDOG * 2).unwrap();
    });
    test_block_on(async {
        let mut connection = options.connect().await.unwrap();
        let data = vec![0_u8; 32 * 1024 * 1024];
        let result = timeout(
            WATCHDOG,
            sqlx::query("SELECT $1")
                .bind(&data)
                .execute(&mut connection),
        )
        .await;
        stop.send(()).unwrap();
        assert_timeout(result.expect("driver write bound missing").unwrap_err());
        assert!(
            connection.should_flush(),
            "must time out while bytes remain to write"
        );
    });
    server.join().unwrap();
}

#[test]
fn filled_write_window_times_out_during_copy_send() {
    let (stop, stopped) = std::sync::mpsc::channel();
    let (options, server) = server(move |mut socket| {
        startup(&mut socket);
        assert_eq!(frame(&mut socket).0, b'Q');
        // CopyInResponse, one text column.
        message(&mut socket, b'G', &[0, 0, 1, 0, 0]);
        // Stop reading. Keep sending bounded-size chunks until the actual window fills,
        // independent of the host's send/receive buffer sizes and TCP autotuning.
        stopped.recv_timeout(WATCHDOG * 2).unwrap();
    });
    test_block_on(async {
        let mut connection = options.connect().await.unwrap();
        let mut copy = connection
            .copy_in_raw("COPY test FROM STDIN")
            .await
            .unwrap();
        let data = vec![0_u8; 64 * 1024];
        let result = timeout(WATCHDOG, async {
            loop {
                if let Err(error) = copy.send(data.as_slice()).await {
                    return error;
                }
            }
        })
        .await;
        stop.send(()).unwrap();
        assert_timeout(result.expect("driver write bound missing"));
    });
    server.join().unwrap();
}

fn successful_query(socket: &mut TcpStream, status: &[u8]) {
    loop {
        match frame(socket).0 {
            b'S' => message(socket, b'Z', status),
            b'Q' => {
                message(socket, b'C', b"SELECT 1\0");
                message(socket, b'Z', status);
                return;
            }
            tag => panic!("unexpected frontend message {tag}"),
        }
    }
}

// F3/F4: dropped transactions queue rollback, but a poisoned socket must never flush it.
fn pool_discards_poisoned_connection(in_transaction: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let mut first = listener.accept().unwrap().0;
        startup(&mut first);
        if in_transaction {
            successful_query(&mut first, b"T"); // BEGIN
        }
        assert_eq!(frame(&mut first).0, b'Q');
        assert_eq!(
            first.read(&mut [0]).unwrap(),
            0,
            "poisoned connection sent bytes"
        );
        let mut second = listener.accept().unwrap().0;
        startup(&mut second);
        successful_query(&mut second, if in_transaction { b"T" } else { b"I" });
    });
    test_block_on(async {
        let options = PgConnectOptions::new_without_pgpass()
            .host("127.0.0.1")
            .port(port)
            .username("test")
            .database("test")
            .ssl_mode(PgSslMode::Disable)
            .socket_timeout(BOUND);
        let pool = sqlx_postgres::PgPoolOptions::new()
            .max_connections(1)
            .test_before_acquire(false)
            .idle_timeout(None)
            .max_lifetime(None)
            .acquire_timeout(WATCHDOG)
            .connect_lazy_with(options);
        let mut connection = pool.acquire().await.unwrap();
        if in_transaction {
            let mut transaction = connection.begin().await.unwrap();
            assert_timeout(
                timeout(WATCHDOG, transaction.execute("SELECT 1"))
                    .await
                    .expect("transaction read bound missing")
                    .unwrap_err(),
            );
            drop(transaction);
        } else {
            assert_timeout(
                timeout(WATCHDOG, connection.execute("SELECT 1"))
                    .await
                    .expect("read bound missing")
                    .unwrap_err(),
            );
        }
        drop(connection);
        timeout(Duration::from_secs(1), async {
            while pool.size() != 0 {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("poisoned pool connection was not discarded promptly");
        let mut connection = pool.acquire().await.unwrap();
        if in_transaction {
            let transaction = connection.begin().await.unwrap();
            // The peer has proved a fresh BEGIN works. Avoid requiring another rollback response.
            drop(transaction);
        } else {
            connection.execute("SELECT 1").await.unwrap();
        }
        drop(connection.detach());
        pool.close().await;
    });
    server.join().unwrap();
}

#[test]
fn pool_discards_poisoned_connection_before_release_ping_deadline() {
    pool_discards_poisoned_connection(false);
}

#[test]
fn transaction_drop_on_poisoned_connection_allows_fresh_begin() {
    pool_discards_poisoned_connection(true);
}

#[test]
fn listener_can_idle_but_partial_notification_is_bounded() {
    for partial in [false, true] {
        let (options, server) = server(move |mut socket| {
            startup(&mut socket);
            successful_query(&mut socket, b"I"); // LISTEN
            thread::sleep(BOUND * 6);
            let body = b"\0\0\0\x01channel\0payload\0";
            if partial {
                socket.write_all(b"A\0").unwrap();
                assert_eq!(socket.read(&mut [0]).unwrap(), 0);
            } else {
                message(&mut socket, b'A', body);
                // On listener drop, UNLISTEN runs before returning the connection.
                successful_query(&mut socket, b"I");
            }
        });
        test_block_on(async {
            let pool = sqlx_postgres::PgPoolOptions::new()
                .max_connections(1)
                .test_before_acquire(false)
                .connect_lazy_with(options);
            let mut listener = sqlx_postgres::PgListener::connect_with(&pool)
                .await
                .unwrap();
            listener.eager_reconnect(false);
            listener.listen("channel").await.unwrap();
            let notification = timeout(WATCHDOG, listener.try_recv())
                .await
                .expect("partial message bound missing")
                .unwrap();
            if partial {
                assert!(
                    notification.is_none(),
                    "partial message must lose its poisoned connection"
                );
            } else {
                let notification = notification.expect("idle listener must not reconnect");
                assert_eq!(notification.channel(), "channel");
                assert_eq!(notification.payload(), "payload");
            }
            drop(listener);
            timeout(WATCHDOG, pool.close()).await.unwrap();
        });
        server.join().unwrap();
    }
}

#[test]
fn none_keeps_delayed_reads_unbounded() {
    assert_eq!(
        PgConnectOptions::new_without_pgpass().get_socket_timeout(),
        None
    );
    let (options, server) = server(|mut socket| {
        startup(&mut socket);
        assert_eq!(frame(&mut socket).0, b'Q');
        thread::sleep(Duration::from_secs(1));
        message(&mut socket, b'C', b"SELECT 1\0");
        message(&mut socket, b'Z', b"I");
    });
    test_block_on(async {
        let mut connection = options.socket_timeout(None).connect().await.unwrap();
        timeout(WATCHDOG, connection.execute("SELECT 1"))
            .await
            .unwrap()
            .unwrap();
    });
    server.join().unwrap();
}

#[test]
fn silent_ssl_request_bounds_connect_phase() {
    let (options, server) = server(|mut socket| {
        socket.set_read_timeout(Some(WATCHDOG)).unwrap();
        let mut request = [0; 8];
        socket.read_exact(&mut request).unwrap();
        assert_eq!(request, [0, 0, 0, 8, 4, 210, 22, 47]);
        assert_eq!(socket.read(&mut [0]).unwrap(), 0);
    });
    let warnings = Arc::new(ConnectWarnings::default());
    tracing::subscriber::with_default(warnings.clone(), || {
        test_block_on(async {
            let error = timeout(WATCHDOG, options.ssl_mode(PgSslMode::Require).connect())
                .await
                .expect("connect phase bound missing")
                .unwrap_err();
            assert_timeout(error);
        })
    });
    server.join().unwrap();
    assert_eq!(warnings.0.load(Ordering::Relaxed), 1);
}

#[test]
fn socket_timeout_url_round_trip() {
    let options: PgConnectOptions = "postgres://test@localhost/test?socket-timeout=0.125"
        .parse()
        .unwrap();
    assert_eq!(
        options.get_socket_timeout(),
        Some(Duration::from_millis(125))
    );
    let restored = PgConnectOptions::from_url(&options.to_url_lossy()).unwrap();
    assert_eq!(
        restored.get_socket_timeout(),
        Some(Duration::from_millis(125))
    );
    for value in ["-1", "NaN", "inf", "1e999", "invalid"] {
        assert!(format!("postgres://localhost/test?socket-timeout={value}")
            .parse::<PgConnectOptions>()
            .is_err());
    }
}

#[test]
#[should_panic(expected = "socket_timeout must be greater than zero; use None to disable it")]
fn socket_timeout_builder_rejects_zero() {
    PgConnectOptions::new_without_pgpass().socket_timeout(Duration::ZERO);
}

#[test]
fn socket_timeout_url_rejects_zero_with_clear_error() {
    for value in ["0", "0.0", "-0.0", "1e-100"] {
        let error = format!("postgres://localhost/test?socket-timeout={value}")
            .parse::<PgConnectOptions>()
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("socket-timeout must be greater than zero"),
            "{error}"
        );
    }
}

#[test]
fn copy_in_slow_user_source_is_not_a_socket_wait() {
    let (options, server) = server(|mut socket| {
        startup(&mut socket);
        assert_eq!(frame(&mut socket).0, b'Q');
        message(&mut socket, b'G', &[0, 0, 0]); // CopyInResponse, zero columns
        let mut data = Vec::new();
        loop {
            let (tag, body) = frame(&mut socket);
            match tag {
                b'd' => data.extend_from_slice(&body),
                b'c' => break,
                tag => panic!("unexpected COPY message {tag}"),
            }
        }
        assert_eq!(data, b"slow data\n");
        message(&mut socket, b'C', b"COPY 1\0");
        message(&mut socket, b'Z', b"I");
    });
    test_block_on(async {
        let mut connection = options.connect().await.unwrap();
        let (mut source_writer, mut source) = tokio::io::duplex(32);
        let producer = async move {
            sleep(BOUND * 5).await;
            tokio::io::AsyncWriteExt::write_all(&mut source_writer, b"slow data\n")
                .await
                .unwrap();
        };
        let copy = async {
            let mut copy = connection
                .copy_in_raw("COPY test FROM STDIN")
                .await
                .unwrap();
            copy.read_from(&mut source).await.unwrap();
            assert_eq!(copy.finish().await.unwrap(), 1);
        };
        timeout(WATCHDOG, futures_util::future::join(producer, copy))
            .await
            .unwrap();
    });
    server.join().unwrap();
}

#[test]
fn partial_reads_reset_the_inactivity_timeout() {
    let (options, server) = server(|mut socket| {
        startup(&mut socket);
        assert_eq!(frame(&mut socket).0, b'Q');
        // The whole message takes longer than the timeout, but each wait makes progress.
        let body = b"SELECT 1\0";
        socket.write_all(b"C").unwrap();
        thread::sleep(BOUND / 5);
        socket
            .write_all(&(u32::try_from(body.len()).unwrap() + 4).to_be_bytes())
            .unwrap();
        for chunk in body.chunks(2) {
            thread::sleep(BOUND / 5);
            socket.write_all(chunk).unwrap();
        }
        message(&mut socket, b'Z', b"I");
    });
    test_block_on(async {
        let mut connection = options.connect().await.unwrap();
        timeout(WATCHDOG, connection.execute("SELECT 1"))
            .await
            .unwrap()
            .unwrap();
    });
    server.join().unwrap();
}

// ---------------------------------------------------------------------------
// TLS transport-progress witnesses (S1/S2, codex review of the socket patch).
//
// The inactivity bound must apply to CIPHERTEXT progress, not to the plaintext
// a TLS session exposes: a record that arrives in fragments over more than the
// bound is a peer that is still talking, and a partly arrived record must not
// leave an idle listener waiting forever. Both witnesses run a scripted rustls
// SERVER on a loopback socket and drive the real client through a TLS session.
// ---------------------------------------------------------------------------

mod tls_progress {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::ops::ControlFlow;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    use sqlx_core::connection::ConnectOptions;
    use sqlx_core::executor::Executor;
    use sqlx_core::net::tls::{self, CertificateInput, TlsConfig};
    use sqlx_core::net::{connect_tcp, BufferedSocket, Socket, SocketIntoBox, WithSocket};
    use sqlx_core::rt::{sleep, test_block_on, timeout};
    use sqlx_postgres::{PgConnectOptions, PgListener, PgPoolOptions, PgSslMode};

    use super::{assert_timeout, BOUND, WATCHDOG};

    const CERT_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/certs");

    fn server_config() -> Arc<rustls::ServerConfig> {
        use rustls::pki_types::pem::PemObject;
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};

        let certs = CertificateDer::pem_slice_iter(
            &std::fs::read(format!("{CERT_DIR}/server.crt")).unwrap(),
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let key = PrivateKeyDer::from_pem_slice(
            &std::fs::read(format!("{CERT_DIR}/keys/server.key")).unwrap(),
        )
        .unwrap();
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
        Arc::new(config)
    }

    /// One scripted Postgres-over-TLS session.
    struct TlsPeer {
        conn: rustls::ServerConnection,
        tcp: TcpStream,
    }

    impl TlsPeer {
        fn accept(mut tcp: TcpStream) -> Self {
            // SSLRequest: length 8, code 80877103; answer `S` and start TLS.
            let mut request = [0u8; 8];
            tcp.read_exact(&mut request).unwrap();
            assert_eq!(&request[4..], &80877103_u32.to_be_bytes());
            tcp.write_all(b"S").unwrap();
            let mut conn = rustls::ServerConnection::new(server_config()).unwrap();
            while conn.is_handshaking() {
                conn.complete_io(&mut tcp).unwrap();
            }
            Self { conn, tcp }
        }

        fn read_plain(&mut self, buf: &mut [u8]) -> usize {
            loop {
                match self.conn.reader().read(buf) {
                    Ok(read) if read > 0 => return read,
                    Ok(_) => panic!("client closed the TLS session"),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        self.conn.complete_io(&mut self.tcp).unwrap();
                    }
                    Err(error) => panic!("TLS read failed: {error}"),
                }
            }
        }

        fn read_exact_plain(&mut self, buf: &mut [u8]) {
            let mut offset = 0;
            while offset < buf.len() {
                offset += self.read_plain(&mut buf[offset..]);
            }
        }

        fn read_frame(&mut self) -> (u8, Vec<u8>) {
            let mut header = [0u8; 5];
            self.read_exact_plain(&mut header);
            let len = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
            let mut body = vec![0; len - 4];
            self.read_exact_plain(&mut body);
            (header[0], body)
        }

        fn startup(&mut self) {
            let mut len = [0u8; 4];
            self.read_exact_plain(&mut len);
            let mut body = vec![0; u32::from_be_bytes(len) as usize - 4];
            self.read_exact_plain(&mut body);
            self.write_plain(&[message(b'R', &[0; 4]), message(b'Z', b"I")].concat());
        }

        fn write_plain(&mut self, plaintext: &[u8]) {
            self.conn.writer().write_all(plaintext).unwrap();
            while self.conn.wants_write() {
                self.conn.write_tls(&mut self.tcp).unwrap();
            }
        }

        /// Encrypt `plaintext` into the wire bytes of ONE TLS record, without
        /// sending them: the caller decides how the ciphertext is paced.
        fn seal(&mut self, plaintext: &[u8]) -> Vec<u8> {
            self.conn.writer().write_all(plaintext).unwrap();
            let mut wire = Vec::new();
            while self.conn.wants_write() {
                self.conn.write_tls(&mut wire).unwrap();
            }
            wire
        }

        fn tcp(&self) -> TcpStream {
            self.tcp.try_clone().unwrap()
        }
    }

    fn message(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(body.len() + 5);
        out.push(tag);
        out.extend_from_slice(&u32::try_from(body.len() + 4).unwrap().to_be_bytes());
        out.extend_from_slice(body);
        out
    }

    fn options(port: u16) -> PgConnectOptions {
        options_with(port, BOUND)
    }

    fn options_with(port: u16, bound: Duration) -> PgConnectOptions {
        PgConnectOptions::new_without_pgpass()
            .host("127.0.0.1")
            .port(port)
            .username("test")
            .database("test")
            .ssl_mode(PgSslMode::VerifyCa)
            .ssl_root_cert(format!("{CERT_DIR}/ca.crt"))
            .socket_timeout(bound)
    }

    fn listener(
        script: impl FnOnce(TlsPeer) + Send + 'static,
    ) -> (PgConnectOptions, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = thread::spawn(move || script(TlsPeer::accept(listener.accept().unwrap().0)));
        (options(port), handle)
    }

    /// Raw-TCP read accounting shared with the witness, so the test can prove
    /// the next record's prefix had ALREADY crossed the client's raw-read
    /// boundary before the idle wait began. One server write alone cannot
    /// guarantee one client read, so the precondition is asserted, not
    /// assumed.
    #[derive(Default)]
    struct RawReads {
        bytes: AtomicUsize,
        last: Mutex<Option<Instant>>,
    }

    impl RawReads {
        fn record(&self, bytes: usize) {
            self.bytes.fetch_add(bytes, Ordering::SeqCst);
            *self.last.lock().unwrap() = Some(Instant::now());
        }

        fn bytes(&self) -> usize {
            self.bytes.load(Ordering::SeqCst)
        }

        fn last(&self) -> Option<Instant> {
            *self.last.lock().unwrap()
        }
    }

    /// The transport under `StdSocket`: counts raw bytes so the witness can
    /// see what the TLS session consumed.
    struct CountingSocket<S> {
        inner: S,
        reads: Arc<RawReads>,
    }

    impl<S: Socket> Socket for CountingSocket<S> {
        fn try_read(&mut self, buf: &mut dyn sqlx_core::io::ReadBuf) -> std::io::Result<usize> {
            let read = self.inner.try_read(buf)?;
            if read > 0 {
                self.reads.record(read);
            }
            Ok(read)
        }

        fn try_write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.inner.try_write(buf)
        }

        fn poll_read_ready(
            &mut self,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            self.inner.poll_read_ready(cx)
        }

        fn poll_write_ready(
            &mut self,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            self.inner.poll_write_ready(cx)
        }

        fn poll_shutdown(
            &mut self,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            self.inner.poll_shutdown(cx)
        }

        fn last_transport_progress(&self) -> Option<Instant> {
            self.inner.last_transport_progress()
        }

        fn last_receive_progress(&self) -> Option<Instant> {
            self.inner.last_receive_progress()
        }

        fn has_incomplete_incoming_record(&self) -> bool {
            // Record framing above this transport is what can be part-way
            // through; a raw socket has no framing of its own.
            false
        }
    }

    /// Performs the Postgres SSLRequest and the TLS upgrade over a counted
    /// raw socket, exactly as the driver's `MaybeUpgradeTls` does.
    struct CountedTlsUpgrade {
        reads: Arc<RawReads>,
        ca: CertificateInput,
    }

    impl WithSocket for CountedTlsUpgrade {
        type Output = sqlx_core::Result<Box<dyn Socket>>;

        async fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
            let mut socket = CountingSocket {
                inner: socket,
                reads: self.reads,
            };

            // SSLRequest: length 8, code 80877103.
            socket.write(&[0, 0, 0, 8, 0x04, 0xD2, 0x16, 0x2F]).await?;
            socket.flush().await?;
            let mut answer = [0u8; 1];
            {
                let mut target: &mut [u8] = &mut answer;
                socket.read(&mut target).await?;
            }
            assert_eq!(&answer, b"S", "the scripted peer must accept TLS");

            let config = TlsConfig {
                accept_invalid_certs: false,
                accept_invalid_hostnames: true,
                hostname: "127.0.0.1",
                root_cert_path: Some(&self.ca),
                client_cert_path: None,
                client_key_path: None,
            };

            tls::handshake(socket, config, SocketIntoBox).await
        }
    }

    fn startup_message() -> Vec<u8> {
        let mut body = vec![0, 3, 0, 0]; // protocol 3.0
        body.extend_from_slice(b"user\0test\0database\0test\0\0");
        let mut out = u32::try_from(body.len() + 4)
            .unwrap()
            .to_be_bytes()
            .to_vec();
        out.extend_from_slice(&body);
        out
    }

    fn query(sql: &str) -> Vec<u8> {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        let mut out = vec![b'Q'];
        out.extend_from_slice(&u32::try_from(body.len() + 4).unwrap().to_be_bytes());
        out.extend_from_slice(&body);
        out
    }

    async fn read_exact(socket: &mut Box<dyn Socket>, len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            let mut chunk = vec![0u8; len - out.len()];
            let mut target: &mut [u8] = &mut chunk;
            let read = socket.read(&mut target).await.expect("protocol read");
            assert!(read > 0, "the scripted peer closed during bootstrap");
            out.extend_from_slice(&chunk[..read]);
        }
        out
    }

    struct Bootstrapped {
        socket: BufferedSocket<Box<dyn Socket>>,
        /// Raw bytes read through the startup response.
        after_startup: usize,
        /// Raw bytes read through the LISTEN response — plus anything the
        /// same reads pulled with it.
        after_listen: usize,
        /// Instant of the last raw read during the LISTEN exchange.
        last_read: Option<Instant>,
    }

    /// Connect over TLS and complete the startup + LISTEN exchange, leaving
    /// the socket idle. Any bytes read beyond the LISTEN response record were
    /// pulled by these reads.
    async fn bootstrap(port: u16, reads: Arc<RawReads>) -> Bootstrapped {
        let ca = CertificateInput::File(format!("{CERT_DIR}/ca.crt").into());
        let mut socket = connect_tcp(
            "127.0.0.1",
            port,
            CountedTlsUpgrade {
                reads: reads.clone(),
                ca,
            },
        )
        .await
        .expect("TCP connect")
        .expect("TLS upgrade");

        socket.write(&startup_message()).await.expect("startup");
        socket.flush().await.expect("startup flush");
        let ready = read_exact(&mut socket, 15).await; // AuthenticationOk + ReadyForQuery
        assert_eq!(ready[0], b'R');
        assert_eq!(ready[9], b'Z');
        let after_startup = reads.bytes();

        socket
            .write(&query("LISTEN \"chan\""))
            .await
            .expect("LISTEN");
        socket.flush().await.expect("LISTEN flush");
        let done = read_exact(&mut socket, 18).await; // CommandComplete + ReadyForQuery
        assert_eq!(done[0], b'C');
        assert_eq!(done[12], b'Z');

        Bootstrapped {
            socket: BufferedSocket::new(socket),
            after_startup,
            after_listen: reads.bytes(),
            last_read: reads.last(),
        }
    }

    fn notification() -> Vec<u8> {
        message(b'A', &[0, 0, 0, 8, b'c', b'h', b'a', b'n', 0])
    }

    fn wait_for_a_byte(
        buf: &mut sqlx_core::bytes::BytesMut,
    ) -> Result<ControlFlow<(), usize>, sqlx_core::Error> {
        Ok(ControlFlow::Continue(buf.len() + 1))
    }

    /// Script a peer that answers LISTEN, then sends the response record and
    /// the first half of the next record AS ONE WRITE, and keeps the
    /// connection OPEN and SILENT — no second half, no EOF, so a reconnect
    /// path is unreachable. The response record's wire length arrives on the
    /// channel for the precondition assertion.
    fn prefetch_peer() -> (
        u16,
        std::sync::mpsc::Receiver<usize>,
        std::sync::mpsc::Sender<()>,
        thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = std::sync::mpsc::channel();
        let (stop, stopped) = std::sync::mpsc::channel::<()>();
        let handle = thread::spawn(move || {
            let mut peer = TlsPeer::accept(listener.accept().unwrap().0);
            peer.startup();
            assert_eq!(peer.read_frame().0, b'Q'); // LISTEN
            let response = peer.seal(&[message(b'C', b"LISTEN\0"), message(b'Z', b"I")].concat());
            let partial = peer.seal(&notification());
            let record_a = response.len();
            let mut one_write = response;
            one_write.extend_from_slice(&partial[..partial.len() / 2]);
            let mut tcp = peer.tcp();
            let wrote = tcp.write(&one_write).unwrap();
            assert_eq!(
                wrote,
                one_write.len(),
                "one write must carry the response record and the prefix"
            );
            tcp.flush().unwrap();
            tx.send(record_a).unwrap();
            // Hold the socket open and silent: no second half, no EOF.
            let _ = stopped.recv_timeout(Duration::from_secs(10));
        });
        (port, rx, stop, handle)
    }

    /// Tests run in parallel in one process, so a callsite whose first event
    /// fires outside any thread-local subscriber can be cached as disabled
    /// and never reach a later `with_default`. A global subscriber that is
    /// interested in everything keeps every callsite open; the events are
    /// still delivered to the thread-local subscriber that is installed
    /// around the wait.
    struct InterestKeepAlive;

    impl tracing::Subscriber for InterestKeepAlive {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, _: &tracing::Event<'_>) {}
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    fn install_interest_keepalive() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            // Another test may have installed one first; either is fine, the
            // callsite interest is what matters.
            let _ = tracing::subscriber::set_global_default(InterestKeepAlive);
        });
    }

    #[derive(Default)]
    struct PoisonWarnings {
        count: AtomicUsize,
    }

    impl tracing::Subscriber for PoisonWarnings {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            // The poison line names the socket; connect-phase warnings name
            // host/port and must not count here.
            if event.metadata().target() == "sqlx::net::timeout"
                && event.fields().any(|field| field.name() == "socket_id")
            {
                self.count.fetch_add(1, Ordering::SeqCst);
            }
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    /// S1: a single TLS record whose ciphertext arrives in fragments over more
    /// than the bound must NOT time out — the transport is making progress, so
    /// the bound restarts (SO_RCVTIMEO semantics), and the query succeeds.
    #[test]
    fn fragmented_tls_record_keeps_the_connection_alive() {
        let (options, server) = listener(|mut peer| {
            peer.startup();
            assert_eq!(peer.read_frame().0, b'Q');
            // The whole response in ONE record; its ciphertext is dribbled on
            // the wire over ~400 ms while the client bound is 250 ms.
            let mut tag = vec![b'x'; 400];
            tag.push(0);
            let response = [message(b'C', &tag), message(b'Z', b"I")].concat();
            let wire = peer.seal(&response);
            let chunk = std::cmp::max(wire.len().div_ceil(8), 1);
            let mut tcp = peer.tcp();
            for piece in wire.chunks(chunk) {
                tcp.write_all(piece).unwrap();
                tcp.flush().unwrap();
                thread::sleep(Duration::from_millis(50));
            }
        });

        test_block_on(async {
            let mut connection = options.connect().await.unwrap();
            timeout(WATCHDOG, connection.execute("SELECT 1"))
                .await
                .expect("the bound must not fire while ciphertext keeps arriving")
                .expect("the fragmented response must be accepted");
        });
        server.join().unwrap();
    }

    /// S2: an idle listener that has received PART of a TLS record must time
    /// out — transport progress arms the bound even though the plaintext
    /// buffer is empty. Before the fix this waited forever (the idle path
    /// disabled the bound on an empty plaintext buffer).
    #[test]
    fn partial_tls_record_times_out_on_an_idle_listener() {
        let (options, server) = listener(|mut peer| {
            peer.startup();
            assert_eq!(peer.read_frame().0, b'Q'); // LISTEN
            peer.write_plain(&[message(b'C', b"LISTEN\0"), message(b'Z', b"I")].concat());

            // Half a notification record, then silence.
            let notification = message(b'A', &[0, 0, 0, 8, b'c', b'h', b'a', b'n', 0]);
            let wire = peer.seal(&notification);
            let mut tcp = peer.tcp();
            tcp.write_all(&wire[..wire.len() / 2]).unwrap();
            tcp.flush().unwrap();
            thread::sleep(Duration::from_secs(2));
        });

        test_block_on(async {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(WATCHDOG)
                .connect_lazy_with(options);
            let mut listener = PgListener::connect_with(&pool).await.unwrap();
            listener.listen("chan").await.unwrap();
            let error = timeout(WATCHDOG, listener.try_recv())
                .await
                .expect("a partly arrived record must arm the bound")
                .expect_err("the partial record must not be deliverable");
            assert_timeout(error);
        });
        server.join().unwrap();
    }

    /// The bound is EXACT: ciphertext that arrives just after the wait starts
    /// and then stops must time out one bound after that byte, never after two.
    /// A restart that hands out a fresh full window would let the silence
    /// stretch to ~2 x bound.
    #[test]
    fn silence_after_progress_times_out_within_one_bound() {
        const EXACT_BOUND: Duration = Duration::from_millis(400);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let mut peer = TlsPeer::accept(listener.accept().unwrap().0);
            peer.startup();
            assert_eq!(peer.read_frame().0, b'Q');
            // One byte of ciphertext of the response record, then silence.
            let wire = peer.seal(&[message(b'C', b"SELECT 1\0"), message(b'Z', b"I")].concat());
            let mut tcp = peer.tcp();
            tcp.write_all(&wire[..1]).unwrap();
            tcp.flush().unwrap();
            thread::sleep(Duration::from_secs(3));
        });

        test_block_on(async {
            let mut connection = options_with(port, EXACT_BOUND).connect().await.unwrap();
            let started = std::time::Instant::now();
            let error = timeout(WATCHDOG, connection.execute("SELECT 1"))
                .await
                .expect("the bound must fire once the silence exceeds it")
                .unwrap_err();
            let elapsed = started.elapsed();
            assert_timeout(error);
            assert!(
                elapsed >= EXACT_BOUND,
                "fired before the bound: {elapsed:?}"
            );
            assert!(
                elapsed < EXACT_BOUND * 2 - Duration::from_millis(50),
                "silence reached nearly two bounds: {elapsed:?}"
            );
        });
        server.join().unwrap();
    }

    /// A message that spans TWO TLS records — record 1 delivers the frame
    /// header, record 2 never arrives — must time out on an idle listener.
    /// The bound arms on the first byte of the MESSAGE that the session
    /// delivered (partial plaintext), not only on ciphertext it has buffered:
    /// the single-record witness above covers "no plaintext yet", this one
    /// covers "some plaintext, rest missing".
    #[test]
    fn partial_message_across_two_tls_records_times_out_on_an_idle_listener() {
        let (options, server) = listener(|mut peer| {
            peer.startup();
            assert_eq!(peer.read_frame().0, b'Q'); // LISTEN
            peer.write_plain(&[message(b'C', b"LISTEN\0"), message(b'Z', b"I")].concat());

            // Record 1 carries only the 5-byte header of a notification; the
            // record with its body is never sent.
            let notification = message(b'A', &[0, 0, 0, 8, b'c', b'h', b'a', b'n', 0]);
            let wire = peer.seal(&notification[..5]);
            let mut tcp = peer.tcp();
            tcp.write_all(&wire).unwrap();
            tcp.flush().unwrap();
            thread::sleep(Duration::from_secs(2));
        });

        test_block_on(async {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(WATCHDOG)
                .connect_lazy_with(options);
            let mut listener = PgListener::connect_with(&pool).await.unwrap();
            listener.listen("chan").await.unwrap();
            let error = timeout(WATCHDOG, listener.try_recv())
                .await
                .expect("a message that started arriving must arm the bound")
                .expect_err("the incomplete message must not be deliverable");
            assert_timeout(error);
        });
        server.join().unwrap();
    }

    /// The ENTRY window of ruling (d), on the public listener: a read that
    /// COMPLETES can pull the first half of the NEXT TLS record into the
    /// session at the same time. The idle wait that follows then has empty
    /// plaintext and no NEW transport progress since that read, so the bound
    /// must be armed by the undecoded bytes the session is holding.
    ///
    /// The deterministic precondition — the prefix crossed the client's
    /// raw-read boundary BEFORE the wait — is asserted in
    /// `a_record_prefetched_by_the_previous_read_bounds_the_next_idle_wait`,
    /// which owns the raw socket; this witness drives the public API. The
    /// peer keeps the connection OPEN and SILENT: no EOF, so a lost-connection
    /// path cannot mask the bound. The connection is not re-established
    /// (`eager_reconnect(false)`), so the original socket's poison is the one
    /// and only outcome, and `try_recv` reports `Ok(None)`.
    #[test]
    fn a_record_pulled_by_the_previous_read_still_bounds_the_next_idle_wait() {
        let (port, record_a, stop, server) = prefetch_peer();
        let poison = Arc::new(PoisonWarnings::default());

        install_interest_keepalive();
        let elapsed = tracing::subscriber::with_default(poison.clone(), || {
            test_block_on(async {
                let options = options(port);
                let pool = PgPoolOptions::new()
                    .max_connections(1)
                    .acquire_timeout(WATCHDOG)
                    .connect_lazy_with(options);
                let mut listener = PgListener::connect_with(&pool).await.unwrap();
                listener.listen("chan").await.unwrap();
                listener.eager_reconnect(false);

                let record_a = record_a.recv_timeout(WATCHDOG).expect("server length");
                assert!(record_a > 0);

                let started = Instant::now();
                let waited = timeout(WATCHDOG, listener.try_recv())
                    .await
                    .expect("the prefetched partial record must arm the bound");
                let elapsed = started.elapsed();
                let notification = waited.expect("a poisoned connection is not a query error");
                assert!(
                    notification.is_none(),
                    "the partial record must not be deliverable"
                );
                assert!(
                    elapsed < BOUND + Duration::from_millis(500),
                    "the idle wait reached {elapsed:?}; the partial record pulled by the \
                     previous read did not bound it"
                );
                elapsed
            })
        });

        assert!(
            elapsed < BOUND + Duration::from_millis(500),
            "the bound must fire without waiting for the outer watchdog"
        );
        assert_eq!(
            poison.count.load(Ordering::SeqCst),
            1,
            "exactly one poison warning for the original socket"
        );
        let _ = stop.send(());
        server.join().unwrap();
    }

    /// The same prefetched prefix, asserted on the raw socket: the read that
    /// COMPLETES the LISTEN exchange also pulls the first half of the next
    /// record, and the idle wait that follows must fire at ONE bound measured
    /// from the RECEIVED BYTE — the original anchor, not a fresh one from the
    /// wait's entry. This is the witness old code fails by hanging: nothing
    /// moves during the wait, so there is nothing for a progress-only rule to
    /// see.
    #[test]
    fn a_record_prefetched_by_the_previous_read_bounds_the_next_idle_wait() {
        let (port, record_a, stop, server) = prefetch_peer();
        let reads = Arc::new(RawReads::default());

        test_block_on(async {
            let boot = timeout(WATCHDOG, bootstrap(port, reads.clone()))
                .await
                .expect("bootstrap must complete");
            let record_a = record_a.recv_timeout(WATCHDOG).expect("server length");
            assert!(
                boot.after_listen - boot.after_startup > record_a,
                "precondition: the next record's prefix must already have crossed the \
                 client's raw-read boundary (read {} ciphertext bytes after startup; \
                 the response record alone is {record_a})",
                boot.after_listen - boot.after_startup,
            );

            let anchor = boot.last_read.expect("the prefix read must be recorded");
            let mut socket = boot.socket;
            socket.set_io_timeout(Some(BOUND));

            let error = timeout(WATCHDOG, socket.try_read_idle(wait_for_a_byte))
                .await
                .expect("the prefetched prefix must arm the bound")
                .unwrap_err();
            let fired_at = Instant::now();
            assert_timeout(error);

            let from_anchor = fired_at.duration_since(anchor);
            assert!(
                from_anchor >= BOUND,
                "the wait fired before the received byte's bound: {from_anchor:?}"
            );
            assert!(
                from_anchor < BOUND + Duration::from_millis(500),
                "the deadline was renewed past the original anchor: {from_anchor:?}"
            );

            // The poison is permanent: the next wait fails immediately with
            // the same typed error instead of waiting again.
            let started = Instant::now();
            let again = socket.try_read_idle(wait_for_a_byte).await.unwrap_err();
            assert_timeout(again);
            assert!(
                started.elapsed() < Duration::from_millis(50),
                "a poisoned socket must not wait again"
            );
        });
        let _ = stop.send(());
        server.join().unwrap();
    }

    /// The prefetched prefix with a DELAYED entry: the wait starts half a
    /// bound after the byte arrived. The deadline must still fall at the
    /// received byte's bound, not restart from the delayed entry — a renewed
    /// full bound would let a dead peer hold the connection for up to two
    /// bounds.
    #[test]
    fn a_delayed_idle_wait_is_not_granted_a_fresh_bound() {
        let (port, record_a, stop, server) = prefetch_peer();
        let reads = Arc::new(RawReads::default());

        test_block_on(async {
            let boot = timeout(WATCHDOG, bootstrap(port, reads.clone()))
                .await
                .expect("bootstrap must complete");
            let record_a = record_a.recv_timeout(WATCHDOG).expect("server length");
            assert!(
                boot.after_listen - boot.after_startup > record_a,
                "precondition: the next record's prefix must already have crossed the \
                 client's raw-read boundary"
            );

            let anchor = boot.last_read.expect("the prefix read must be recorded");
            let mut socket = boot.socket;
            socket.set_io_timeout(Some(BOUND));

            // Enter the wait half a bound late.
            sleep(BOUND / 2).await;

            let started = Instant::now();
            let error = timeout(WATCHDOG, socket.try_read_idle(wait_for_a_byte))
                .await
                .expect("the prefetched prefix must arm the bound")
                .unwrap_err();
            let elapsed = started.elapsed();
            assert_timeout(error);
            assert!(
                elapsed < BOUND * 3 / 4,
                "the delayed entry was granted a fresh bound: waited {elapsed:?}"
            );
            assert!(
                Instant::now().duration_since(anchor) >= BOUND,
                "the deadline must not fall before the received byte's bound"
            );
        });
        let _ = stop.send(());
        server.join().unwrap();
    }

    /// The other half of S2: an idle listener with NO transport activity at all
    /// must keep waiting — the bound arms on the first byte, it does not fire
    /// on a quiet connection.
    #[test]
    fn idle_listener_without_transport_activity_keeps_waiting() {
        let (options, server) = listener(|mut peer| {
            peer.startup();
            assert_eq!(peer.read_frame().0, b'Q'); // LISTEN
            peer.write_plain(&[message(b'C', b"LISTEN\0"), message(b'Z', b"I")].concat());
            thread::sleep(Duration::from_secs(2));
        });

        test_block_on(async {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(WATCHDOG)
                .connect_lazy_with(options);
            let mut listener = PgListener::connect_with(&pool).await.unwrap();
            listener.listen("chan").await.unwrap();
            let waited = timeout(BOUND * 3, listener.try_recv()).await;
            assert!(
                waited.is_err(),
                "an idle connection with no bytes must not time out"
            );
        });
        server.join().unwrap();
    }
}
