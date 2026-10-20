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

/// billing-7t7r P2 (grok): the plaintext backend's outgoing waits count
/// OUTBOUND progress only. A peer that stops reading a COPY send while
/// streaming bytes back must not hold the blocked flush open: it fires one
/// bound after the last byte the kernel accepted from us. (Plaintext never
/// read during a write, so this was already true; the witness pins the
/// uniform rule the TLS backends now share, and fails if a future change
/// starts counting inbound bytes on this arm.)
#[test]
fn inbound_bytes_do_not_extend_a_blocked_plaintext_flush() {
    let (stop, stopped) = std::sync::mpsc::channel();
    let (options, server) = server(move |mut socket| {
        startup(&mut socket);
        assert_eq!(frame(&mut socket).0, b'Q');
        // CopyInResponse, one text column.
        message(&mut socket, b'G', &[0, 0, 1, 0, 0]);
        // Stop reading; keep the wire alive from our side instead.
        for _ in 0..12 {
            if socket.write_all(&[0]).is_err() {
                return;
            }
            let _ = socket.flush();
            thread::sleep(BOUND / 4);
        }
        stopped.recv_timeout(WATCHDOG * 2).unwrap();
    });
    test_block_on(async {
        let mut connection = options.connect().await.unwrap();
        let mut copy = connection
            .copy_in_raw("COPY test FROM STDIN")
            .await
            .unwrap();
        let data = vec![0_u8; 64 * 1024];
        let (error, waited) = timeout(WATCHDOG, async {
            let mut last_accepted = std::time::Instant::now();
            loop {
                match copy.send(data.as_slice()).await {
                    Ok(_) => last_accepted = std::time::Instant::now(),
                    Err(error) => return (error, last_accepted.elapsed()),
                }
            }
        })
        .await
        .expect("the outbound window must fill and the bound must fire");
        stop.send(()).unwrap();
        assert_timeout(error);
        assert!(
            waited >= BOUND,
            "the flush failed before one bound of outbound silence: {waited:?}"
        );
        assert!(
            waited < BOUND + BOUND / 2,
            "an inbound byte extended the outgoing wait ({waited:?}); outgoing waits \
             count outbound progress only"
        );
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

        fn last_send_progress(&self) -> Option<Instant> {
            self.inner.last_send_progress()
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

    struct Bootstrapped {
        socket: BufferedSocket<Box<dyn Socket>>,
        /// Raw bytes read through the startup response.
        after_startup: usize,
        /// Raw bytes read through the LISTEN response — plus anything the
        /// same reads pulled with it.
        after_listen: usize,
    }

    /// Connect over TLS and complete the startup + LISTEN exchange, leaving
    /// the socket idle. Any bytes read beyond the LISTEN response record were
    /// pulled by these reads.
    async fn bootstrap(port: u16, reads: Arc<RawReads>) -> Bootstrapped {
        let ca = CertificateInput::File(format!("{CERT_DIR}/ca.crt").into());
        let socket = connect_tcp(
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

        // The SAME buffered socket a driver uses, from the first exchange on:
        // its "progress since the last completed read" memory has to be primed
        // by these reads, or the entry window under test is not the driver's.
        let mut socket = BufferedSocket::new(socket);
        socket.set_io_timeout(Some(BOUND));

        let startup = startup_message();
        socket.write(startup.as_slice()).expect("startup write");
        socket.flush().await.expect("startup flush");
        let ready = socket.read_buffered(15).await.expect("startup response");
        // AuthenticationOk + ReadyForQuery
        assert_eq!(ready[0], b'R');
        assert_eq!(ready[9], b'Z');
        let after_startup = reads.bytes();

        let listen = query("LISTEN \"chan\"");
        socket.write(listen.as_slice()).expect("LISTEN write");
        socket.flush().await.expect("LISTEN flush");
        let done = socket.read_buffered(18).await.expect("LISTEN response");
        // CommandComplete + ReadyForQuery
        assert_eq!(done[0], b'C');
        assert_eq!(done[12], b'Z');

        Bootstrapped {
            socket,
            after_startup,
            after_listen: reads.bytes(),
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

    /// What [`prefetch_peer`] hands back: the port (read only once the
    /// listener is bound, never before), the channel carrying the response
    /// record's wire length for the precondition assertion, the stop channel
    /// the test uses to release the peer thread, and that thread's handle.
    type PrefetchPeer = (
        u16,
        std::sync::mpsc::Receiver<(usize, usize)>,
        std::sync::mpsc::Sender<()>,
        thread::JoinHandle<()>,
    );

    /// Script a peer that answers LISTEN, then sends the response record and
    /// the first half of the next record AS ONE WRITE, and keeps the
    /// connection OPEN and SILENT — no second half, no EOF, so a reconnect
    /// path is unreachable. The response record's wire length arrives on the
    /// channel for the precondition assertion.
    fn prefetch_peer() -> PrefetchPeer {
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
            // (the response record on its own, the whole single write)
            tx.send((record_a, one_write.len())).unwrap();
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

    /// Codex P2: the rustls backend must finish negotiation INSIDE the setup
    /// deadline. A server that paces its first handshake flight over several
    /// bounds must make `connect()` fail AT the bound, exactly as native-tls
    /// already does; rustls's `complete_io` used to return while
    /// `is_handshaking` was still true, so the connection resolved early and
    /// the remaining flight was negotiated by later buffered flushes, outside
    /// the deadline (~920 ms measured with a 250 ms bound).
    ///
    /// A raw TCP listener answers the SSLRequest, reads the client's
    /// ClientHello through a capture sink so the server's first flight can be
    /// PACED, then dribbles it at 64 bytes every 60 ms.
    #[test]
    fn a_paced_tls_handshake_is_bounded_inside_the_setup_deadline() {
        struct Capture {
            tcp: TcpStream,
            out: Vec<u8>,
        }

        impl Read for Capture {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                self.tcp.read(buf)
            }
        }

        impl Write for Capture {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.out.extend_from_slice(buf);
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let mut tcp = listener.accept().unwrap().0;
            let mut request = [0u8; 8];
            tcp.read_exact(&mut request).unwrap();
            assert_eq!(&request[4..], &80877103_u32.to_be_bytes());
            tcp.write_all(b"S").unwrap();
            tcp.flush().unwrap();

            let mut conn = rustls::ServerConnection::new(server_config()).unwrap();
            // Read the ClientHello and process it without `complete_io`'s
            // read-after-write: the response flight is captured, not sent, so
            // the test can pace it.
            loop {
                let read = conn.read_tls(&mut tcp).unwrap();
                assert!(read > 0, "the client closed before the ClientHello");
                conn.process_new_packets()
                    .expect("the ClientHello is well formed");
                if conn.wants_write() {
                    break;
                }
            }
            let mut capture = Capture {
                tcp: tcp.try_clone().unwrap(),
                out: Vec::new(),
            };
            while conn.wants_write() {
                conn.write_tls(&mut capture).unwrap();
            }
            let flight = capture.out;
            assert!(!flight.is_empty(), "the server produced a handshake flight");
            for chunk in flight.chunks(64) {
                // The client's deadline may already have closed the socket;
                // that is the outcome under test.
                if tcp.write_all(chunk).is_err() {
                    break;
                }
                let _ = tcp.flush();
                thread::sleep(Duration::from_millis(60));
            }
            // Keep the connection open while the client's deadline decides.
            thread::sleep(Duration::from_secs(2));
        });

        test_block_on(async {
            let started = Instant::now();
            let error = timeout(WATCHDOG, options(port).connect())
                .await
                .expect("connect must return inside the watchdog")
                .expect_err("a handshake paced past the setup deadline must not connect");
            let elapsed = started.elapsed();
            assert_timeout(error);
            // The DISCRIMINATOR: with the fix, negotiation runs inside the
            // setup deadline and connect fails AT the bound; a premature
            // handshake return lets the paced flight finish on later buffered
            // flushes (codex measured ~918 ms with this 250 ms bound). Half a
            // bound of slack keeps a loaded host from failing the fix.
            assert!(
                elapsed < BOUND + BOUND / 2,
                "connect returned after {elapsed:?}; negotiation escaped the setup \
                 deadline (the fix fails setup at ~{BOUND:?})"
            );
        });
        server.join().unwrap();
    }

    /// Accepts everything until `flight_sent` is raised, then lets at most
    /// `budget` further bytes cross and reports write-blocked forever after.
    /// Reads and read readiness pass through untouched.
    struct BlockFinalFlight<S> {
        inner: S,
        flight_sent: Arc<AtomicBool>,
        budget: usize,
    }

    impl<S: Socket> Socket for BlockFinalFlight<S> {
        fn try_read(&mut self, buf: &mut dyn sqlx_core::io::ReadBuf) -> std::io::Result<usize> {
            self.inner.try_read(buf)
        }

        fn try_write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.flight_sent.load(Ordering::SeqCst) {
                if self.budget == 0 {
                    return Err(std::io::ErrorKind::WouldBlock.into());
                }
                let allowed = std::cmp::min(self.budget, buf.len());
                let written = self.inner.try_write(&buf[..allowed])?;
                self.budget -= written;
                return Ok(written);
            }
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
            if self.flight_sent.load(Ordering::SeqCst) && self.budget == 0 {
                // The final flight is stuck by construction: never ready,
                // never woken. Only the setup deadline can end this.
                return std::task::Poll::Pending;
            }
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

        fn last_send_progress(&self) -> Option<Instant> {
            self.inner.last_send_progress()
        }

        fn has_incomplete_incoming_record(&self) -> bool {
            self.inner.has_incomplete_incoming_record()
        }
    }

    /// The production upgrade path — SSLRequest, then `tls::handshake` — over
    /// a socket that blocks the client's FINAL handshake flight: once the
    /// scripted peer has raised `flight_sent` (it has written its own flight),
    /// at most four further client bytes are permitted.
    struct BlockedFinalFlightUpgrade {
        ca: CertificateInput,
        flight_sent: Arc<AtomicBool>,
    }

    impl WithSocket for BlockedFinalFlightUpgrade {
        type Output = sqlx_core::Result<Box<dyn Socket>>;

        async fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
            let mut socket = BlockFinalFlight {
                inner: socket,
                flight_sent: self.flight_sent,
                budget: 4,
            };

            // SSLRequest: length 8, code 80877103; the scripted peer answers
            // `S` and starts TLS.
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

    /// Codex P2 (second look on 93e469ab): rustls can clear
    /// `is_handshaking()` while the client's FINAL handshake flight is still
    /// queued. A partial write then lets `complete_io()` return successfully,
    /// setup reports success at ~192 ms, and the rest of the flight is
    /// written by the first query's flush — which times out at ~443 ms with a
    /// 250 ms bound, OUTSIDE the setup deadline. Native-tls stayed inside
    /// setup (~254 ms), which is the semantics this witness restores.
    ///
    /// The peer holds its own flight for three quarters of the bound, then
    /// raises `flight_sent` and lets at most FOUR further client bytes cross
    /// before blocking writes forever. Setup must stay inside its deadline
    /// until the queued handshake output drains: the deadline, not a later
    /// flush, is what ends this connection.
    #[test]
    fn a_blocked_final_handshake_flight_cannot_escape_the_setup_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let flight_sent = Arc::new(AtomicBool::new(false));
        let peer_sent = flight_sent.clone();
        let server = thread::spawn(move || {
            let mut tcp = listener.accept().unwrap().0;
            let mut request = [0u8; 8];
            tcp.read_exact(&mut request).unwrap();
            assert_eq!(&request[4..], &80877103_u32.to_be_bytes());
            tcp.write_all(b"S").unwrap();
            tcp.flush().unwrap();

            // Hold the ClientHello unanswered for three quarters of the
            // bound, then send the server flight.
            thread::sleep(BOUND * 3 / 4);
            let mut conn = rustls::ServerConnection::new(server_config()).unwrap();
            loop {
                match conn.read_tls(&mut tcp) {
                    Ok(0) => return,
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
                    Err(_) => return,
                }
                if conn.process_new_packets().is_err() {
                    return;
                }
                if conn.wants_write() {
                    // Raised BEFORE the bytes leave: the client cannot write
                    // its final flight until it has processed this one, so
                    // the injection covers exactly that flight.
                    peer_sent.store(true, Ordering::SeqCst);
                    while conn.wants_write() {
                        if conn.write_tls(&mut tcp).is_err() {
                            return;
                        }
                    }
                    break;
                }
            }
            // Keep the connection OPEN and silent: a close would look like
            // EOF, not like the write backpressure under test.
            thread::sleep(Duration::from_secs(2));
        });

        let outcome = test_block_on(async {
            let ca = CertificateInput::File(format!("{CERT_DIR}/ca.crt").into());
            let started = Instant::now();
            let outcome = timeout(
                BOUND,
                connect_tcp(
                    "127.0.0.1",
                    port,
                    BlockedFinalFlightUpgrade { ca, flight_sent },
                ),
            )
            .await;
            (started.elapsed(), outcome)
        });
        let (elapsed, outcome) = outcome;
        match outcome {
            Ok(Ok(_)) => panic!(
                "TLS setup reported success at {elapsed:?} while the final handshake \
                 flight was still blocked; negotiation escaped the setup deadline"
            ),
            Ok(Err(error)) => {
                panic!("the handshake surfaced a non-deadline error at {elapsed:?}: {error:?}")
            }
            Err(_) => assert!(
                elapsed < BOUND + BOUND / 2,
                "the setup deadline fired at {elapsed:?}, after the bound"
            ),
        }
        server.join().unwrap();
    }

    /// Codex P1 / messaging's required witness: a COMPLETE TLS control record
    /// (a TLS 1.3 KeyUpdate) arriving on an idle LISTEN connection is
    /// keep-alive traffic, not the start of a message. It extends the
    /// inactivity deadline (the transport is alive) but must not arm it, so
    /// the listener survives well past the bound and still receives the next
    /// real notification. Before the fix the control record armed the wait
    /// and poisoned the connection one bound later.
    ///
    /// The client backend is whichever the feature set selected — this witness
    /// runs on tokio/rustls, async-io/rustls and tokio/native-tls; the peer is
    /// a rustls SERVER in every case.
    #[test]
    fn a_complete_tls_control_record_does_not_arm_the_idle_listener_bound() {
        let (options, server) = listener(|mut peer| {
            peer.startup();
            assert_eq!(peer.read_frame().0, b'Q'); // LISTEN
            peer.write_plain(&[message(b'C', b"LISTEN\0"), message(b'Z', b"I")].concat());
            // One COMPLETE control record, sealed into its own TLS record.
            // `refresh_traffic_keys` errs on a version before TLS 1.3, so the
            // expect is also the assertion that a KeyUpdate is possible at all.
            peer.conn
                .refresh_traffic_keys()
                .expect("a TLS 1.3 KeyUpdate control record");
            while peer.conn.wants_write() {
                peer.conn.write_tls(&mut peer.tcp).unwrap();
            }
            peer.tcp.flush().unwrap();

            // Silence for EIGHT bounds — twice the client's four-bound floor,
            // so host jitter in either process cannot cross the assertion. A
            // wait armed by the control record would already be poisoned one
            // bound in. Then a real, well-formed NotificationResponse (pid,
            // channel, empty payload), which the same session must deliver.
            thread::sleep(BOUND * 8);
            let mut payload = vec![0, 0, 0, 8];
            payload.extend_from_slice(b"chan\0");
            payload.push(0);
            peer.write_plain(&message(b'A', &payload));
            thread::sleep(Duration::from_secs(2));
        });

        test_block_on(async {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(WATCHDOG)
                .connect_lazy_with(options);
            let mut listener = PgListener::connect_with(&pool).await.unwrap();
            listener.listen("chan").await.unwrap();
            listener.eager_reconnect(false);

            let started = Instant::now();
            let notification = timeout(WATCHDOG, listener.try_recv())
                .await
                .expect("a control record must not poison the idle wait")
                .expect("a poisoned connection is not a query error");
            let notification =
                notification.expect("the notification after the control record must arrive");
            assert_eq!(notification.channel(), "chan");
            assert!(
                started.elapsed() >= BOUND * 4,
                "the wait returned after {:?}, before the peer's silence ended",
                started.elapsed()
            );
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

                let (record_a, wire_len) = record_a.recv_timeout(WATCHDOG).expect("server lengths");
                assert!(record_a > 0 && wire_len > record_a);

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

    /// What the bootstrap's reads pulled beyond the LISTEN response record.
    /// A read-ahead backend (rustls) pulls the next record's prefix; a
    /// record-at-a-time backend (OpenSSL under native-tls) stops at the
    /// record boundary and the prefix arrives during the idle wait instead.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum EntryWindow {
        Prefetched,
        NotPrefetched,
    }

    fn classify_entry_window(boot: &Bootstrapped, record_a: usize, wire_len: usize) -> EntryWindow {
        let pulled = boot.after_listen - boot.after_startup;
        if pulled > record_a {
            assert_eq!(
                pulled, wire_len,
                "a read-ahead backend pulls the whole single write"
            );
            EntryWindow::Prefetched
        } else {
            assert_eq!(
                pulled, record_a,
                "a record-at-a-time backend stops exactly at the response record's boundary"
            );
            EntryWindow::NotPrefetched
        }
    }

    /// The entry window, asserted on the raw socket: the read that COMPLETES
    /// the LISTEN exchange may also pull the first half of the NEXT record,
    /// and the idle wait that follows has empty plaintext and no NEW transport
    /// progress since that read. The bound must fire at ONE bound measured
    /// from the LAST RECEIVED BYTE — the original anchor, not a fresh one from
    /// the wait's entry. On old code the prefetched byte is invisible and the
    /// wait runs into the outer watchdog; a record-at-a-time backend delivers
    /// the prefix during the wait and the transport-progress rule bounds it,
    /// which the same assertion pins.
    #[test]
    fn a_record_at_the_entry_window_bounds_the_next_idle_wait() {
        let (port, lengths, stop, server) = prefetch_peer();
        let reads = Arc::new(RawReads::default());

        test_block_on(async {
            let boot = timeout(WATCHDOG, bootstrap(port, reads.clone()))
                .await
                .expect("bootstrap must complete");
            let (record_a, wire_len) = lengths.recv_timeout(WATCHDOG).expect("server lengths");
            let window = classify_entry_window(&boot, record_a, wire_len);

            let mut socket = boot.socket;
            socket.set_io_timeout(Some(BOUND));

            let error = timeout(WATCHDOG, socket.try_read_idle(wait_for_a_byte))
                .await
                .expect("a record in flight must arm the bound")
                .unwrap_err();
            let fired_at = Instant::now();
            assert_timeout(error);

            let last_byte = reads.last().expect("a received byte must be recorded");
            let from_byte = fired_at.duration_since(last_byte);
            assert!(
                from_byte >= BOUND,
                "the wait fired before the received byte's bound ({window:?}): {from_byte:?}"
            );
            assert!(
                from_byte < BOUND + Duration::from_millis(500),
                "the deadline was renewed past the received byte ({window:?}): {from_byte:?}"
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

    /// The same record, with a DELAYED entry: the wait starts half a bound
    /// after the response. When the prefix was already pulled, the deadline
    /// must still fall at ITS byte's bound, not restart from the entry (a
    /// renewed full bound would let a dead peer hold the connection for up to
    /// two bounds). A record-at-a-time backend has no earlier byte to renew:
    /// the prefix arrives at the entry, so exactly one bound from there is the
    /// correct answer and the assertion says so.
    #[test]
    fn a_delayed_idle_wait_is_not_granted_a_fresh_bound() {
        let (port, lengths, stop, server) = prefetch_peer();
        let reads = Arc::new(RawReads::default());

        test_block_on(async {
            let boot = timeout(WATCHDOG, bootstrap(port, reads.clone()))
                .await
                .expect("bootstrap must complete");
            let (record_a, wire_len) = lengths.recv_timeout(WATCHDOG).expect("server lengths");
            let window = classify_entry_window(&boot, record_a, wire_len);

            let mut socket = boot.socket;
            socket.set_io_timeout(Some(BOUND));

            // Enter the wait half a bound late.
            sleep(BOUND / 2).await;

            let started = Instant::now();
            let error = timeout(WATCHDOG, socket.try_read_idle(wait_for_a_byte))
                .await
                .expect("a record in flight must arm the bound")
                .unwrap_err();
            let elapsed = started.elapsed();
            let fired_at = Instant::now();
            assert_timeout(error);

            let last_byte = reads.last().expect("a received byte must be recorded");
            let from_byte = fired_at.duration_since(last_byte);
            assert!(
                from_byte >= BOUND,
                "the deadline fell before the received byte's bound ({window:?}): {from_byte:?}"
            );
            assert!(
                from_byte < BOUND + Duration::from_millis(500),
                "the deadline was renewed past the received byte ({window:?}): {from_byte:?}"
            );
            match window {
                EntryWindow::Prefetched => assert!(
                    elapsed < BOUND * 3 / 4,
                    "the delayed entry was granted a fresh bound: waited {elapsed:?}"
                ),
                EntryWindow::NotPrefetched => assert!(
                    elapsed >= BOUND,
                    "the prefix arrived at the entry; one bound from there: waited {elapsed:?}"
                ),
            }
        });
        let _ = stop.send(());
        server.join().unwrap();
    }

    /// billing-7t7r P2 (grok): an OUTGOING wait is extended only by OUTBOUND
    /// transport progress, on every backend. A peer that stops reading while
    /// streaming us data must not hold a blocked flush open: the flush fires
    /// one bound after the last byte the transport ACCEPTED from us.
    ///
    /// The peer answers the LISTEN exchange, then never reads again and sends
    /// one TLS 1.3 KeyUpdate control record every quarter bound for three
    /// bounds. The client fills its outbound window with 64 KiB writes until
    /// one wait blocks; that wait is the one under test. Before this fix
    /// rustls's `complete_io` read those records inside the blocked wait and
    /// re-anchored the bound to each inbound byte, pushing the timeout past
    /// the end of the stream while native-tls and plaintext already ignored
    /// them — the mismatch this witness makes uniform.
    #[test]
    fn inbound_ciphertext_does_not_extend_a_blocked_outbound_flush() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (stop, stopped) = std::sync::mpsc::channel::<()>();
        let server = thread::spawn(move || {
            let mut peer = TlsPeer::accept(listener.accept().unwrap().0);
            peer.startup();
            assert_eq!(peer.read_frame().0, b'Q'); // LISTEN
            peer.write_plain(&[message(b'C', b"LISTEN\0"), message(b'Z', b"I")].concat());
            // From here the peer only SENDS, and sends CONTROL records: a TLS
            // 1.3 KeyUpdate adds no plaintext, so the client's session keeps
            // reading the wire while its outbound window is full — the
            // condition under test. (Application data would make rustls stop
            // reading once its plaintext buffer is non-empty.) Once the
            // client has concluded (it closes with unread data, so its close
            // is an RST), a send fails; the witness has already been decided,
            // so the peer stops instead of failing the test.
            for _ in 0..12 {
                // Errs before TLS 1.3, so this is also the assertion that the
                // control record is possible at all.
                peer.conn
                    .refresh_traffic_keys()
                    .expect("a TLS 1.3 KeyUpdate");
                while peer.conn.wants_write() {
                    if peer.conn.write_tls(&mut peer.tcp).is_err() {
                        return;
                    }
                }
                thread::sleep(BOUND / 4);
            }
            let _ = stopped.recv_timeout(Duration::from_secs(10));
        });

        test_block_on(async {
            let reads = Arc::new(RawReads::default());
            let boot = timeout(WATCHDOG, bootstrap(port, reads))
                .await
                .expect("bootstrap must complete");
            let mut socket = boot.socket;
            socket.set_io_timeout(Some(BOUND));
            let chunk = vec![0u8; 64 * 1024];

            let (error, waited) = timeout(WATCHDOG, async {
                let mut last_accepted = Instant::now();
                loop {
                    socket.write(chunk.as_slice()).expect("buffer one chunk");
                    match socket.flush().await {
                        Ok(()) => last_accepted = Instant::now(),
                        Err(error) => return (error, last_accepted.elapsed()),
                    }
                }
            })
            .await
            .expect("the outbound window must fill and the bound must fire");

            assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
            assert!(
                error
                    .get_ref()
                    .is_some_and(|inner| inner.is::<sqlx_core::error::SocketTimedOut>()),
                "the wait must carry the configured bound: {error:?}"
            );
            assert!(
                waited >= BOUND,
                "the flush failed before one bound of outbound silence: {waited:?}"
            );
            assert!(
                waited < BOUND + BOUND / 2,
                "an inbound record extended the outgoing wait ({waited:?}); outgoing waits \
                 count outbound progress only"
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
