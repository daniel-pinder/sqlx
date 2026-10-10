use std::fmt::{self, Debug, Formatter};
use std::future::{self, Future};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::sync::AsyncSemaphoreReleaser;

use crate::connection::Connection;
use crate::database::Database;
use crate::error::Error;

use super::inner::{is_beyond_max_lifetime, DecrementSizeGuard, PoolInner};
use crate::pool::options::PoolConnectionMetadata;

const CLOSE_ON_DROP_TIMEOUT: Duration = Duration::from_secs(5);

/// Upper bound for one step of the on-release connection check.
///
/// Neither `Connection::ping()` nor a graceful `Connection::close()` carries a
/// timeout of its own: the Postgres `ping()` is a raw socket read loop until
/// `ReadyForQuery` (`sqlx-postgres/src/connection/mod.rs`), and `close()` waits
/// for the terminator round trip. An unresponsive server would otherwise park
/// the releasing task forever — and this runs in a spawned task, so nothing
/// else notices. Named for the ping because that is the step this bound was
/// added for; the `close()` calls on the same path share it, since they are the
/// same "give the server this long, then move on" budget. 5s matches the
/// neighbouring `CLOSE_ON_DROP_TIMEOUT`.
///
/// Accepted trade: the bound measures from the executor giving up, not from the
/// server going idle, so a connection can be hard-closed while a statement is
/// still running server-side. A caller that drops a query future mid-flight
/// leaves the server draining it, and five seconds later this bound closes the
/// connection and the server aborts what was left. The consumer facts the trade
/// rests on: the admin path, where the long queries are, caps them at 10s via
/// `BILLING_ADMIN_STATEMENT_TIMEOUT_SECONDS` (`billing` `src/config.rs:665-683`);
/// the job-hook queries set their own clamped `statement_timeout`
/// (`billing` `src/jobs.rs:1139-1150`); and the HTTP application path sets
/// none — that last case is what this bound is for. Unbounded is not the
/// alternative to weigh: it turns one slow query into a pool slot that is never
/// reclaimed.
const RELEASE_PING_TIMEOUT: Duration = Duration::from_secs(5);

/// A connection managed by a [`Pool`][crate::pool::Pool].
///
/// Will be returned to the pool on-drop.
pub struct PoolConnection<DB: Database> {
    live: Option<Live<DB>>,
    close_on_drop: bool,
    pub(crate) pool: Arc<PoolInner<DB>>,
}

pub(super) struct Live<DB: Database> {
    pub(super) raw: DB::Connection,
    pub(super) created_at: Instant,
}

pub(super) struct Idle<DB: Database> {
    pub(super) live: Live<DB>,
    pub(super) idle_since: Instant,
}

/// RAII wrapper for connections being handled by functions that may drop them
pub(super) struct Floating<DB: Database, C> {
    pub(super) inner: C,
    pub(super) guard: DecrementSizeGuard<DB>,
}

const EXPECT_MSG: &str = "BUG: inner connection already taken!";

impl<DB: Database> Debug for PoolConnection<DB> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // TODO: Show the type name of the connection ?
        f.debug_struct("PoolConnection").finish()
    }
}

impl<DB: Database> Deref for PoolConnection<DB> {
    type Target = DB::Connection;

    fn deref(&self) -> &Self::Target {
        &self.live.as_ref().expect(EXPECT_MSG).raw
    }
}

impl<DB: Database> DerefMut for PoolConnection<DB> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.live.as_mut().expect(EXPECT_MSG).raw
    }
}

impl<DB: Database> AsRef<DB::Connection> for PoolConnection<DB> {
    fn as_ref(&self) -> &DB::Connection {
        self
    }
}

impl<DB: Database> AsMut<DB::Connection> for PoolConnection<DB> {
    fn as_mut(&mut self) -> &mut DB::Connection {
        self
    }
}

impl<DB: Database> PoolConnection<DB> {
    /// Close this connection, allowing the pool to open a replacement.
    ///
    /// Equivalent to calling [`.detach()`] then [`.close()`], but the connection permit is retained
    /// for the duration so that the pool may not exceed `max_connections`.
    ///
    /// [`.detach()`]: PoolConnection::detach
    /// [`.close()`]: Connection::close
    pub async fn close(mut self) -> Result<(), Error> {
        let floating = self.take_live().float(self.pool.clone());
        floating.inner.raw.close().await
    }

    /// Close this connection on-drop, instead of returning it to the pool.
    ///
    /// May be used in cases where waiting for the [`.close()`][Self::close] call
    /// to complete is unacceptable, but you still want the connection to be closed gracefully
    /// so that the server can clean up resources.
    #[inline(always)]
    pub fn close_on_drop(&mut self) {
        self.close_on_drop = true;
    }

    /// Detach this connection from the pool, allowing it to open a replacement.
    ///
    /// Note that if your application uses a single shared pool, this
    /// effectively lets the application exceed the [`max_connections`] setting.
    ///
    /// If [`min_connections`] is nonzero, a task will be spawned to replace this connection.
    ///
    /// If you want the pool to treat this connection as permanently checked-out,
    /// use [`.leak()`][Self::leak] instead.
    ///
    /// [`max_connections`]: crate::pool::PoolOptions::max_connections
    /// [`min_connections`]: crate::pool::PoolOptions::min_connections
    pub fn detach(mut self) -> DB::Connection {
        self.take_live().float(self.pool.clone()).detach()
    }

    /// Detach this connection from the pool, treating it as permanently checked-out.
    ///
    /// This effectively will reduce the maximum capacity of the pool by 1 every time it is used.
    ///
    /// If you don't want to impact the pool's capacity, use [`.detach()`][Self::detach] instead.
    pub fn leak(mut self) -> DB::Connection {
        self.take_live().raw
    }

    fn take_live(&mut self) -> Live<DB> {
        self.live.take().expect(EXPECT_MSG)
    }

    /// Test the connection to make sure it is still live before returning it to the pool.
    ///
    /// This effectively runs the drop handler eagerly instead of spawning a task to do it.
    #[doc(hidden)]
    pub fn return_to_pool(&mut self) -> impl Future<Output = ()> + Send + 'static {
        // float the connection in the pool before we move into the task
        // in case the returned `Future` isn't executed, like if it's spawned into a dying runtime
        // https://github.com/launchbadge/sqlx/issues/1396
        // Type hints seem to be broken by `Option` combinators in IntelliJ Rust right now (6/22).
        let floating: Option<Floating<DB, Live<DB>>> =
            self.live.take().map(|live| live.float(self.pool.clone()));

        let pool = self.pool.clone();

        async move {
            let returned_to_pool = if let Some(floating) = floating {
                floating.return_to_pool().await
            } else {
                false
            };

            if !returned_to_pool {
                pool.min_connections_maintenance(None).await;
            }
        }
    }

    fn take_and_close(&mut self) -> impl Future<Output = ()> + Send + 'static {
        // float the connection in the pool before we move into the task
        // in case the returned `Future` isn't executed, like if it's spawned into a dying runtime
        // https://github.com/launchbadge/sqlx/issues/1396
        // Type hints seem to be broken by `Option` combinators in IntelliJ Rust right now (6/22).
        let floating = self.live.take().map(|live| live.float(self.pool.clone()));

        let pool = self.pool.clone();

        async move {
            if let Some(floating) = floating {
                // Don't hold the connection forever if it hangs while trying to close
                crate::rt::timeout(CLOSE_ON_DROP_TIMEOUT, floating.close())
                    .await
                    .ok();
            }

            pool.min_connections_maintenance(None).await;
        }
    }
}

impl<'c, DB: Database> crate::acquire::Acquire<'c> for &'c mut PoolConnection<DB> {
    type Database = DB;

    type Connection = &'c mut <DB as Database>::Connection;

    #[inline]
    fn acquire(self) -> futures_core::future::BoxFuture<'c, Result<Self::Connection, Error>> {
        Box::pin(future::ready(Ok(&mut **self)))
    }

    #[inline]
    fn begin(
        self,
    ) -> futures_core::future::BoxFuture<'c, Result<crate::transaction::Transaction<'c, DB>, Error>>
    {
        crate::transaction::Transaction::begin(&mut **self, None)
    }
}

/// Returns the connection to the [`Pool`][crate::pool::Pool] it was checked-out from.
impl<DB: Database> Drop for PoolConnection<DB> {
    fn drop(&mut self) {
        if self.close_on_drop {
            crate::rt::spawn(self.take_and_close());
            return;
        }

        // We still need to spawn a task to maintain `min_connections`.
        if self.live.is_some() || self.pool.options.min_connections > 0 {
            crate::rt::spawn(self.return_to_pool());
        }
    }
}

impl<DB: Database> Live<DB> {
    pub fn float(self, pool: Arc<PoolInner<DB>>) -> Floating<DB, Self> {
        Floating {
            inner: self,
            // create a new guard from a previously leaked permit
            guard: DecrementSizeGuard::new_permit(pool),
        }
    }

    pub fn into_idle(self) -> Idle<DB> {
        Idle {
            live: self,
            idle_since: Instant::now(),
        }
    }
}

impl<DB: Database> Deref for Idle<DB> {
    type Target = Live<DB>;

    fn deref(&self) -> &Self::Target {
        &self.live
    }
}

impl<DB: Database> DerefMut for Idle<DB> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.live
    }
}

impl<DB: Database> Floating<DB, Live<DB>> {
    pub fn new_live(conn: DB::Connection, guard: DecrementSizeGuard<DB>) -> Self {
        Self {
            inner: Live {
                raw: conn,
                created_at: Instant::now(),
            },
            guard,
        }
    }

    pub fn reattach(self) -> PoolConnection<DB> {
        let Floating { inner, guard } = self;

        let pool = Arc::clone(&guard.pool);

        guard.cancel();
        PoolConnection {
            live: Some(inner),
            close_on_drop: false,
            pool,
        }
    }

    pub fn release(self) {
        self.guard.pool.clone().release(self);
    }

    /// Return the connection to the pool.
    ///
    /// Returns `true` if the connection was successfully returned, `false` if it was closed.
    async fn return_to_pool(mut self) -> bool {
        // Immediately close the connection.
        if self.guard.pool.is_closed() {
            self.close_bounded().await;
            return false;
        }

        // If the connection is beyond max lifetime, close the connection and
        // immediately create a new connection
        if is_beyond_max_lifetime(&self.inner, &self.guard.pool.options) {
            self.close_bounded().await;
            return false;
        }

        if let Some(test) = &self.guard.pool.options.after_release {
            let meta = self.metadata();
            match (test)(&mut self.inner.raw, meta).await {
                Ok(true) => (),
                Ok(false) => {
                    self.close_bounded().await;
                    return false;
                }
                Err(error) => {
                    tracing::warn!(%error, "error from `after_release`");
                    // Connection is broken, don't try to gracefully close as
                    // something weird might happen.
                    //
                    // Drop rather than `close_hard().await`: `close_hard()`
                    // reaches `BufferedSocket::shutdown()`, which starts with
                    // `flush()` on the same write buffer that is unflushable
                    // whenever the peer has stopped reading
                    // (`sqlx-core/src/net/socket/buffered.rs:137`), so awaiting
                    // it here would park with the permit still held. Dropping
                    // closes the socket and releases the size guard
                    // synchronously, with no await to block on.
                    drop(self);
                    return false;
                }
            }
        }

        // test the connection on-release to ensure it is still viable,
        // and flush anything time-sensitive like transaction rollbacks
        // if an Executor future/stream is dropped during an `.await` call, the connection
        // is likely to be left in an inconsistent state, in which case it should not be
        // returned to the pool; also of course, if it was dropped due to an error
        // this is simply a band-aid as SQLx-next connections should be able
        // to recover from cancellations
        //
        // `ping()` is an un-timed read loop, so bound it: an unresponsive server
        // must not park the releasing task here forever.
        let ping_failed = match crate::rt::timeout(RELEASE_PING_TIMEOUT, self.raw.ping()).await {
            Ok(Ok(())) => false,
            Ok(Err(error)) => {
                tracing::warn!(
                    %error,
                    "error occurred while testing the connection on-release",
                );

                true
            }
            Err(_elapsed) => {
                tracing::warn!(
                    "on-release connection test timed out after {:?}",
                    RELEASE_PING_TIMEOUT,
                );

                true
            }
        };

        if ping_failed {
            // Connection is broken, don't try to gracefully close.
            //
            // In particular, do NOT `close_hard().await` here. `close_hard()`
            // ends in `BufferedSocket::shutdown()`, which begins with
            // `flush()` on the very write buffer `ping()` just failed to drain
            // (`sqlx-core/src/net/socket/buffered.rs:137`): against a peer that
            // ACKs but stops reading, that flush parks forever with the permit
            // still held, so the bound would only have moved the hang from
            // `ping()` to the close. Dropping the `Floating` closes the socket
            // and releases the size guard synchronously — no await at all —
            // which is also what "don't try to gracefully close" means here.
            drop(self);
            false
        } else {
            // if the connection is still viable, release it to the pool
            self.release();
            true
        }
    }

    /// Gracefully close the connection, but never wait on the server for longer
    /// than [`RELEASE_PING_TIMEOUT`].
    ///
    /// `Floating::close()` discards the error from `Connection::close()`, so an
    /// abandoned close is treated the same way: the timeout future is dropped,
    /// which drops the connection and, with it, the size guard — releasing the
    /// pool slot. That matches the "proceed on a failed close" semantics the rest
    /// of this file already uses (see `PoolConnection::take_and_close`, which
    /// bounds the same call with `CLOSE_ON_DROP_TIMEOUT`).
    async fn close_bounded(self) {
        crate::rt::timeout(RELEASE_PING_TIMEOUT, self.close())
            .await
            .ok();
    }

    pub async fn close(self) {
        // This isn't used anywhere that we care about the return value
        let _ = self.inner.raw.close().await;

        // `guard` is dropped as intended
    }

    pub fn detach(self) -> DB::Connection {
        self.inner.raw
    }

    pub fn into_idle(self) -> Floating<DB, Idle<DB>> {
        Floating {
            inner: self.inner.into_idle(),
            guard: self.guard,
        }
    }

    pub fn metadata(&self) -> PoolConnectionMetadata {
        PoolConnectionMetadata {
            age: self.created_at.elapsed(),
            idle_for: Duration::ZERO,
        }
    }
}

impl<DB: Database> Floating<DB, Idle<DB>> {
    pub fn from_idle(
        idle: Idle<DB>,
        pool: Arc<PoolInner<DB>>,
        permit: AsyncSemaphoreReleaser<'_>,
    ) -> Self {
        Self {
            inner: idle,
            guard: DecrementSizeGuard::from_permit(pool, permit),
        }
    }

    pub async fn ping(&mut self) -> Result<(), Error> {
        self.live.raw.ping().await
    }

    pub fn into_live(self) -> Floating<DB, Live<DB>> {
        Floating {
            inner: self.inner.live,
            guard: self.guard,
        }
    }

    pub async fn close(self) -> DecrementSizeGuard<DB> {
        if let Err(error) = self.inner.live.raw.close().await {
            tracing::debug!(%error, "error occurred while closing the pool connection");
        }
        self.guard
    }

    pub async fn close_hard(self) -> DecrementSizeGuard<DB> {
        let _ = self.inner.live.raw.close_hard().await;

        self.guard
    }

    pub fn metadata(&self) -> PoolConnectionMetadata {
        // Use a single `now` value for consistency.
        let now = Instant::now();

        PoolConnectionMetadata {
            // NOTE: the receiver is the later `Instant` and the arg is the earlier
            // https://github.com/launchbadge/sqlx/issues/1912
            age: now.saturating_duration_since(self.created_at),
            idle_for: now.saturating_duration_since(self.idle_since),
        }
    }
}

impl<DB: Database, C> Deref for Floating<DB, C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<DB: Database, C> DerefMut for Floating<DB, C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
