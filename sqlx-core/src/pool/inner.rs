use super::connection::{Floating, Idle, Live};
use crate::connection::ConnectOptions;
use crate::connection::Connection;
use crate::database::Database;
use crate::error::Error;
use crate::pool::{deadline_as_timeout, CloseEvent, Pool, PoolOptions};
use crossbeam_queue::ArrayQueue;

use crate::sync::{AsyncSemaphore, AsyncSemaphoreReleaser};

use std::cmp;
use std::future::{self, Future};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::task::Poll;

use crate::logger::private_level_filter_to_trace_level;
use crate::pool::options::PoolConnectionMetadata;
use crate::private_tracing_dynamic_event;
use futures_util::FutureExt;
use std::time::{Duration, Instant};
use tracing::Level;

/// Upper bound on the two unbounded waits in `PoolInner::close()`.
///
/// `close()` first joins every checked-out connection — a caller that never
/// returns its `PoolConnection` would otherwise block shutdown forever, since
/// the pool has no handle with which to force-close it — and then serially
/// closes the idle connections, each of which is an un-timed round trip to a
/// possibly-wedged server. One deadline is computed once and shared by both, so
/// the whole call is bounded at this value rather than at N x this value.
///
/// A const rather than a `PoolOptions` field: this is a shutdown-liveness
/// guard, not a per-pool tuning knob, and a new field would change the public
/// `PoolOptions` API for a fork patch. 5s matches `CLOSE_ON_DROP_TIMEOUT`.
const CLOSE_JOIN_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct PoolInner<DB: Database> {
    pub(super) connect_options: RwLock<Arc<<DB::Connection as Connection>::Options>>,
    pub(super) idle_conns: ArrayQueue<Idle<DB>>,
    pub(super) semaphore: AsyncSemaphore,
    pub(super) size: AtomicU32,
    pub(super) num_idle: AtomicUsize,
    is_closed: AtomicBool,
    pub(super) on_closed: event_listener::Event,
    pub(super) options: PoolOptions<DB>,
    pub(crate) acquire_time_level: Option<Level>,
    pub(crate) acquire_slow_level: Option<Level>,
}

impl<DB: Database> PoolInner<DB> {
    pub(super) fn new_arc(
        options: PoolOptions<DB>,
        connect_options: <DB::Connection as Connection>::Options,
    ) -> Arc<Self> {
        let capacity = options.max_connections as usize;

        let semaphore_capacity = if let Some(parent) = &options.parent_pool {
            assert!(options.max_connections <= parent.options().max_connections);
            assert_eq!(options.fair, parent.options().fair);
            // The child pool must steal permits from the parent
            0
        } else {
            capacity
        };

        let pool = Self {
            connect_options: RwLock::new(Arc::new(connect_options)),
            idle_conns: ArrayQueue::new(capacity),
            semaphore: AsyncSemaphore::new(options.fair, semaphore_capacity),
            size: AtomicU32::new(0),
            num_idle: AtomicUsize::new(0),
            is_closed: AtomicBool::new(false),
            on_closed: event_listener::Event::new(),
            acquire_time_level: private_level_filter_to_trace_level(options.acquire_time_level),
            acquire_slow_level: private_level_filter_to_trace_level(options.acquire_slow_level),
            options,
        };

        let pool = Arc::new(pool);

        spawn_maintenance_tasks(&pool);

        pool
    }

    pub(super) fn size(&self) -> u32 {
        self.size.load(Ordering::Acquire)
    }

    pub(super) fn num_idle(&self) -> usize {
        // We don't use `self.idle_conns.len()` as it waits for the internal
        // head and tail pointers to stop changing for a moment before calculating the length,
        // which may take a long time at high levels of churn.
        //
        // By maintaining our own atomic count, we avoid that issue entirely.
        self.num_idle.load(Ordering::Acquire)
    }

    pub(super) fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Acquire)
    }

    fn mark_closed(&self) {
        self.is_closed.store(true, Ordering::Release);
        self.on_closed.notify(usize::MAX);
    }

    pub(super) fn close(self: &Arc<Self>) -> impl Future<Output = ()> + '_ {
        self.mark_closed();

        async move {
            // For child pools, we need to acquire permits we actually have rather than
            // max_connections
            let permits_to_acquire = if self.options.parent_pool.is_some() {
                // Child pools start with 0 permits, so we acquire based on current size
                self.size()
            } else {
                // Parent pools can acquire all max_connections permits
                self.options.max_connections
            };

            // `mark_closed()` above is what makes it safe for a caller to drop a
            // checked-out connection at any moment, so waiting on those
            // checkouts here is a courtesy, not a prerequisite. It is also the
            // only thing that can make `close()` take unbounded time when a
            // caller never gives its connection back. Bound it; on expiry carry
            // straight on to the drain — there is nothing here to force close.
            //
            // The permits, when we do get them, stay bound for the rest of
            // `close()` exactly as they did before, so the drain still runs with
            // every permit held.
            let deadline = Instant::now() + CLOSE_JOIN_TIMEOUT;

            let _permits = crate::rt::timeout(
                CLOSE_JOIN_TIMEOUT,
                self.semaphore.acquire(permits_to_acquire),
            )
            .await
            .inspect_err(|_elapsed| {
                tracing::warn!(
                    "timed out after {:?} waiting for {permits_to_acquire} checked-out \
                     connection permit(s) to be released while closing the pool; proceeding \
                     without joining them",
                    CLOSE_JOIN_TIMEOUT,
                );
            });

            while let Some(idle) = self.idle_conns.pop() {
                // Each `close()` is a serial round trip to a (possibly wedged)
                // server, so they share what is left of the one deadline above
                // instead of each getting a fresh timeout.
                let remaining = deadline.saturating_duration_since(Instant::now());

                if crate::rt::timeout(remaining, idle.live.raw.close())
                    .await
                    .is_err()
                {
                    tracing::warn!(
                        "on-close deadline reached while closing idle pooled \
                         connections; dropping the rest without waiting on them"
                    );

                    // Do not `break`: that would leave connections sitting in
                    // `idle_conns` while `num_idle` and `size` are stored 0
                    // below, so the accounting would lie and the queue entries
                    // would be dropped only when the pool itself is dropped.
                    // `remaining` is already zero and a zero `timeout` still
                    // polls the close future once, so `continue` drops the rest
                    // without re-introducing any waiting.
                    continue;
                }
            }

            self.num_idle.store(0, Ordering::Release);
            self.size.store(0, Ordering::Release);
        }
    }

    pub(crate) fn close_event(&self) -> CloseEvent {
        CloseEvent {
            listener: (!self.is_closed()).then(|| self.on_closed.listen()),
        }
    }

    /// Attempt to pull a permit from `self.semaphore` or steal one from the parent.
    ///
    /// If we steal a permit from the parent but *don't* open a connection,
    /// it should be returned to the parent.
    async fn acquire_permit(self: &Arc<Self>) -> Result<AsyncSemaphoreReleaser<'_>, Error> {
        let parent = self
            .parent()
            // If we're already at the max size, we shouldn't try to steal from the parent.
            // This is just going to cause unnecessary churn in `acquire()`.
            .filter(|_| self.size() < self.options.max_connections);

        let mut acquire_self = pin!(self.semaphore.acquire(1).fuse());
        let mut close_event = pin!(self.close_event());

        if let Some(parent) = parent {
            let mut acquire_parent = pin!(parent.0.semaphore.acquire(1));
            let mut parent_close_event = pin!(parent.0.close_event());

            let mut poll_parent = false;

            future::poll_fn(|cx| {
                if close_event.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Err(Error::PoolClosed));
                }

                if parent_close_event.as_mut().poll(cx).is_ready() {
                    // Propagate the parent's close event to the child.
                    self.mark_closed();
                    return Poll::Ready(Err(Error::PoolClosed));
                }

                if let Poll::Ready(permit) = acquire_self.as_mut().poll(cx) {
                    return Poll::Ready(Ok(permit));
                }

                // Don't try the parent right away.
                if poll_parent {
                    acquire_parent.as_mut().poll(cx).map(Ok)
                } else {
                    poll_parent = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await
        } else {
            close_event.do_until(acquire_self).await
        }
    }

    fn parent(&self) -> Option<&Pool<DB>> {
        self.options.parent_pool.as_ref()
    }

    #[inline]
    pub(super) fn try_acquire(self: &Arc<Self>) -> Option<Floating<DB, Idle<DB>>> {
        if self.is_closed() {
            return None;
        }

        let permit = self.semaphore.try_acquire(1)?;

        self.pop_idle(permit).ok()
    }

    fn pop_idle<'a>(
        self: &'a Arc<Self>,
        permit: AsyncSemaphoreReleaser<'a>,
    ) -> Result<Floating<DB, Idle<DB>>, AsyncSemaphoreReleaser<'a>> {
        if let Some(idle) = self.idle_conns.pop() {
            // Saturating: `close()` stores `num_idle = 0` when it gives up
            // joining checked-out connections (see `CLOSE_JOIN_TIMEOUT`), so a
            // decrement that was already in flight — this pop raced the drain —
            // would otherwise wrap the count to `usize::MAX`. Zero is also the
            // truthful value once the connection has been handed out of the
            // idle set. No behaviour change while the count is non-zero.
            let _ = self
                .num_idle
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    Some(count.saturating_sub(1))
                });

            Ok(Floating::from_idle(idle, (*self).clone(), permit))
        } else {
            Err(permit)
        }
    }

    pub(super) fn release(&self, floating: Floating<DB, Live<DB>>) {
        // `options.after_release` and other checks are in `PoolConnection::return_to_pool()`.

        let Floating { inner: idle, guard } = floating.into_idle();

        if self.idle_conns.push(idle).is_err() {
            panic!("BUG: connection queue overflow in release()");
        }

        // NOTE: we need to make sure we drop the permit *after* we push to the idle queue
        // don't decrease the size
        guard.release_permit();

        self.num_idle.fetch_add(1, Ordering::AcqRel);
    }

    /// Try to atomically increment the pool size for a new connection.
    ///
    /// Returns `Err` if the pool is at max capacity already or is closed.
    pub(super) fn try_increment_size<'a>(
        self: &'a Arc<Self>,
        permit: AsyncSemaphoreReleaser<'a>,
    ) -> Result<DecrementSizeGuard<DB>, AsyncSemaphoreReleaser<'a>> {
        let result = self
            .size
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |size| {
                if self.is_closed() {
                    return None;
                }

                size.checked_add(1)
                    .filter(|size| size <= &self.options.max_connections)
            });

        match result {
            // we successfully incremented the size
            Ok(_) => Ok(DecrementSizeGuard::from_permit((*self).clone(), permit)),
            // the pool is at max capacity or is closed
            Err(_) => Err(permit),
        }
    }

    pub(super) async fn acquire(self: &Arc<Self>) -> Result<Floating<DB, Live<DB>>, Error> {
        if self.is_closed() {
            return Err(Error::PoolClosed);
        }

        let acquire_started_at = Instant::now();
        let deadline = acquire_started_at + self.options.acquire_timeout;

        let acquired = crate::rt::timeout(
            self.options.acquire_timeout,
            async {
                loop {
                    // Handles the close-event internally
                    let permit = self.acquire_permit().await?;


                    // First attempt to pop a connection from the idle queue.
                    let guard = match self.pop_idle(permit) {

                        // Then, check that we can use it...
                        Ok(conn) => match check_idle_conn(conn, &self.options, deadline).await {

                            // All good!
                            Ok(live) => return Ok(live),

                            // if the connection isn't usable for one reason or another,
                            // we get the `DecrementSizeGuard` back to open a new one
                            Err(guard) => guard,
                        },
                        Err(permit) => if let Ok(guard) = self.try_increment_size(permit) {
                            // we can open a new connection
                            guard
                        } else {
                            // This can happen for a child pool that's at its connection limit,
                            // or if the pool was closed between `acquire_permit()` and
                            // `try_increment_size()`.
                            tracing::debug!("woke but was unable to acquire idle connection or open new one; retrying");
                            // If so, we're likely in the current-thread runtime if it's Tokio,
                            // and so we should yield to let any spawned return_to_pool() tasks
                            // execute.
                            crate::rt::yield_now().await;
                            continue;
                        }
                    };

                    // Attempt to connect...
                    return self.connect(deadline, guard).await;
                }
            }
        )
            .await
            .map_err(|_| Error::PoolTimedOut)??;

        let acquired_after = acquire_started_at.elapsed();

        let acquire_slow_level = self
            .acquire_slow_level
            .filter(|_| acquired_after > self.options.acquire_slow_threshold);

        if let Some(level) = acquire_slow_level {
            private_tracing_dynamic_event!(
                target: "sqlx::pool::acquire",
                level,
                acquired_after_secs = acquired_after.as_secs_f64(),
                slow_acquire_threshold_secs = self.options.acquire_slow_threshold.as_secs_f64(),
                "acquired connection, but time to acquire exceeded slow threshold"
            );
        } else if let Some(level) = self.acquire_time_level {
            private_tracing_dynamic_event!(
                target: "sqlx::pool::acquire",
                level,
                acquired_after_secs = acquired_after.as_secs_f64(),
                "acquired connection"
            );
        }

        Ok(acquired)
    }

    pub(super) async fn connect(
        self: &Arc<Self>,
        deadline: Instant,
        guard: DecrementSizeGuard<DB>,
    ) -> Result<Floating<DB, Live<DB>>, Error> {
        if self.is_closed() {
            return Err(Error::PoolClosed);
        }

        let mut backoff = Duration::from_millis(10);
        let max_backoff = deadline_as_timeout(deadline)? / 5;

        loop {
            let timeout = deadline_as_timeout(deadline)?;

            // clone the connect options arc so it can be used without holding the RwLockReadGuard
            // across an async await point
            let connect_options = self
                .connect_options
                .read()
                .expect("write-lock holder panicked")
                .clone();

            // result here is `Result<Result<C, Error>, TimeoutError>`
            // if this block does not return, sleep for the backoff timeout and try again
            match crate::rt::timeout(timeout, connect_options.connect()).await {
                // successfully established connection
                Ok(Ok(mut raw)) => {
                    // See comment on `PoolOptions::after_connect`
                    let meta = PoolConnectionMetadata {
                        age: Duration::ZERO,
                        idle_for: Duration::ZERO,
                    };

                    let res = if let Some(callback) = &self.options.after_connect {
                        callback(&mut raw, meta).await
                    } else {
                        Ok(())
                    };

                    match res {
                        Ok(()) => return Ok(Floating::new_live(raw, guard)),
                        Err(error) => {
                            tracing::error!(%error, "error returned from after_connect");
                            // The connection is broken, don't try to close nicely.
                            let _ = raw.close_hard().await;

                            // Fall through to the backoff.
                        }
                    }
                }

                // an IO error while connecting is assumed to be the system starting up
                Ok(Err(Error::Io(e))) if e.kind() == std::io::ErrorKind::ConnectionRefused => (),

                // We got a transient database error, retry.
                Ok(Err(Error::Database(error))) if error.is_transient_in_connect_phase() => (),

                // Any other error while connection should immediately
                // terminate and bubble the error up
                Ok(Err(e)) => return Err(e),

                // timed out
                Err(_) => return Err(Error::PoolTimedOut),
            }

            // If the connection is refused, wait in exponentially
            // increasing steps for the server to come up,
            // capped by a factor of the remaining time until the deadline
            crate::rt::sleep(backoff).await;
            backoff = cmp::min(backoff * 2, max_backoff);
        }
    }

    /// Try to maintain `min_connections`, returning any errors (including `PoolTimedOut`).
    pub async fn try_min_connections(self: &Arc<Self>, deadline: Instant) -> Result<(), Error> {
        while self.size() < self.options.min_connections {
            // Don't wait for a semaphore permit.
            //
            // If no extra permits are available then we shouldn't be trying to spin up
            // connections anyway.
            let Some(permit) = self.semaphore.try_acquire(1) else {
                return Ok(());
            };

            // We must always obey `max_connections`.
            let Some(guard) = self.try_increment_size(permit).ok() else {
                return Ok(());
            };

            // We skip `after_release` since the connection was never provided to user code
            // besides `after_connect`, if they set it.
            self.release(self.connect(deadline, guard).await?);
        }

        Ok(())
    }

    /// Attempt to maintain `min_connections`, logging if unable.
    pub async fn min_connections_maintenance(self: &Arc<Self>, deadline: Option<Instant>) {
        let deadline = deadline.unwrap_or_else(|| {
            // Arbitrary default deadline if the caller doesn't care.
            Instant::now() + Duration::from_secs(300)
        });

        match self.try_min_connections(deadline).await {
            Ok(()) => (),
            Err(Error::PoolClosed) => (),
            Err(Error::PoolTimedOut) => {
                tracing::debug!("unable to complete `min_connections` maintenance before deadline")
            }
            Err(error) => tracing::debug!(%error, "error while maintaining min_connections"),
        }
    }
}

impl<DB: Database> Drop for PoolInner<DB> {
    fn drop(&mut self) {
        self.mark_closed();

        if let Some(parent) = &self.options.parent_pool {
            // Release the stolen permits.
            parent.0.semaphore.release(self.semaphore.permits());
        }
    }
}

/// Returns `true` if the connection has exceeded `options.max_lifetime` if set, `false` otherwise.
pub(super) fn is_beyond_max_lifetime<DB: Database>(
    live: &Live<DB>,
    options: &PoolOptions<DB>,
) -> bool {
    options
        .max_lifetime
        .is_some_and(|max| live.created_at.elapsed() > max)
}

/// Returns `true` if the connection has exceeded `options.idle_timeout` if set, `false` otherwise.
fn is_beyond_idle_timeout<DB: Database>(idle: &Idle<DB>, options: &PoolOptions<DB>) -> bool {
    options
        .idle_timeout
        .is_some_and(|timeout| idle.idle_since.elapsed() > timeout)
}

async fn check_idle_conn<DB: Database>(
    mut conn: Floating<DB, Idle<DB>>,
    options: &PoolOptions<DB>,
    deadline: Instant,
) -> Result<Floating<DB, Live<DB>>, DecrementSizeGuard<DB>> {
    if options.test_before_acquire {
        // Check that the connection is still live.
        //
        // `ping()` is an un-timed round trip on the network drivers, so bound
        // it. The budget is HALF the acquire time still remaining, not a fixed
        // bound: a server that has stopped answering must cost this acquire at
        // most half of its budget, so the loop keeps enough time to discard the
        // connection and open a fresh one. A fixed bound equal to the fleet's
        // `acquire_timeout` spent the whole budget and turned the recovery into
        // `PoolTimedOut` — the failure this bound exists to prevent.
        // On either an error or an expiry the connection is discarded without
        // an awaited close — see `Floating::discard` for why `close_hard()` is
        // not used here.
        let ping_budget = deadline.saturating_duration_since(Instant::now()) / 2;
        match crate::rt::timeout(ping_budget, conn.ping()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                // an error here means the other end has hung up or we lost connectivity
                // either way we're fine to just discard the connection
                // the error itself here isn't necessarily unexpected so WARN is too strong
                tracing::info!(%error, "ping on idle connection returned error");
                // connection is broken so don't try to close nicely
                return Err(conn.discard());
            }
            Err(_elapsed) => {
                tracing::warn!("ping on idle connection timed out after {:?}", ping_budget,);
                // connection is broken so don't try to close nicely
                return Err(conn.discard());
            }
        }
    }

    if let Some(test) = &options.before_acquire {
        let meta = conn.metadata();
        match test(&mut conn.live.raw, meta).await {
            Ok(false) => {
                // connection was rejected by user-defined hook, close nicely
                return Err(conn.close().await);
            }

            Err(error) => {
                tracing::warn!(%error, "error from `before_acquire`");
                // connection is broken so don't try to close nicely
                return Err(conn.close_hard().await);
            }

            Ok(true) => {}
        }
    }

    // No need to re-connect; connection is alive or we don't care
    Ok(conn.into_live())
}

fn spawn_maintenance_tasks<DB: Database>(pool: &Arc<PoolInner<DB>>) {
    // NOTE: use `pool_weak` for the maintenance tasks
    // so they don't keep `PoolInner` from being dropped.
    let pool_weak = Arc::downgrade(pool);

    let period = match (pool.options.max_lifetime, pool.options.idle_timeout) {
        (Some(it), None) | (None, Some(it)) => it,

        (Some(a), Some(b)) => cmp::min(a, b),

        (None, None) => {
            if pool.options.min_connections > 0 {
                crate::rt::spawn(async move {
                    if let Some(pool) = pool_weak.upgrade() {
                        pool.min_connections_maintenance(None).await;
                    }
                });
            }

            return;
        }
    };

    // Immediately cancel this task if the pool is closed.
    let mut close_event = pool.close_event();

    crate::rt::spawn(async move {
        let _ = close_event
            .do_until(async {
                // If the last handle to the pool was dropped while we were sleeping
                while let Some(pool) = pool_weak.upgrade() {
                    if pool.is_closed() {
                        return;
                    }

                    let next_run = Instant::now() + period;

                    // Go over all idle connections, check for idleness and lifetime,
                    // and if we have fewer than min_connections after reaping a connection,
                    // open a new one immediately. Note that other connections may be popped from
                    // the queue in the meantime - that's fine, there is no harm in checking more
                    for _ in 0..pool.num_idle() {
                        if let Some(conn) = pool.try_acquire() {
                            if is_beyond_idle_timeout(&conn, &pool.options)
                                || is_beyond_max_lifetime(&conn, &pool.options)
                            {
                                let _ = conn.close().await;
                                pool.min_connections_maintenance(Some(next_run)).await;
                            } else {
                                pool.release(conn.into_live());
                            }
                        }
                    }

                    // Don't hold a reference to the pool while sleeping.
                    drop(pool);

                    if let Some(duration) = next_run.checked_duration_since(Instant::now()) {
                        // `async-std` doesn't have a `sleep_until()`
                        crate::rt::sleep(duration).await;
                    } else {
                        // `next_run` is in the past, just yield.
                        crate::rt::yield_now().await;
                    }
                }
            })
            .await;
    });
}

/// RAII guard returned by `Pool::try_increment_size()` and others.
///
/// Will decrement the pool size if dropped, to avoid semantically "leaking" connections
/// (where the pool thinks it has more connections than it does).
pub(in crate::pool) struct DecrementSizeGuard<DB: Database> {
    pub(crate) pool: Arc<PoolInner<DB>>,
    cancelled: bool,
}

impl<DB: Database> DecrementSizeGuard<DB> {
    /// Create a new guard that will release a semaphore permit on-drop.
    pub fn new_permit(pool: Arc<PoolInner<DB>>) -> Self {
        Self {
            pool,
            cancelled: false,
        }
    }

    pub fn from_permit(pool: Arc<PoolInner<DB>>, permit: AsyncSemaphoreReleaser<'_>) -> Self {
        // here we effectively take ownership of the permit
        permit.disarm();
        Self::new_permit(pool)
    }

    /// Release the semaphore permit without decreasing the pool size.
    ///
    /// If the permit was stolen from the pool's parent, it will be returned to the child's semaphore.
    fn release_permit(self) {
        self.pool.semaphore.release(1);
        self.cancel();
    }

    pub fn cancel(mut self) {
        self.cancelled = true;
    }
}

impl<DB: Database> Drop for DecrementSizeGuard<DB> {
    fn drop(&mut self) {
        if !self.cancelled {
            // Saturating: `PoolInner::close()` stores `size = 0` once it stops
            // waiting for checked-out connections (see `CLOSE_JOIN_TIMEOUT`),
            // and a connection that was still checked out at that point is
            // dropped — taking this guard with it — only later. A plain
            // `fetch_sub` would then wrap `size` to `u32::MAX` and make
            // `Pool::size()` report a lie. Zero is the truthful value at that
            // point, because this connection is genuinely gone. No behaviour
            // change while the count is non-zero (the normal path).
            let _ = self
                .pool
                .size
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |size| {
                    Some(size.saturating_sub(1))
                });

            // and here we release the permit we got on construction
            self.pool.semaphore.release(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use either::Either;
    use futures_core::future::BoxFuture;
    use futures_core::stream::BoxStream;
    use sqlx::pool::PoolOptions;
    use sqlx::Sqlite;

    use crate::arguments::Arguments;
    use crate::column::Column;
    use crate::connection::{ConnectOptions, Connection};
    use crate::database::Database;
    use crate::encode::Encode;
    use crate::error::{BoxDynError, Error};
    use crate::executor::{Execute, Executor};
    use crate::row::Row;
    use crate::sql_str::SqlStr;
    use crate::statement::Statement;
    use crate::transaction::{Transaction, TransactionManager};
    use crate::type_info::TypeInfo;
    use crate::types::Type;
    use crate::value::{Value, ValueRef};

    use super::CLOSE_JOIN_TIMEOUT;

    /// `Pool::close()` must not be holdable open forever by a caller that never
    /// returns its connection: the join on checked-out permits is bounded, and
    /// on expiry `close()` proceeds to the drain and returns anyway.
    ///
    /// Runs against in-memory SQLite, so it needs no database server. Run with
    /// the crate's own command: `cargo test -p sqlx-core --all-features`.
    #[test]
    fn close_gives_up_on_a_permit_that_is_never_released() {
        crate::rt::test_block_on(async {
            let pool = PoolOptions::<Sqlite>::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .expect("connect to in-memory SQLite");

            // Take the pool's only permit and never give it back, standing in
            // for a caller that holds a `PoolConnection` for the life of the
            // process.
            std::mem::forget(pool.acquire().await.expect("acquire a connection"));

            let start = Instant::now();
            pool.close().await;
            let elapsed = start.elapsed();

            // It had to wait out the bound — so the join really was abandoned
            // rather than satisfied — but it did not wait forever.
            assert!(
                elapsed >= CLOSE_JOIN_TIMEOUT,
                "close() returned in {elapsed:?}, less than the {CLOSE_JOIN_TIMEOUT:?} bound, \
                 so it never had to give up on the held permit"
            );
            assert!(
                elapsed < CLOSE_JOIN_TIMEOUT * 4,
                "close() took {elapsed:?}; the join on checked-out connections is not bounded \
                 by {CLOSE_JOIN_TIMEOUT:?}"
            );
        });
    }

    /// A connection that was still checked out when the join was abandoned must
    /// not wrap the pool's size counter when it is finally dropped.
    ///
    /// `close()` stores `size = 0` on giving up the join, so the guard this
    /// connection still holds decrements against zero; with a plain `fetch_sub`
    /// that wraps to `u32::MAX` and `Pool::size()` reports the lie.
    #[test]
    fn size_saturates_when_an_abandoned_connection_is_dropped() {
        crate::rt::test_block_on(async {
            let pool = PoolOptions::<Sqlite>::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .expect("connect to in-memory SQLite");

            let mut conn = pool.acquire().await.expect("acquire a connection");
            assert_eq!(pool.size(), 1);

            // The permit is held for the whole call, so the join is abandoned
            // and `size` is stored 0 while this connection is still checked out.
            pool.close().await;
            assert_eq!(pool.size(), 0);

            // Dropping it here runs the size guard against that zero.
            conn.return_to_pool().await;

            assert_eq!(
                pool.size(),
                0,
                "the size counter wrapped instead of saturating"
            );
        });
    }

    /// The happy path of the bounded on-release ping: a healthy connection is
    /// still returned to the pool rather than closed.
    ///
    /// If the timeout wrapper mis-mapped its `Ok(Ok(()))` result, every release
    /// would take the ping-failure branch and hard-close instead, so `size()`
    /// would fall to 0 and the connection would never reach the idle set.
    #[test]
    fn release_returns_a_healthy_connection_to_the_pool() {
        crate::rt::test_block_on(async {
            let pool = PoolOptions::<Sqlite>::new()
                .max_connections(2)
                .connect("sqlite::memory:")
                .await
                .expect("connect to in-memory SQLite");

            let mut conn = pool.acquire().await.expect("acquire a connection");
            assert_eq!(pool.size(), 1);

            conn.return_to_pool().await;

            assert_eq!(
                pool.size(),
                1,
                "the connection was reaped instead of being returned to the pool"
            );
            assert_eq!(
                pool.num_idle(),
                1,
                "the connection was not placed in the idle set"
            );
        });
    }

    /// The fleet's `acquire_timeout`. The ping budget is derived from it (half
    /// of what remains), so a wedged ping leaves enough time to reconnect.
    const ACQUIRE_BUDGET: Duration = Duration::from_secs(5);

    /// An idle connection whose server stops answering the liveness ping must
    /// cost the acquire at most HALF its budget, not the whole of it: the pool
    /// discards the connection and hands out a fresh one, reusing the discarded
    /// connection's pool slot, and the acquire still SUCCEEDS inside the
    /// caller's timeout. A fixed ping bound equal to `acquire_timeout` would
    /// spend the whole budget and fail the acquire `PoolTimedOut` — the
    /// recovery this bound exists to make possible.
    ///
    /// `max_connections(1)` makes the permit accounting load-bearing: if the
    /// discarded connection took its slot with it, the replacement connect
    /// could not happen and the acquire would time out instead of returning.
    #[test]
    fn acquire_replaces_an_idle_connection_whose_ping_never_answers() {
        crate::rt::test_block_on(async {
            let gate = Arc::new(MockGate::default());
            // `crate::pool::PoolOptions`, not the facade re-export: the mock's
            // `Database` impl lives in this crate, and the lib-test target links
            // the facade as a separate sqlx-core build.
            let pool = crate::pool::PoolOptions::<MockDb>::new()
                .max_connections(1)
                .acquire_timeout(ACQUIRE_BUDGET)
                .connect_with(MockOptions {
                    gate: Arc::clone(&gate),
                })
                .await
                .expect("build the mock pool");

            assert_eq!(gate.connects(), 1, "connect_with opened no connection");
            assert_eq!(pool.size(), 1);
            assert_eq!(pool.num_idle(), 1);

            // Healthy side first: the idle ping answers, the same connection is
            // handed out, and no new connection is opened.
            let mut conn = pool
                .acquire()
                .await
                .expect("acquire the healthy idle connection");
            assert_eq!(conn.id, 1);
            assert_eq!(
                gate.connects(),
                1,
                "a healthy idle connection was not reused"
            );
            conn.return_to_pool().await;

            // Now the server behind the idle connection stops answering.
            gate.set_wedged(true);

            let start = Instant::now();
            let mut conn = pool
                .acquire()
                .await
                .expect("acquire a replacement for the wedged idle connection");
            let elapsed = start.elapsed();

            assert_eq!(
                conn.id, 2,
                "the acquire returned the wedged connection instead of a fresh one"
            );
            assert_eq!(gate.connects(), 2, "no fresh connection was opened");
            assert!(
                elapsed >= ACQUIRE_BUDGET / 2,
                "acquire returned in {elapsed:?}, less than half the {ACQUIRE_BUDGET:?} \
                 budget, so the idle ping was never bounded"
            );
            assert!(
                elapsed < ACQUIRE_BUDGET,
                "acquire took {elapsed:?}; the ping spent so much of the \
                 {ACQUIRE_BUDGET:?} budget that the replacement did not fit"
            );
            assert_eq!(
                pool.size(),
                1,
                "the discarded connection's pool slot was not reused"
            );
            assert_eq!(pool.num_idle(), 0);

            // The replacement is wedged too (the gate is shared), so let its
            // return ping answer before the pool is closed.
            gate.set_wedged(false);
            conn.return_to_pool().await;
            pool.close().await;
        });
    }

    /// A ping that FAILS (the peer hung up) must be answered by an immediate
    /// discard and reconnect — the fast arm of the same bound, well inside the
    /// budget rather than waiting it out.
    #[test]
    fn acquire_reconnects_immediately_when_the_ping_errors() {
        crate::rt::test_block_on(async {
            let gate = Arc::new(MockGate::default());
            let pool = crate::pool::PoolOptions::<MockDb>::new()
                .max_connections(1)
                .acquire_timeout(ACQUIRE_BUDGET)
                .connect_with(MockOptions {
                    gate: Arc::clone(&gate),
                })
                .await
                .expect("build the mock pool");
            assert_eq!(pool.num_idle(), 1);

            gate.set_fail_ping(true);

            let start = Instant::now();
            let mut conn = pool
                .acquire()
                .await
                .expect("a failing ping must be replaced, not surfaced");
            let elapsed = start.elapsed();

            assert_eq!(conn.id, 2, "the failed-ping connection was handed out");
            assert_eq!(gate.connects(), 2, "no fresh connection was opened");
            assert!(
                elapsed < ACQUIRE_BUDGET / 4,
                "a failing ping took {elapsed:?}; it must be discarded at once, not held \
                 for the {ACQUIRE_BUDGET:?} budget"
            );
            assert_eq!(pool.size(), 1);

            gate.set_fail_ping(false);
            conn.return_to_pool().await;
            pool.close().await;
        });
    }

    /// Cancelling an acquire while the liveness ping is in flight must leave
    /// the pool usable: the popped connection and its size guard are released
    /// with the dropped future, so the only permit is not stranded.
    #[test]
    fn dropping_an_acquire_mid_ping_does_not_strand_the_pool() {
        crate::rt::test_block_on(async {
            let gate = Arc::new(MockGate::default());
            let pool = crate::pool::PoolOptions::<MockDb>::new()
                .max_connections(1)
                .acquire_timeout(ACQUIRE_BUDGET)
                .connect_with(MockOptions {
                    gate: Arc::clone(&gate),
                })
                .await
                .expect("build the mock pool");
            assert_eq!(pool.num_idle(), 1);

            // The server stops answering and the caller gives up mid-ping.
            gate.set_wedged(true);
            {
                let acquire = pool.acquire();
                let mut acquire = std::pin::pin!(acquire);
                let polled = crate::rt::timeout(Duration::from_millis(100), &mut acquire).await;
                assert!(
                    polled.is_err(),
                    "the liveness ping must still be in flight when the caller is dropped"
                );
                // `acquire` (and the connection it popped) is dropped here.
            }

            // The permit and the size guard must not have leaked with it.
            gate.set_wedged(false);
            let mut conn = crate::rt::timeout(Duration::from_secs(2), pool.acquire())
                .await
                .expect("a cancelled acquire stranded the pool's only permit")
                .expect("acquire after a cancelled ping");
            assert_eq!(pool.size(), 1, "the pool's size accounting drifted");

            conn.return_to_pool().await;
            pool.close().await;
        });
    }

    // ------------------------------------------------------------------
    // A mock database for the acquire-side wait bounds.
    //
    // The pool's behaviour when a server never answers `Connection::ping()`
    // is unreachable through the in-memory SQLite driver: SQLite's ping is a
    // channel send to its worker (`Command::Ping`), so there is nothing to
    // withhold. This mock exists only to hold a ping open on demand. Every
    // other method satisfies the trait surface and is never reached by the
    // pool path under test.
    // ------------------------------------------------------------------

    #[derive(Debug, Default)]
    struct MockGate {
        /// While true, `MockConnection::ping` never completes.
        wedge: AtomicBool,
        /// While true, `MockConnection::ping` returns an error immediately.
        fail_ping: AtomicBool,
        /// Number of connections opened through these options.
        connects: AtomicUsize,
    }

    impl MockGate {
        fn set_wedged(&self, wedged: bool) {
            self.wedge.store(wedged, Ordering::SeqCst);
        }

        fn set_fail_ping(&self, fail: bool) {
            self.fail_ping.store(fail, Ordering::SeqCst);
        }

        fn connects(&self) -> usize {
            self.connects.load(Ordering::SeqCst)
        }
    }

    #[derive(Debug, Default)]
    struct MockDb;

    #[derive(Debug, Clone)]
    struct MockOptions {
        gate: Arc<MockGate>,
    }

    impl Default for MockOptions {
        fn default() -> Self {
            Self {
                gate: Arc::new(MockGate::default()),
            }
        }
    }

    impl std::str::FromStr for MockOptions {
        type Err = Error;

        fn from_str(_url: &str) -> Result<Self, Self::Err> {
            Ok(Self::default())
        }
    }

    impl ConnectOptions for MockOptions {
        type Connection = MockConnection;

        fn from_url(_url: &url::Url) -> Result<Self, Error> {
            Ok(Self::default())
        }

        async fn connect(&self) -> Result<MockConnection, Error> {
            let id = self.gate.connects.fetch_add(1, Ordering::SeqCst) + 1;

            Ok(MockConnection {
                gate: Arc::clone(&self.gate),
                id,
            })
        }

        fn log_statements(self, _level: log::LevelFilter) -> Self {
            self
        }

        fn log_slow_statements(self, _level: log::LevelFilter, _duration: Duration) -> Self {
            self
        }
    }

    #[derive(Debug)]
    struct MockConnection {
        gate: Arc<MockGate>,
        id: usize,
    }

    impl Connection for MockConnection {
        type Database = MockDb;
        type Options = MockOptions;

        async fn close(self) -> Result<(), Error> {
            Ok(())
        }

        async fn close_hard(self) -> Result<(), Error> {
            // NEVER completes. The acquire-path fix must DISCARD the wedged
            // connection (`Floating::discard`, a synchronous drop), never
            // await a hard close. An implementation that swapped `discard()`
            // back to `close_hard().await` would park here until the acquire
            // timeout and fail the witness — the mutation this mock exists to
            // kill.
            std::future::pending::<()>().await;

            Ok(())
        }

        async fn ping(&mut self) -> Result<(), Error> {
            if self.gate.fail_ping.load(Ordering::SeqCst) {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "mock ping failure",
                )));
            }
            if self.gate.wedge.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }

            Ok(())
        }

        async fn begin(&mut self) -> Result<Transaction<'_, MockDb>, Error> {
            unimplemented!("the mock database never begins transactions")
        }

        fn shrink_buffers(&mut self) {}

        async fn flush(&mut self) -> Result<(), Error> {
            Ok(())
        }

        fn should_flush(&self) -> bool {
            false
        }
    }

    impl<'c> Executor<'c> for &'c mut MockConnection {
        type Database = MockDb;

        fn fetch_many<'e, 'q: 'e, E>(
            self,
            _query: E,
        ) -> BoxStream<'e, Result<Either<MockQueryResult, MockRow>, Error>>
        where
            'c: 'e,
            E: 'q + Execute<'q, MockDb>,
        {
            unimplemented!("the mock database never executes statements")
        }

        fn fetch_optional<'e, 'q: 'e, E>(
            self,
            _query: E,
        ) -> BoxFuture<'e, Result<Option<MockRow>, Error>>
        where
            'c: 'e,
            E: 'q + Execute<'q, MockDb>,
        {
            unimplemented!("the mock database never executes statements")
        }

        fn prepare_with<'e>(
            self,
            _sql: SqlStr,
            _parameters: &'e [MockTypeInfo],
        ) -> BoxFuture<'e, Result<MockStatement, Error>>
        where
            'c: 'e,
        {
            unimplemented!("the mock database never prepares statements")
        }

        #[cfg(feature = "offline")]
        fn describe<'e>(
            self,
            _sql: SqlStr,
        ) -> BoxFuture<'e, Result<crate::describe::Describe<MockDb>, Error>>
        where
            'c: 'e,
        {
            unimplemented!("the mock database never describes statements")
        }
    }

    impl TransactionManager for MockTransactionManager {
        type Database = MockDb;

        async fn begin(
            _conn: &mut MockConnection,
            _statement: Option<SqlStr>,
        ) -> Result<(), Error> {
            Ok(())
        }

        async fn commit(_conn: &mut MockConnection) -> Result<(), Error> {
            Ok(())
        }

        async fn rollback(_conn: &mut MockConnection) -> Result<(), Error> {
            Ok(())
        }

        fn start_rollback(_conn: &mut MockConnection) {}

        fn get_transaction_depth(_conn: &MockConnection) -> usize {
            0
        }
    }

    #[derive(Debug)]
    struct MockTransactionManager;

    #[derive(Debug, Default)]
    struct MockQueryResult;

    impl Extend<MockQueryResult> for MockQueryResult {
        fn extend<I: IntoIterator<Item = MockQueryResult>>(&mut self, _iter: I) {}
    }

    #[derive(Debug, Clone, PartialEq)]
    struct MockTypeInfo;

    static MOCK_TYPE_INFO: MockTypeInfo = MockTypeInfo;

    impl std::fmt::Display for MockTypeInfo {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(MOCK_TYPE_INFO.name())
        }
    }

    impl TypeInfo for MockTypeInfo {
        fn is_null(&self) -> bool {
            true
        }

        fn name(&self) -> &str {
            "MOCK"
        }
    }

    #[derive(Debug, Clone)]
    struct MockColumn;

    impl Column for MockColumn {
        type Database = MockDb;

        fn ordinal(&self) -> usize {
            0
        }

        fn name(&self) -> &str {
            "mock"
        }

        fn type_info(&self) -> &MockTypeInfo {
            &MOCK_TYPE_INFO
        }
    }

    #[derive(Debug)]
    struct MockRow;

    impl Row for MockRow {
        type Database = MockDb;

        fn columns(&self) -> &[MockColumn] {
            &[]
        }

        fn try_get_raw<I>(&self, _index: I) -> Result<MockValueRef, Error>
        where
            I: crate::column::ColumnIndex<Self>,
        {
            unimplemented!("the mock database never returns rows")
        }
    }

    #[derive(Debug)]
    struct MockValue;

    #[derive(Debug)]
    struct MockValueRef;

    impl Value for MockValue {
        type Database = MockDb;

        fn as_ref(&self) -> MockValueRef {
            MockValueRef
        }

        fn type_info(&self) -> Cow<'_, MockTypeInfo> {
            Cow::Borrowed(&MOCK_TYPE_INFO)
        }

        fn is_null(&self) -> bool {
            true
        }
    }

    impl<'r> ValueRef<'r> for MockValueRef {
        type Database = MockDb;

        fn to_owned(&self) -> MockValue {
            MockValue
        }

        fn type_info(&self) -> Cow<'_, MockTypeInfo> {
            Cow::Borrowed(&MOCK_TYPE_INFO)
        }

        fn is_null(&self) -> bool {
            true
        }
    }

    #[derive(Debug, Clone)]
    struct MockStatement;

    impl Statement for MockStatement {
        type Database = MockDb;

        fn into_sql(self) -> SqlStr {
            unimplemented!("the mock database never prepares statements")
        }

        fn sql(&self) -> &SqlStr {
            unimplemented!("the mock database never prepares statements")
        }

        fn parameters(&self) -> Option<Either<&[MockTypeInfo], usize>> {
            None
        }

        fn columns(&self) -> &[MockColumn] {
            &[]
        }

        crate::impl_statement_query!(MockArguments);
    }

    #[derive(Debug, Default)]
    struct MockArguments;

    impl Arguments for MockArguments {
        type Database = MockDb;

        fn reserve(&mut self, _additional: usize, _size: usize) {}

        fn add<'t, T>(&mut self, _value: T) -> Result<(), BoxDynError>
        where
            T: Encode<'t, MockDb> + Type<MockDb>,
        {
            unimplemented!("the mock database never binds arguments")
        }

        fn len(&self) -> usize {
            0
        }
    }

    impl Database for MockDb {
        type Connection = MockConnection;
        type TransactionManager = MockTransactionManager;
        type Row = MockRow;
        type QueryResult = MockQueryResult;
        type Column = MockColumn;
        type TypeInfo = MockTypeInfo;
        type Value = MockValue;
        type ValueRef<'r> = MockValueRef;
        type Arguments = MockArguments;
        type ArgumentBuffer = Vec<u8>;
        type Statement = MockStatement;

        const NAME: &'static str = "mock";
        const URL_SCHEMES: &'static [&'static str] = &["mock"];
    }
}
