"""Async sources provider for the async worker.

Port of osprey.worker.lib.sources_provider with asyncio instead of gevent.
The etcd watcher runs in a thread pool (run_in_executor) since the underlying
etcd client is synchronous. This is acceptable because etcd updates are
infrequent (rule deployments, not per-request).
"""

import asyncio
import inspect
import json
import logging
import random
from typing import Any, Awaitable, Callable, Dict, Iterator, Optional, Union

import sentry_sdk
from osprey.engine.ast.sources import Sources
from osprey.worker.lib.etcd import BaseWatcher, EtcdClient, FullSyncOne, FullSyncOneNoKey, KeyNotFoundError
from osprey.worker.lib.instruments import metrics
from osprey.worker.lib.sources_provider_base import BaseSourcesProvider

# The async engine's _handle_updated_sources is a coroutine function so the
# compile can run in a thread pool while the event loop continues servicing
# in-flight tasks. We accept either a sync or async callable for back-compat.
SourcesWatcherCallback = Callable[[], Union[None, Awaitable[None]]]

# A named logger: the worker's root logger carries a DynamicLogSampler that drops records once the rate is high.
log = logging.getLogger(__name__)

_WATCH_EXHAUSTED = object()
_MAX_RETRY_SECONDS = 300.0


class AsyncEtcdSourcesProvider(BaseSourcesProvider):
    """Provides sources dynamically updated by etcd, using asyncio.

    The etcd client is synchronous, so watch operations are offloaded to
    a thread pool via run_in_executor. This is fine because etcd updates
    happen infrequently (rule deployments).
    """

    def __init__(
        self,
        etcd_key: str,
        etcd_client: Optional[EtcdClient] = None,
        reload_jitter_seconds: float = 0,
        unapplied_retry_seconds: float = 0,
        max_attempts_per_hash: int = 5,
        exit_on_retry_exhausted: bool = False,
    ):
        self._etcd_key = etcd_key
        self._client = etcd_client or EtcdClient()
        self._current_sources: Optional[Sources] = None
        # Hash of the sources the engine confirmed via mark_sources_applied. Dedup keys off
        # this, not the last received hash, so a failed compile is retried on re-delivery.
        self._applied_sources_hash: Optional[str] = None
        self._sources_watcher_callback: Optional[SourcesWatcherCallback] = None
        self._reload_jitter_seconds = reload_jitter_seconds
        self._watcher: Optional[BaseWatcher] = None
        # Long-lived iterator over the watcher's event stream. continue_watching()
        # is a generator function — every call creates a new generator with a
        # fresh WatchMux and a reset _index, which defeats the watcher's built-in
        # dedup of redundant FullSyncOne events. Iterate one generator persistently
        # to match how ReadOnlyEtcdDict drives the gevent watcher.
        self._watcher_iter: Optional[Iterator[Any]] = None
        self._watcher_task: Optional[asyncio.Task[None]] = None
        # The watcher's mux drops an unchanged value re-delivered on the same stream, so a failed
        # compile is only retried on a watcher reset. When > 0 (off by default), re-read etcd and
        # retry after this many seconds, doubling up to _MAX_RETRY_SECONDS while sources stay unapplied.
        # Any retry path stops after max_attempts_per_hash compiles of one hash, until a different hash arrives.
        self._unapplied_retry_seconds = unapplied_retry_seconds
        self._max_attempts_per_hash = max_attempts_per_hash
        self._retry_delay = unapplied_retry_seconds
        self._tracked_hash: Optional[str] = None
        self._attempts = 0
        self._exhausted_reported_hash: Optional[str] = None
        # When set, exhaustion is fatal: wait_for_retry_exhausted() returns and the runner must exit
        # so a fresh process loads the latest sources, instead of serving the old graph indefinitely.
        self._exit_on_retry_exhausted = exit_on_retry_exhausted
        self._retry_exhausted = asyncio.Event()
        self._exhausted_exit_hash = ''
        self._retry_task: Optional[asyncio.Task[None]] = None
        self._handle_lock = asyncio.Lock()

    async def start(self) -> None:
        """Initialize sources from etcd and start watching for changes."""
        loop = asyncio.get_running_loop()

        # Initial load in thread pool (sync etcd client)
        initial_dict = await loop.run_in_executor(None, self._load_initial)
        self._current_sources = Sources.from_dict(initial_dict)
        # The engine compiles this initial snapshot at construction.
        self._applied_sources_hash = self._current_sources.hash()

        # Start watcher loop as an async task
        self._watcher_task = asyncio.create_task(self._watch_loop())

    def _load_initial(self) -> Dict[str, str]:
        """Load initial sources from etcd. Runs in thread pool."""
        watcher = self._client.get_watcher(self._etcd_key, recursive=False)
        initial_event = watcher.begin_watching()
        self._watcher = watcher
        return self._parse_event(initial_event)

    def _parse_event(self, event) -> Dict[str, str]:
        """Parse an etcd event into a sources dict."""
        if isinstance(event, FullSyncOne):
            return json.loads(str(event.value))
        elif isinstance(event, FullSyncOneNoKey):
            return {}
        return {}

    async def _watch_loop(self) -> None:
        """Watch for etcd changes, running the sync watcher in a thread pool."""
        loop = asyncio.get_running_loop()
        backoff = 1.0
        fresh_stream = True
        try:
            while True:
                if self._watcher is None:
                    self._watcher = await loop.run_in_executor(None, self._client.get_watcher, self._etcd_key, False)
                    self._watcher_iter = None
                if self._watcher_iter is None:
                    assert self._watcher is not None
                    self._watcher_iter = self._watcher.continue_watching()
                    fresh_stream = True

                # Block in thread pool waiting for next etcd event. Drive the
                # SAME generator each iteration — the watcher's WatchMux dedups
                # consecutive identical FullSyncOne events (which are common
                # post-rule-deploy as etcd's wait API re-syncs), but the dedup
                # state lives on the generator. Recreating the generator per
                # event would defeat the dedup and put the loop into a tight
                # SYNC re-fetch loop on the main asyncio thread.
                watcher_iter = self._watcher_iter
                try:
                    # next() with a default: asyncio cannot raise StopIteration into a future, so
                    # a bare next() on an exhausted generator hangs this task forever.
                    event = await loop.run_in_executor(None, next, watcher_iter, _WATCH_EXHAUSTED)
                except Exception:
                    log.exception('Error in etcd watcher loop, retrying in %.1fs', backoff)
                    self._watcher = None
                    self._watcher_iter = None
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)
                    continue

                if event is _WATCH_EXHAUSTED:
                    # Generator exhausted (e.g. transient etcd error inside
                    # continue_watching). Reopen the watcher fresh.
                    log.warning('etcd watcher stream ended, reopening in %.1fs', backoff)
                    self._watcher = None
                    self._watcher_iter = None
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)
                    continue

                # A reopened stream's initial sync says nothing about its health, so it keeps the backoff.
                if fresh_stream:
                    fresh_stream = False
                else:
                    backoff = 1.0

                if event is not None:
                    # The event was read before the lock, so a retry may apply a newer value first and
                    # this one lands after it. The stream then delivers the newer value too, so it converges.
                    await self._handle_event_safely(event)
                    self._schedule_unapplied_retry()
        except asyncio.CancelledError:
            return
        finally:
            self._watcher = None
            self._watcher_iter = None

    def _report_handler_error(self) -> None:
        log.exception('Error handling etcd sources event; watcher stays alive')
        metrics.increment('osprey.sources_provider.handler_error')
        sentry_sdk.capture_exception()

    async def _handle_event_safely(self, event) -> None:
        # A raise here would end the caller's task and silently stop all future rule updates.
        try:
            async with self._handle_lock:
                await self._handle_event(event)
        except Exception:
            self._report_handler_error()

    def _fetch_current_event(self):
        # Plain GET: a throwaway watcher could open a long-lived stream that nothing cancels.
        try:
            return FullSyncOne.from_node(self._client.get(self._etcd_key).node)
        except KeyNotFoundError:
            return FullSyncOneNoKey(self._etcd_key)

    def _report_exhausted(self, sources_hash: str, attempts: int) -> None:
        # Once per hash: both the retry scheduler and a later re-delivery can reach this.
        if self._exhausted_reported_hash == sources_hash:
            return
        self._exhausted_reported_hash = sources_hash
        log.error('Giving up on sources=%s after %d failed attempts to apply', sources_hash, attempts)
        metrics.increment('osprey.sources_provider.retry_exhausted')
        if self._exit_on_retry_exhausted:
            sentry_sdk.capture_message(
                f'Giving up on sources={sources_hash} after {attempts} failed attempts; exiting', level='error'
            )
            self._exhausted_exit_hash = sources_hash
            self._retry_exhausted.set()

    async def wait_for_retry_exhausted(self) -> str:
        """Returns the given-up sources hash once retries are exhausted; never returns unless exit_on_retry_exhausted."""
        await self._retry_exhausted.wait()
        return self._exhausted_exit_hash

    def _retries_exhausted(self) -> bool:
        return (
            self._current_sources is not None
            and self._current_sources.hash() == self._tracked_hash
            and self._attempts >= self._max_attempts_per_hash
        )

    def _schedule_unapplied_retry(self) -> None:
        if self._unapplied_retry_seconds <= 0 or (self._retry_task is not None and not self._retry_task.done()):
            return
        if self._current_sources is None or self._current_sources.hash() == self._applied_sources_hash:
            self._retry_delay = self._unapplied_retry_seconds
            return
        if self._retries_exhausted():
            self._report_exhausted(self._current_sources.hash(), self._attempts)
            return
        self._retry_task = asyncio.create_task(self._retry_unapplied())

    async def _retry_unapplied(self) -> None:
        delay = self._retry_delay
        self._retry_delay = min(delay * 2, _MAX_RETRY_SECONDS)
        await asyncio.sleep(delay)
        # Stagger outside the lock so a fresh push is not queued behind the jitter.
        if self._reload_jitter_seconds > 0:
            await asyncio.sleep(random.uniform(0, self._reload_jitter_seconds))
        try:
            # Read inside the lock so a stale value cannot be applied after a newer one.
            async with self._handle_lock:
                try:
                    event = await asyncio.get_running_loop().run_in_executor(None, self._fetch_current_event)
                except Exception:
                    log.exception('Error re-reading etcd sources for retry')
                    metrics.increment('osprey.sources_provider.retry_read_error')
                    sentry_sdk.capture_exception()
                else:
                    await self._handle_event(event, jitter=False)
        except Exception:
            self._report_handler_error()
        self._retry_task = None
        self._schedule_unapplied_retry()

    async def _handle_event(self, event, jitter: bool = True) -> None:
        """Handle an etcd event by updating sources and notifying watchers."""
        sources_dict = self._parse_event(event)
        new_sources = Sources.from_dict(sources_dict)

        # Etcd watcher reconnects and session refreshes re-deliver the current
        # value as a FullSyncOne event, so we see many events where the
        # content is unchanged. Skip recompile when the sources hash matches
        # what the engine has already APPLIED — recompile transiently doubles
        # peak memory and can push pods over the OOM line. Comparing against the
        # last received hash would suppress the retry after a failed compile and
        # leave the pod on the old graph until restart.
        new_hash = new_sources.hash()
        if self._applied_sources_hash is not None and new_hash == self._applied_sources_hash:
            # Desired sources are the applied ones again (e.g. a rollback after a failed compile).
            # Forget the failed hash's attempts so pushing it again later gets a fresh budget.
            self._current_sources = new_sources
            self._tracked_hash = None
            self._attempts = 0
            self._exhausted_reported_hash = None
            self._retry_delay = self._unapplied_retry_seconds
            return

        if new_hash != self._tracked_hash:
            self._tracked_hash = new_hash
            self._attempts = 0
            self._retry_delay = self._unapplied_retry_seconds
        if self._attempts >= self._max_attempts_per_hash:
            self._report_exhausted(new_hash, self._attempts)
            return
        if self._attempts > 0:
            log.warning('Retrying unapplied sources=%s (attempt %d)', new_hash, self._attempts + 1)
            metrics.increment('osprey.sources_provider.reapply_unapplied')
        self._attempts += 1

        # Keep the stream connected while the engine serves its old graph; the engine
        # gates executions only for the final graph swap.
        if jitter and self._reload_jitter_seconds > 0:
            await asyncio.sleep(random.uniform(0, self._reload_jitter_seconds))

        self._current_sources = new_sources
        if self._sources_watcher_callback:
            try:
                result = self._sources_watcher_callback()
                if inspect.isawaitable(result):
                    await result
            except Exception:
                # The engine may have swapped graphs before raising, so what is applied is unknown.
                # Cost: if only the engine's post-swap tail failed, the live hash is recompiled on each
                # retry, at most max_attempts_per_hash times, and then logged as given up on.
                self._applied_sources_hash = None
                raise
            finally:
                # Report on the final failure itself: with timed retries off, no later event may come.
                if self._attempts >= self._max_attempts_per_hash and self._applied_sources_hash != new_hash:
                    self._report_exhausted(new_hash, self._attempts)

    # NOTE: BaseSourcesProvider.get_current_sources is typed -> Sources, but this
    # provider legitimately returns None before start() loads from etcd (see
    # test_provider_get_current_sources_default_none). Widening the base return type
    # to Optional[Sources] is the correct fix but lives in sources_provider_base.py
    # (a shared file outside this change). Keep the precise return type here.
    def get_current_sources(self) -> Optional[Sources]:  # type: ignore[override]
        return self._current_sources

    def set_sources_watcher(self, callback: SourcesWatcherCallback) -> None:
        self._sources_watcher_callback = callback

    def mark_sources_applied(self, sources_hash: str) -> None:
        self._applied_sources_hash = sources_hash

    async def stop(self) -> None:
        """Stop watching for etcd changes."""
        if self._watcher_task is not None:
            self._watcher_task.cancel()
            try:
                await self._watcher_task
            except asyncio.CancelledError:
                pass
            self._watcher_task = None
        if self._retry_task is not None:
            self._retry_task.cancel()
            try:
                await self._retry_task
            except asyncio.CancelledError:
                pass
            self._retry_task = None
