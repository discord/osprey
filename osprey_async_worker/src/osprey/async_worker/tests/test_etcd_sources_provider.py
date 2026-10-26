"""Tests for the async etcd sources provider and input stream signaler."""

import asyncio
from unittest.mock import MagicMock, patch

import pytest
from osprey.async_worker.lib.etcd.sources_provider import AsyncEtcdSourcesProvider

# --- AsyncEtcdSourcesProvider ---


@pytest.mark.asyncio
async def test_provider_get_current_sources_default_none():
    """Before start(), sources should be None."""
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock())
    sources = provider.get_current_sources()
    assert sources is None


@pytest.mark.asyncio
async def test_provider_set_sources_watcher():
    """Watcher callback can be set."""
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock())
    callback = MagicMock()
    provider.set_sources_watcher(callback)
    assert provider._sources_watcher_callback is callback


@pytest.mark.asyncio
async def test_provider_stop_without_start():
    """Stop without start should be safe."""
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock())
    await provider.stop()  # Should not raise


# --- _handle_event callback path ---


def _make_full_sync_event(payload: dict):
    from osprey.worker.lib.etcd import FullSyncOne

    event = MagicMock(spec=FullSyncOne)
    import json as _json

    event.value = _json.dumps(payload)
    return event


@pytest.mark.asyncio
async def test_handle_event_awaits_async_callback():
    """An async (coroutine-returning) sources_watcher_callback must be awaited.

    The async engine's _handle_updated_sources is a coroutine function that
    runs compile in a thread pool. If _handle_event calls it without awaiting,
    the compile coroutine is dropped and never executes.
    """
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock())
    awaited = asyncio.Event()

    async def async_callback() -> None:
        awaited.set()

    provider.set_sources_watcher(async_callback)

    with patch('osprey.async_worker.lib.etcd.sources_provider.asyncio.sleep', return_value=None):
        await provider._handle_event(_make_full_sync_event({'main.sml': '# noop'}))

    assert awaited.is_set(), 'async callback was not awaited'


@pytest.mark.asyncio
async def test_handle_event_staggers_reload_before_callback():
    provider = AsyncEtcdSourcesProvider(
        etcd_key='/test/key',
        etcd_client=MagicMock(),
        reload_jitter_seconds=600,
    )
    events = []

    async def async_callback() -> None:
        events.append(('callback', None))

    async def record_sleep(seconds: float) -> None:
        events.append(('sleep', seconds))

    provider.set_sources_watcher(async_callback)

    with (
        patch('osprey.async_worker.lib.etcd.sources_provider.random.uniform', return_value=42),
        patch('osprey.async_worker.lib.etcd.sources_provider.asyncio.sleep', side_effect=record_sleep),
    ):
        await provider._handle_event(_make_full_sync_event({'main.sml': '# changed'}))

    assert events == [('sleep', 42), ('callback', None)]


@pytest.mark.asyncio
async def test_handle_event_calls_sync_callback():
    """A plain (non-coroutine) callback continues to work for back-compat."""
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock())
    callback = MagicMock()
    provider.set_sources_watcher(callback)

    with patch('osprey.async_worker.lib.etcd.sources_provider.asyncio.sleep', return_value=None):
        await provider._handle_event(_make_full_sync_event({'main.sml': '# noop'}))

    callback.assert_called_once()


@pytest.mark.asyncio
async def test_handle_event_skips_callback_on_unchanged_hash():
    """Hash-equality short-circuit: if the new sources match current, no callback."""
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock())
    payload = {'main.sml': '# noop'}

    # Seed the applied hash with the same content the next event will deliver.
    from osprey.engine.ast.sources import Sources

    provider._applied_sources_hash = Sources.from_dict(payload).hash()

    callback = MagicMock()
    provider.set_sources_watcher(callback)

    await provider._handle_event(_make_full_sync_event(payload))

    callback.assert_not_called()


@pytest.mark.asyncio
async def test_redelivery_after_failed_compile_recompiles_until_applied():
    """A callback that never calls mark_sources_applied (compile failed) must be retried."""
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock())
    callback = MagicMock()
    provider.set_sources_watcher(callback)
    event = _make_full_sync_event({'main.sml': '# broken'})

    await provider._handle_event(event)
    await provider._handle_event(event)
    assert callback.call_count == 2

    provider.mark_sources_applied(provider.get_current_sources().hash())
    await provider._handle_event(event)
    assert callback.call_count == 2


@pytest.mark.asyncio
async def test_handler_exception_does_not_kill_watch_loop():
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock())
    second_handled = asyncio.Event()
    calls = []

    def callback() -> None:
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError('boom')
        second_handled.set()

    provider.set_sources_watcher(callback)
    events = [_make_full_sync_event({'main.sml': '# one'}), _make_full_sync_event({'main.sml': '# two'})]
    watcher = MagicMock()
    watcher.continue_watching.return_value = iter(events)
    provider._watcher = watcher

    with patch('osprey.async_worker.lib.etcd.sources_provider.metrics') as mock_metrics:
        provider._watcher_task = asyncio.create_task(provider._watch_loop())
        await asyncio.wait_for(second_handled.wait(), timeout=5)
        assert not provider._watcher_task.done()
        mock_metrics.increment.assert_called_once_with('osprey.sources_provider.handler_error')

    await provider.stop()


@pytest.mark.asyncio
async def test_exhausted_watch_stream_reopens_watcher_and_redelivers():
    """An exhausted event stream must reopen the watcher, not hang the loop on StopIteration."""
    handled = asyncio.Event()
    first = MagicMock()
    first.continue_watching.return_value = iter([])
    second = MagicMock()
    second.continue_watching.return_value = iter([_make_full_sync_event({'main.sml': '# one'})])
    client = MagicMock()
    client.get_watcher.return_value = second
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=client)
    provider.set_sources_watcher(handled.set)
    provider._watcher = first

    provider._watcher_task = asyncio.create_task(provider._watch_loop())
    await asyncio.wait_for(handled.wait(), timeout=5)
    await provider.stop()


@pytest.mark.asyncio
async def test_unapplied_sources_are_retried_by_rereading_etcd():
    """Opt-in retry: the watcher mux drops an unchanged re-delivery, so the provider re-reads etcd itself."""
    event = _make_full_sync_event({'main.sml': '# flaky'})
    client = MagicMock()
    client.get.return_value.node = MagicMock(key='/test/key', value=event.value)
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=client, unapplied_retry_seconds=0.01)
    applied = asyncio.Event()
    calls = []

    def callback() -> None:
        calls.append(1)
        if len(calls) == 2:
            provider.mark_sources_applied(provider.get_current_sources().hash())
            applied.set()

    provider.set_sources_watcher(callback)
    await provider._handle_event_safely(event)
    provider._schedule_unapplied_retry()
    await asyncio.wait_for(applied.wait(), timeout=5)
    assert len(calls) == 2
    await provider.stop()


@pytest.mark.asyncio
async def test_retries_stop_after_max_attempts_until_a_different_hash_arrives():
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock(), max_attempts_per_hash=2)
    callback = MagicMock()
    provider.set_sources_watcher(callback)
    broken = _make_full_sync_event({'main.sml': '# broken'})

    for _ in range(4):
        await provider._handle_event(broken)
    assert callback.call_count == 2

    await provider._handle_event(_make_full_sync_event({'main.sml': '# other'}))
    assert callback.call_count == 3


@pytest.mark.asyncio
async def test_callback_exception_clears_applied_hash_so_the_hash_is_retried():
    """The engine can mark applied, then raise in its tail; what is applied is unknown, so nothing may be skipped."""
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock())
    calls = []

    def callback() -> None:
        calls.append(1)
        if len(calls) == 1:
            provider.mark_sources_applied(provider.get_current_sources().hash())
            raise RuntimeError('tail failed')

    provider.set_sources_watcher(callback)
    event = _make_full_sync_event({'main.sml': '# swapped'})

    with pytest.raises(RuntimeError):
        await provider._handle_event(event)
    await provider._handle_event(event)
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_redelivery_of_applied_hash_resets_current_sources():
    """A rollback to the applied hash after a failed compile must make it the desired sources again."""
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock())
    provider.set_sources_watcher(MagicMock())
    applied = _make_full_sync_event({'main.sml': '# applied'})
    await provider._handle_event(applied)
    provider.mark_sources_applied(provider.get_current_sources().hash())
    await provider._handle_event(_make_full_sync_event({'main.sml': '# broken'}))
    assert provider.get_current_sources().hash() != provider._applied_sources_hash

    await provider._handle_event(applied)

    assert provider.get_current_sources().hash() == provider._applied_sources_hash


@pytest.mark.asyncio
async def test_rollback_resets_attempts_so_a_rolled_forward_hash_is_tried_again():
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock(), max_attempts_per_hash=1)
    callback = MagicMock()
    provider.set_sources_watcher(callback)
    h = _make_full_sync_event({'main.sml': '# h'})
    h2 = _make_full_sync_event({'main.sml': '# h2'})
    await provider._handle_event(h)
    provider.mark_sources_applied(provider.get_current_sources().hash())
    await provider._handle_event(h2)
    await provider._handle_event(h2)
    assert callback.call_count == 2, 'h2 is exhausted after one attempt'

    await provider._handle_event(h)
    await provider._handle_event(h2)

    assert callback.call_count == 3


@pytest.mark.asyncio
async def test_new_hash_resets_retry_delay():
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock(), unapplied_retry_seconds=2)
    provider.set_sources_watcher(MagicMock())
    await provider._handle_event(_make_full_sync_event({'main.sml': '# x'}))
    provider._retry_delay = 64

    await provider._handle_event(_make_full_sync_event({'main.sml': '# y'}))

    assert provider._retry_delay == 2


@pytest.mark.asyncio
async def test_retry_is_not_scheduled_once_attempts_are_exhausted():
    """Guards against an endless etcd GET loop on a permanently broken hash."""
    provider = AsyncEtcdSourcesProvider(
        etcd_key='/test/key', etcd_client=MagicMock(), unapplied_retry_seconds=60, max_attempts_per_hash=1
    )
    provider.set_sources_watcher(MagicMock())
    await provider._handle_event(_make_full_sync_event({'main.sml': '# broken'}))

    provider._schedule_unapplied_retry()
    assert provider._retry_task is None

    provider._max_attempts_per_hash = 2
    provider._schedule_unapplied_retry()
    assert provider._retry_task is not None
    await provider.stop()


def test_fetch_current_event_is_a_plain_get_and_maps_a_missing_key():
    from osprey.worker.lib.etcd import FullSyncOne, FullSyncOneNoKey, KeyNotFoundError

    client = MagicMock()
    client.get.return_value.node = MagicMock(key='/test/key', value='{}')
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=client)
    assert provider._fetch_current_event() == FullSyncOne('/test/key', '{}')
    client.get_watcher.assert_not_called()

    client.get.side_effect = KeyNotFoundError(100, '/test/key', 1, 'Key not found')
    assert provider._fetch_current_event() == FullSyncOneNoKey('/test/key')


@pytest.mark.asyncio
async def test_retry_read_error_has_its_own_counter_and_retry_continues():
    client = MagicMock()
    reads = []

    def failing_get(_key):
        reads.append(1)
        raise RuntimeError('etcd down')

    client.get.side_effect = failing_get
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=client, unapplied_retry_seconds=0.01)
    provider.set_sources_watcher(MagicMock())
    await provider._handle_event(_make_full_sync_event({'main.sml': '# broken'}))

    with patch('osprey.async_worker.lib.etcd.sources_provider.metrics') as mock_metrics:
        provider._schedule_unapplied_retry()

        async def two_reads() -> None:
            while len(reads) < 2:
                await asyncio.sleep(0.01)

        await asyncio.wait_for(two_reads(), timeout=1)
        await provider.stop()

    names = {call.args[0] for call in mock_metrics.increment.call_args_list}
    assert names == {'osprey.sources_provider.retry_read_error'}


@pytest.mark.asyncio
async def test_retry_reads_etcd_only_after_taking_the_handler_lock():
    event = _make_full_sync_event({'main.sml': '# flaky'})
    client = MagicMock()
    client.get.return_value.node = MagicMock(key='/test/key', value=event.value)
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=client, unapplied_retry_seconds=0.01)
    provider.set_sources_watcher(lambda: provider.mark_sources_applied(provider.get_current_sources().hash()))

    await provider._handle_lock.acquire()
    task = asyncio.create_task(provider._retry_unapplied())
    await asyncio.sleep(0.1)
    assert client.get.call_count == 0

    provider._handle_lock.release()
    await task
    assert client.get.call_count == 1


@pytest.mark.asyncio
async def test_stop_awaits_the_cancelled_retry_task():
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=MagicMock())
    task = asyncio.create_task(asyncio.sleep(60))
    provider._retry_task = task

    await provider.stop()

    assert task.cancelled()
    assert provider._retry_task is None


@pytest.mark.asyncio
async def test_reopened_stream_initial_sync_keeps_the_backoff():
    """A stream that delivers one event and ends must back off exponentially, not reset to 1s each reopen."""
    event = _make_full_sync_event({'main.sml': '# one'})
    watcher = MagicMock()
    watcher.continue_watching.side_effect = lambda: iter([event])
    client = MagicMock()
    client.get_watcher.return_value = watcher
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=client)
    provider.set_sources_watcher(MagicMock())
    provider._watcher = watcher
    delays = []

    async def record_sleep(seconds: float) -> None:
        delays.append(seconds)
        if len(delays) == 3:
            raise asyncio.CancelledError

    with patch('osprey.async_worker.lib.etcd.sources_provider.asyncio.sleep', side_effect=record_sleep):
        await provider._watch_loop()

    assert delays == [1.0, 2.0, 4.0]
