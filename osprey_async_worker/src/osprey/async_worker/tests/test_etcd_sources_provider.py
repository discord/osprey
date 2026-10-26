"""Tests for the async etcd sources provider and input stream signaler."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

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
    """A re-delivery of the sources loaded at start() is a no-op: no callback."""
    payload = {'main.sml': '# noop'}
    etcd_client = MagicMock()
    etcd_client.get_watcher.return_value.begin_watching.return_value = _make_full_sync_event(payload)
    provider = AsyncEtcdSourcesProvider(etcd_key='/test/key', etcd_client=etcd_client)
    with patch.object(provider, '_watch_loop', AsyncMock()):
        await provider.start()

    callback = MagicMock()
    provider.set_sources_watcher(callback)

    await provider._handle_event(_make_full_sync_event(payload))

    callback.assert_not_called()
