"""Unit tests for the Cloud Tasks background worker."""
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock


PROJECT = "local-project"
LOCATION = "us-central1"


def _queue_name(qid="test-q"):
    return f"projects/{PROJECT}/locations/{LOCATION}/queues/{qid}"


def _task_name(qid="test-q", tid="task-1"):
    return f"{_queue_name(qid)}/tasks/{tid}"


def _setup_queue(store, qid="test-q", state="RUNNING", max_attempts=100):
    name = _queue_name(qid)
    store.set("queues", name, {
        "name": name,
        "state": state,
        "rateLimits": {},
        "retryConfig": {"maxAttempts": max_attempts},
    })
    return name


def _setup_task(store, qid="test-q", tid="task-1", url="http://example.com/work",
                schedule_offset_secs=-1):
    now = datetime.now(timezone.utc)
    sched = (now + timedelta(seconds=schedule_offset_secs)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    name = _task_name(qid, tid)
    store.set("tasks", name, {
        "name": name,
        "httpRequest": {"url": url, "httpMethod": "POST", "headers": {}, "body": ""},
        "scheduleTime": sched,
        "dispatchCount": 0,
    })
    return name


# ---------------------------------------------------------------------------
# _parse_dt
# ---------------------------------------------------------------------------


def test_parse_dt_valid():
    from localgcp.services.tasks.worker import _parse_dt
    dt = _parse_dt("2024-06-01T10:00:00")
    assert dt.year == 2024
    assert dt.tzinfo == timezone.utc


def test_parse_dt_with_microseconds():
    from localgcp.services.tasks.worker import _parse_dt
    dt = _parse_dt("2024-06-01T10:00:00.123456")
    assert dt.microsecond == 123456


# ---------------------------------------------------------------------------
# _dispatch (unit)
# ---------------------------------------------------------------------------


async def test_dispatch_success_deletes_task(reset_stores):
    from localgcp.services.tasks.worker import _dispatch
    from localgcp.services.tasks.store import get_store

    store = get_store()
    _setup_queue(store)
    task_key = _setup_task(store)
    task = store.get("tasks", task_key)

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_client = AsyncMock()
    mock_client.request = AsyncMock(return_value=mock_resp)

    await _dispatch(mock_client, store, task_key, task, task["httpRequest"])

    assert store.get("tasks", task_key) is None  # deleted on success


async def test_dispatch_failure_increments_dispatch_count(reset_stores):
    from localgcp.services.tasks.worker import _dispatch
    from localgcp.services.tasks.store import get_store

    store = get_store()
    _setup_queue(store, max_attempts=5)
    task_key = _setup_task(store)
    task = store.get("tasks", task_key)

    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_client = AsyncMock()
    mock_client.request = AsyncMock(return_value=mock_resp)

    await _dispatch(mock_client, store, task_key, task, task["httpRequest"])

    updated = store.get("tasks", task_key)
    assert updated is not None  # still exists
    assert updated["dispatchCount"] == 1


async def test_dispatch_drops_task_after_max_attempts(reset_stores):
    from localgcp.services.tasks.worker import _dispatch
    from localgcp.services.tasks.store import get_store

    store = get_store()
    _setup_queue(store, max_attempts=3)
    task_key = _setup_task(store)
    task = store.get("tasks", task_key)
    task["dispatchCount"] = 2  # one away from limit

    mock_resp = MagicMock()
    mock_resp.status_code = 503
    mock_client = AsyncMock()
    mock_client.request = AsyncMock(return_value=mock_resp)

    await _dispatch(mock_client, store, task_key, task, task["httpRequest"])

    assert store.get("tasks", task_key) is None  # dropped


async def test_dispatch_connection_error_keeps_task(reset_stores):
    from localgcp.services.tasks.worker import _dispatch
    from localgcp.services.tasks.store import get_store

    store = get_store()
    _setup_queue(store)
    task_key = _setup_task(store)
    task = store.get("tasks", task_key)

    mock_client = AsyncMock()
    mock_client.request = AsyncMock(side_effect=Exception("connection refused"))

    await _dispatch(mock_client, store, task_key, task, task["httpRequest"])

    assert store.get("tasks", task_key) is not None  # kept for retry


async def test_dispatch_records_first_and_last_attempt(reset_stores):
    from localgcp.services.tasks.worker import _dispatch
    from localgcp.services.tasks.store import get_store

    store = get_store()
    _setup_queue(store, max_attempts=5)
    task_key = _setup_task(store)
    task = store.get("tasks", task_key)

    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_client = AsyncMock()
    mock_client.request = AsyncMock(return_value=mock_resp)

    await _dispatch(mock_client, store, task_key, task, task["httpRequest"])

    updated = store.get("tasks", task_key)
    assert "firstAttempt" in updated
    assert "lastAttempt" in updated
    assert "dispatchTime" in updated["lastAttempt"]


# ---------------------------------------------------------------------------
# _tick (integration)
# ---------------------------------------------------------------------------


async def test_tick_dispatches_ready_task(reset_stores):
    from localgcp.services.tasks.worker import _tick
    from localgcp.services.tasks.store import get_store

    store = get_store()
    _setup_queue(store)
    _setup_task(store, schedule_offset_secs=-10)  # overdue

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_client = AsyncMock()
    mock_client.request = AsyncMock(return_value=mock_resp)

    await _tick(mock_client)

    mock_client.request.assert_called_once()


async def test_tick_skips_future_task(reset_stores):
    from localgcp.services.tasks.worker import _tick
    from localgcp.services.tasks.store import get_store

    store = get_store()
    _setup_queue(store)
    _setup_task(store, schedule_offset_secs=3600)  # 1 hour in the future

    mock_client = AsyncMock()
    mock_client.request = AsyncMock()

    await _tick(mock_client)

    mock_client.request.assert_not_called()


async def test_tick_skips_paused_queue(reset_stores):
    from localgcp.services.tasks.worker import _tick
    from localgcp.services.tasks.store import get_store

    store = get_store()
    _setup_queue(store, state="PAUSED")
    _setup_task(store, schedule_offset_secs=-10)

    mock_client = AsyncMock()
    mock_client.request = AsyncMock()

    await _tick(mock_client)

    mock_client.request.assert_not_called()


async def test_tick_task_without_http_request_is_deleted(reset_stores):
    """Tasks with no httpRequest are silently dropped by the worker."""
    from localgcp.services.tasks.worker import _tick
    from localgcp.services.tasks.store import get_store

    store = get_store()
    _setup_queue(store)
    task_key = _setup_task(store, schedule_offset_secs=-10)
    task = store.get("tasks", task_key)
    task.pop("httpRequest")
    store.set("tasks", task_key, task)

    mock_client = AsyncMock()
    mock_client.request = AsyncMock()

    await _tick(mock_client)

    mock_client.request.assert_not_called()
    assert store.get("tasks", task_key) is None  # cleaned up
