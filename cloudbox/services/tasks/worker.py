"""Background asyncio worker that dispatches HTTP tasks.

The worker loop runs as a FastAPI lifespan task. It scans all RUNNING
queues every second, picks up tasks whose scheduleTime has passed, and
dispatches them via HTTP.
"""
from __future__ import annotations

import asyncio
import base64
import logging
from datetime import datetime, timezone

import httpx

from cloudbox.services.tasks.store import get_store

logger = logging.getLogger("cloudbox.tasks.worker")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _parse_dt(s: str) -> datetime:
    s = s.rstrip("Z")
    if "." in s:
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


async def dispatch_loop() -> None:
    """Run forever, dispatching tasks that are ready."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            try:
                await _tick(client)
            except Exception:
                logger.exception("Worker tick error")
            await asyncio.sleep(1.0)


async def _tick(client: httpx.AsyncClient) -> None:
    store = get_store()
    now = datetime.now(timezone.utc)

    queues = store.list("queues")
    for queue in queues:
        if queue.get("state") != "RUNNING":
            continue

        queue_name = queue["name"]
        prefix = f"{queue_name}/tasks/"
        task_keys = [k for k in store.keys("tasks") if k.startswith(prefix)]

        for task_key in task_keys:
            task = store.get("tasks", task_key)
            if task is None:
                continue

            # Check schedule time
            try:
                sched = _parse_dt(task["scheduleTime"])
            except Exception:
                sched = now

            if sched > now:
                continue

            http_req = task.get("httpRequest")
            if not http_req:
                # No HTTP target — just delete the task
                store.delete("tasks", task_key)
                continue

            await _dispatch(client, store, task_key, task, http_req)


async def _dispatch(client, store, task_key: str, task: dict, http_req: dict) -> None:
    url = http_req.get("url", "")
    method = http_req.get("httpMethod", "POST")
    headers = dict(http_req.get("headers", {}))
    body_b64 = http_req.get("body", "")
    body = base64.b64decode(body_b64) if body_b64 else b""

    now = _now()
    task["dispatchCount"] = task.get("dispatchCount", 0) + 1
    attempt = {
        "scheduleTime": task["scheduleTime"],
        "dispatchTime": now,
    }
    if not task.get("firstAttempt"):
        task["firstAttempt"] = attempt
    task["lastAttempt"] = attempt

    try:
        response = await client.request(method, url, headers=headers, content=body)
        task["responseCount"] = task.get("responseCount", 0) + 1
        task["lastAttempt"]["responseTime"] = _now()
        task["lastAttempt"]["responseStatus"] = {"code": response.status_code}

        if 200 <= response.status_code < 300:
            logger.info("Task %s dispatched successfully (%d)", task_key, response.status_code)
            store.delete("tasks", task_key)
            return

        logger.warning("Task %s returned %d", task_key, response.status_code)
    except Exception as exc:
        logger.warning("Task %s dispatch error: %s", task_key, exc)

    # Retry logic: check maxAttempts
    queue = store.get("queues", task.get("name", "").rsplit("/tasks/", 1)[0])
    max_attempts = 100
    if queue:
        max_attempts = queue.get("retryConfig", {}).get("maxAttempts", 100)

    if task["dispatchCount"] >= max_attempts:
        logger.warning("Task %s exceeded maxAttempts, dropping", task_key)
        store.delete("tasks", task_key)
        return

    store.set("tasks", task_key, task)
