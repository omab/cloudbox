"""Tests for Pub/Sub message retention, snapshots, and seek."""
import base64


PROJECT = "local-project"
TOPIC = f"projects/{PROJECT}/topics/snap-topic"
SUB = f"projects/{PROJECT}/subscriptions/snap-sub"
SNAP = f"projects/{PROJECT}/snapshots/snap-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _setup(client):
    client.put(f"/v1/{TOPIC}", json={})
    client.put(f"/v1/{SUB}", json={"name": SUB, "topic": TOPIC})


def _publish(client, messages: list[str]):
    msgs = [{"data": base64.b64encode(m.encode()).decode()} for m in messages]
    r = client.post(f"/v1/{TOPIC}:publish", json={"messages": msgs})
    assert r.status_code == 200
    return r.json()["messageIds"]


def _pull_all(client, max_messages=100) -> list[tuple[str, str]]:
    """Return list of (ackId, decoded_data) tuples."""
    r = client.post(f"/v1/{SUB}:pull", json={"maxMessages": max_messages})
    received = r.json().get("receivedMessages", [])
    return [(m["ackId"], base64.b64decode(m["message"]["data"]).decode()) for m in received]


def _ack(client, ack_ids: list[str]):
    client.post(f"/v1/{SUB}:acknowledge", json={"ackIds": ack_ids})


# ---------------------------------------------------------------------------
# Topic message log / retention
# ---------------------------------------------------------------------------


def test_publish_logs_to_topic(pubsub_client):
    """Messages published via REST are stored in the topic log."""
    from localgcp.services.pubsub.store import _topic_log
    _setup(pubsub_client)
    _publish(pubsub_client, ["hello", "world"])
    assert len(_topic_log.get(TOPIC, [])) == 2


def test_topic_log_survives_subscription_ack(pubsub_client):
    """Acking a message from a subscription does not remove it from the topic log."""
    from localgcp.services.pubsub.store import _topic_log
    _setup(pubsub_client)
    _publish(pubsub_client, ["persistent"])
    pulled = _pull_all(pubsub_client)
    assert len(pulled) == 1
    _ack(pubsub_client, [pulled[0][0]])
    # Message is gone from the subscription queue but still in the topic log
    assert len(_topic_log.get(TOPIC, [])) == 1


# ---------------------------------------------------------------------------
# Snapshot CRUD
# ---------------------------------------------------------------------------


def test_create_snapshot(pubsub_client):
    _setup(pubsub_client)
    _publish(pubsub_client, ["msg1"])
    r = pubsub_client.put(f"/v1/{SNAP}", json={"subscription": SUB})
    assert r.status_code == 200
    body = r.json()
    assert body["name"] == SNAP
    assert body["topic"] == TOPIC
    assert body["expireTime"] != ""


def test_create_snapshot_with_labels(pubsub_client):
    _setup(pubsub_client)
    r = pubsub_client.put(f"/v1/{SNAP}", json={"subscription": SUB, "labels": {"env": "test"}})
    assert r.status_code == 200
    assert r.json()["labels"]["env"] == "test"


def test_create_snapshot_missing_subscription_returns_404(pubsub_client):
    r = pubsub_client.put(f"/v1/{SNAP}", json={"subscription": "projects/p/subscriptions/no-such"})
    assert r.status_code == 404


def test_get_snapshot(pubsub_client):
    _setup(pubsub_client)
    pubsub_client.put(f"/v1/{SNAP}", json={"subscription": SUB})
    r = pubsub_client.get(f"/v1/{SNAP}")
    assert r.status_code == 200
    assert r.json()["name"] == SNAP


def test_get_snapshot_missing_returns_404(pubsub_client):
    r = pubsub_client.get(f"/v1/{SNAP}")
    assert r.status_code == 404


def test_list_snapshots(pubsub_client):
    _setup(pubsub_client)
    pubsub_client.put(f"/v1/{SNAP}", json={"subscription": SUB})
    pubsub_client.put(f"/v1/projects/{PROJECT}/snapshots/snap-2", json={"subscription": SUB})
    r = pubsub_client.get(f"/v1/projects/{PROJECT}/snapshots")
    assert r.status_code == 200
    names = [s["name"] for s in r.json()["snapshots"]]
    assert SNAP in names
    assert f"projects/{PROJECT}/snapshots/snap-2" in names


def test_update_snapshot_labels(pubsub_client):
    _setup(pubsub_client)
    pubsub_client.put(f"/v1/{SNAP}", json={"subscription": SUB})
    r = pubsub_client.patch(f"/v1/{SNAP}", json={"name": SNAP, "topic": TOPIC, "labels": {"updated": "yes"}})
    assert r.status_code == 200
    assert r.json()["labels"]["updated"] == "yes"


def test_delete_snapshot(pubsub_client):
    _setup(pubsub_client)
    pubsub_client.put(f"/v1/{SNAP}", json={"subscription": SUB})
    r = pubsub_client.delete(f"/v1/{SNAP}")
    assert r.status_code == 204
    assert pubsub_client.get(f"/v1/{SNAP}").status_code == 404


def test_delete_snapshot_missing_returns_404(pubsub_client):
    r = pubsub_client.delete(f"/v1/{SNAP}")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Seek — to timestamp
# ---------------------------------------------------------------------------


def test_seek_to_time_replays_messages(pubsub_client):
    """Seeking to a past time re-enqueues messages from the topic log."""
    from datetime import datetime, timezone, timedelta
    _setup(pubsub_client)

    _publish(pubsub_client, ["before-seek"])
    # Drain the subscription
    pulled = _pull_all(pubsub_client)
    _ack(pubsub_client, [p[0] for p in pulled])

    # Publish a second message
    _publish(pubsub_client, ["after-ack"])

    # Seek to a time before any messages were published → replay everything
    past = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    r = pubsub_client.post(f"/v1/{SUB}:seek", json={"time": past})
    assert r.status_code == 200

    # Both messages should now be in the subscription queue
    replayed = _pull_all(pubsub_client)
    payloads = {p[1] for p in replayed}
    assert "before-seek" in payloads
    assert "after-ack" in payloads


def test_seek_to_future_time_clears_queue(pubsub_client):
    """Seeking to a future time leaves nothing to deliver."""
    from datetime import datetime, timezone, timedelta
    _setup(pubsub_client)
    _publish(pubsub_client, ["will-be-cleared"])

    future = (datetime.now(timezone.utc) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    r = pubsub_client.post(f"/v1/{SUB}:seek", json={"time": future})
    assert r.status_code == 200

    replayed = _pull_all(pubsub_client)
    assert replayed == []


def test_seek_missing_subscription_returns_404(pubsub_client):
    r = pubsub_client.post(
        f"/v1/projects/{PROJECT}/subscriptions/no-sub:seek",
        json={"time": "2024-01-01T00:00:00Z"},
    )
    assert r.status_code == 404


def test_seek_no_target_returns_400(pubsub_client):
    _setup(pubsub_client)
    r = pubsub_client.post(f"/v1/{SUB}:seek", json={})
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# Seek — to snapshot
# ---------------------------------------------------------------------------


def test_seek_to_snapshot_replays_backlog(pubsub_client):
    """Seek to snapshot rewinds the subscription to the snapshot's cursor."""
    _setup(pubsub_client)

    # Publish and leave unacked → snapshot captures these
    _publish(pubsub_client, ["snap-msg-1", "snap-msg-2"])

    # Create snapshot while messages are in the queue
    pubsub_client.put(f"/v1/{SNAP}", json={"subscription": SUB})

    # Drain and ack
    pulled = _pull_all(pubsub_client)
    _ack(pubsub_client, [p[0] for p in pulled])

    # Seek back to snapshot → messages should be redelivered
    r = pubsub_client.post(f"/v1/{SUB}:seek", json={"snapshot": SNAP})
    assert r.status_code == 200

    replayed = _pull_all(pubsub_client)
    payloads = {p[1] for p in replayed}
    assert "snap-msg-1" in payloads
    assert "snap-msg-2" in payloads


def test_seek_to_snapshot_missing_returns_404(pubsub_client):
    _setup(pubsub_client)
    r = pubsub_client.post(f"/v1/{SUB}:seek", json={"snapshot": f"projects/{PROJECT}/snapshots/no-such"})
    assert r.status_code == 404


def test_seek_to_snapshot_empty_queue_captures_now(pubsub_client):
    """Snapshot on empty queue captures 'now'; seeking to it yields only future messages."""
    from datetime import datetime, timezone, timedelta
    _setup(pubsub_client)
    # No messages — snapshot captures current time
    pubsub_client.put(f"/v1/{SNAP}", json={"subscription": SUB})

    # Publish after snapshot
    _publish(pubsub_client, ["after-snap"])

    # Seek to snapshot
    pubsub_client.post(f"/v1/{SUB}:seek", json={"snapshot": SNAP})

    replayed = _pull_all(pubsub_client)
    payloads = {p[1] for p in replayed}
    assert "after-snap" in payloads


# ---------------------------------------------------------------------------
# gRPC _seek / _create_snapshot unit tests
# ---------------------------------------------------------------------------


async def test_grpc_seek_to_time(reset_stores):
    """_seek handler replays messages from a timestamp."""
    from datetime import datetime, timezone, timedelta
    from localgcp.services.pubsub.grpc_server import _seek
    from localgcp.services.pubsub import store as ps_store
    from localgcp.services.pubsub.store import get_store
    from google.pubsub_v1.types import pubsub as t
    import grpc

    store = get_store()
    store.set("topics", TOPIC, {"name": TOPIC, "messageRetentionDuration": "604800s"})
    store.set("subscriptions", SUB, {
        "name": SUB, "topic": TOPIC, "ackDeadlineSeconds": 10,
        "filter": "", "enableMessageOrdering": False,
        "retainAckedMessages": False, "labels": {},
        "pushConfig": {"pushEndpoint": ""},
    })
    ps_store.ensure_queue(SUB)

    msg = {
        "data": base64.b64encode(b"grpc-msg").decode(),
        "attributes": {}, "messageId": "m1",
        "publishTime": "2024-06-01T12:00:00.000Z", "orderingKey": "",
    }
    ps_store.log_to_topic(TOPIC, msg)

    past = datetime(2024, 1, 1, tzinfo=timezone.utc)  # before the message's publishTime

    class MockContext:
        async def abort(self, code, details):
            raise RuntimeError(details)

    req = t.SeekRequest(subscription=SUB, time=past)
    resp = await _seek(req, MockContext())
    assert resp is not None

    # Message should now be in the queue
    results = ps_store.pull(SUB, 10)
    assert len(results) == 1


async def test_grpc_create_snapshot(reset_stores):
    """_create_snapshot stores a snapshot and returns it."""
    from localgcp.services.pubsub.grpc_server import _create_snapshot
    from localgcp.services.pubsub import store as ps_store
    from localgcp.services.pubsub.store import get_store
    from google.pubsub_v1.types import pubsub as t

    store = get_store()
    store.set("topics", TOPIC, {"name": TOPIC, "messageRetentionDuration": "604800s"})
    store.set("subscriptions", SUB, {
        "name": SUB, "topic": TOPIC, "ackDeadlineSeconds": 10,
        "filter": "", "enableMessageOrdering": False,
        "retainAckedMessages": False, "labels": {},
        "pushConfig": {"pushEndpoint": ""},
    })
    ps_store.ensure_queue(SUB)

    class MockContext:
        async def abort(self, code, details):
            raise RuntimeError(details)

    req = t.CreateSnapshotRequest(name=SNAP, subscription=SUB)
    resp = await _create_snapshot(req, MockContext())
    assert resp.name == SNAP
    assert resp.topic == TOPIC
    assert store.get("snapshots", SNAP) is not None
