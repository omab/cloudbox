"""Tests for GCS Pub/Sub notification configurations."""
import base64
import json

import pytest
from fastapi.testclient import TestClient

from localgcp.services.gcs.app import app as gcs_app
from localgcp.services.pubsub.app import app as pubsub_app

GCS = TestClient(gcs_app)
PS = TestClient(pubsub_app)

PROJECT = "projects/test-proj"
TOPIC = f"{PROJECT}/topics/gcs-events"
SUB = f"{PROJECT}/subscriptions/gcs-sub"
TOPIC_RESOURCE = f"//pubsub.googleapis.com/{TOPIC}"

BUCKET = "notif-bucket"


def _setup_bucket():
    GCS.post("/storage/v1/b", json={"name": BUCKET})


def _setup_pubsub():
    PS.put(f"/v1/{TOPIC}")
    PS.put(f"/v1/{SUB}", json={"topic": TOPIC, "name": SUB})


def _upload(name: str, content: bytes = b"data"):
    return GCS.post(
        f"/upload/storage/v1/b/{BUCKET}/o?name={name}&uploadType=media",
        content=content,
        headers={"content-type": "application/octet-stream"},
    )


def _pull(max_messages: int = 5):
    r = PS.post(f"/v1/{SUB}:pull", json={"maxMessages": max_messages})
    assert r.status_code == 200
    return r.json().get("receivedMessages", [])


def _create_notif(event_types=None, prefix="", payload_format="JSON_API_V1"):
    body = {"topic": TOPIC_RESOURCE, "payload_format": payload_format}
    if event_types is not None:
        body["event_types"] = event_types
    if prefix:
        body["object_name_prefix"] = prefix
    r = GCS.post(f"/storage/v1/b/{BUCKET}/notificationConfigs", json=body)
    assert r.status_code == 200
    return r.json()


# ---------------------------------------------------------------------------
# Config CRUD
# ---------------------------------------------------------------------------


def test_create_notification_config():
    _setup_bucket()
    config = _create_notif(event_types=["OBJECT_FINALIZE"])
    assert config["id"] == "1"
    assert config["topic"] == TOPIC_RESOURCE
    assert config["event_types"] == ["OBJECT_FINALIZE"]
    assert "notificationConfigs/1" in config["selfLink"]


def test_list_notification_configs():
    _setup_bucket()
    _create_notif(event_types=["OBJECT_FINALIZE"])
    _create_notif(event_types=["OBJECT_DELETE"])
    r = GCS.get(f"/storage/v1/b/{BUCKET}/notificationConfigs")
    assert r.status_code == 200
    assert len(r.json()["items"]) == 2


def test_get_notification_config():
    _setup_bucket()
    created = _create_notif()
    r = GCS.get(f"/storage/v1/b/{BUCKET}/notificationConfigs/{created['id']}")
    assert r.status_code == 200
    assert r.json()["id"] == created["id"]


def test_delete_notification_config():
    _setup_bucket()
    created = _create_notif()
    r = GCS.delete(f"/storage/v1/b/{BUCKET}/notificationConfigs/{created['id']}")
    assert r.status_code == 204
    r = GCS.get(f"/storage/v1/b/{BUCKET}/notificationConfigs/{created['id']}")
    assert r.status_code == 404


def test_notification_ids_are_sequential():
    _setup_bucket()
    ids = [_create_notif()["id"] for _ in range(3)]
    assert ids == ["1", "2", "3"]


def test_create_notification_on_missing_bucket():
    r = GCS.post("/storage/v1/b/no-such-bucket/notificationConfigs",
                 json={"topic": TOPIC_RESOURCE, "payload_format": "JSON_API_V1"})
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Event dispatch — OBJECT_FINALIZE
# ---------------------------------------------------------------------------


def test_upload_fires_finalize_notification():
    _setup_bucket()
    _setup_pubsub()
    _create_notif(event_types=["OBJECT_FINALIZE"])

    _upload("file.txt", b"hello")

    msgs = _pull()
    assert len(msgs) == 1
    attrs = msgs[0]["message"]["attributes"]
    assert attrs["eventType"] == "OBJECT_FINALIZE"
    assert attrs["bucketId"] == BUCKET
    assert attrs["objectId"] == "file.txt"


def test_finalize_notification_payload_contains_object_metadata():
    _setup_bucket()
    _setup_pubsub()
    _create_notif(event_types=["OBJECT_FINALIZE"], payload_format="JSON_API_V1")

    _upload("meta.txt", b"body")

    msgs = _pull()
    payload = json.loads(base64.b64decode(msgs[0]["message"]["data"]))
    assert payload["name"] == "meta.txt"
    assert payload["bucket"] == BUCKET
    assert payload["size"] == "4"


def test_finalize_none_payload_sends_empty_data():
    _setup_bucket()
    _setup_pubsub()
    _create_notif(event_types=["OBJECT_FINALIZE"], payload_format="NONE")

    _upload("quiet.txt", b"x")

    msgs = _pull()
    assert len(msgs) == 1
    data = base64.b64decode(msgs[0]["message"]["data"])
    assert data == b""


# ---------------------------------------------------------------------------
# Event dispatch — OBJECT_DELETE
# ---------------------------------------------------------------------------


def test_delete_fires_delete_notification():
    _setup_bucket()
    _setup_pubsub()
    _create_notif(event_types=["OBJECT_DELETE"])

    _upload("todelete.txt")
    _pull()  # drain finalize (not subscribed to it in this config)

    GCS.delete(f"/storage/v1/b/{BUCKET}/o/todelete.txt")

    msgs = _pull()
    assert len(msgs) == 1
    assert msgs[0]["message"]["attributes"]["eventType"] == "OBJECT_DELETE"
    assert msgs[0]["message"]["attributes"]["objectId"] == "todelete.txt"


# ---------------------------------------------------------------------------
# Event dispatch — OBJECT_METADATA_UPDATE
# ---------------------------------------------------------------------------


def test_metadata_update_fires_notification():
    _setup_bucket()
    _setup_pubsub()
    _create_notif(event_types=["OBJECT_METADATA_UPDATE"])

    _upload("update-me.txt")
    _pull()  # drain finalize

    GCS.patch(
        f"/storage/v1/b/{BUCKET}/o/update-me.txt",
        json={"metadata": {"env": "test"}},
    )

    msgs = _pull()
    assert len(msgs) == 1
    assert msgs[0]["message"]["attributes"]["eventType"] == "OBJECT_METADATA_UPDATE"


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------


def test_object_name_prefix_filter():
    _setup_bucket()
    _setup_pubsub()
    _create_notif(event_types=["OBJECT_FINALIZE"], prefix="images/")

    _upload("images/photo.jpg")
    _upload("docs/readme.txt")  # should NOT match

    msgs = _pull()
    assert len(msgs) == 1
    assert msgs[0]["message"]["attributes"]["objectId"] == "images/photo.jpg"


def test_event_type_filter_suppresses_unmatched():
    _setup_bucket()
    _setup_pubsub()
    # Only interested in deletes
    _create_notif(event_types=["OBJECT_DELETE"])

    _upload("file.txt")  # OBJECT_FINALIZE — should be suppressed

    msgs = _pull()
    assert len(msgs) == 0


def test_all_event_types_when_none_specified():
    """Empty event_types list means subscribe to all events."""
    _setup_bucket()
    _setup_pubsub()
    _create_notif()  # no event_types → all

    _upload("f.txt")
    GCS.patch(f"/storage/v1/b/{BUCKET}/o/f.txt", json={"metadata": {"k": "v"}})
    GCS.delete(f"/storage/v1/b/{BUCKET}/o/f.txt")

    msgs = _pull(max_messages=10)
    event_types = {m["message"]["attributes"]["eventType"] for m in msgs}
    assert event_types == {"OBJECT_FINALIZE", "OBJECT_METADATA_UPDATE", "OBJECT_DELETE"}


def test_no_notification_when_topic_does_not_exist():
    """If the configured topic doesn't exist in Pub/Sub, dispatch is silently skipped."""
    _setup_bucket()
    # deliberately do NOT create the pubsub topic
    GCS.post(
        f"/storage/v1/b/{BUCKET}/notificationConfigs",
        json={"topic": "//pubsub.googleapis.com/projects/x/topics/ghost", "payload_format": "JSON_API_V1"},
    )
    r = _upload("silent.txt")
    assert r.status_code == 200  # upload itself must succeed


def test_multiple_configs_fan_out():
    """A single object event fans out to all matching configs."""
    _setup_bucket()
    _setup_pubsub()

    # Second topic/sub
    topic2 = f"{PROJECT}/topics/gcs-events-2"
    sub2 = f"{PROJECT}/subscriptions/gcs-sub-2"
    PS.put(f"/v1/{topic2}")
    PS.put(f"/v1/{sub2}", json={"topic": topic2, "name": sub2})

    _create_notif(event_types=["OBJECT_FINALIZE"])
    GCS.post(f"/storage/v1/b/{BUCKET}/notificationConfigs", json={
        "topic": f"//pubsub.googleapis.com/{topic2}",
        "payload_format": "JSON_API_V1",
        "event_types": ["OBJECT_FINALIZE"],
    })

    _upload("fanout.txt")

    msgs1 = _pull()
    r2 = PS.post(f"/v1/{sub2}:pull", json={"maxMessages": 5})
    msgs2 = r2.json().get("receivedMessages", [])

    assert len(msgs1) == 1
    assert len(msgs2) == 1
