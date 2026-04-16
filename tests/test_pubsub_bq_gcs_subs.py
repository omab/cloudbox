"""Tests for Pub/Sub BigQuery and Cloud Storage subscriptions."""
import base64
import json

PROJECT = "local-project"
PARENT = f"projects/{PROJECT}"
TOPIC = f"{PARENT}/topics/sink-topic"
BQ_SUB = f"{PARENT}/subscriptions/bq-sub"
GCS_SUB = f"{PARENT}/subscriptions/gcs-sub"
DATASET = "sink_ds"
TABLE = "sink_tbl"
BQ_TABLE_REF = f"{PROJECT}:{DATASET}.{TABLE}"
GCS_BUCKET = "sink-bucket"


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


def _publish(client, messages: list[str]):
    msgs = [{"data": _b64(m)} for m in messages]
    r = client.post(f"/v1/{TOPIC}:publish", json={"messages": msgs})
    assert r.status_code == 200
    return r.json()["messageIds"]


# ---------------------------------------------------------------------------
# Fixtures: create topic, BQ table, GCS bucket
# ---------------------------------------------------------------------------

def _setup_topic(client):
    client.put(f"/v1/{TOPIC}", json={})


def _setup_bq(dataset=DATASET, table=TABLE):
    from localgcp.services.bigquery.engine import get_engine
    engine = get_engine()
    try:
        engine.create_dataset(PROJECT, dataset, {})
    except ValueError:
        pass
    try:
        engine.create_table(PROJECT, dataset, table, {
            "schema": {"fields": [
                {"name": "data", "type": "STRING"},
            ]}
        })
    except ValueError:
        pass
    return engine


def _setup_bq_with_metadata(dataset=DATASET, table="meta_tbl"):
    from localgcp.services.bigquery.engine import get_engine
    engine = get_engine()
    try:
        engine.create_dataset(PROJECT, dataset, {})
    except ValueError:
        pass
    try:
        engine.create_table(PROJECT, dataset, table, {
            "schema": {"fields": [
                {"name": "data", "type": "STRING"},
                {"name": "subscription_name", "type": "STRING"},
                {"name": "message_id", "type": "STRING"},
                {"name": "publish_time", "type": "STRING"},
                {"name": "attributes", "type": "STRING"},
            ]}
        })
    except ValueError:
        pass
    return engine


def _setup_gcs():
    from localgcp.services.gcs.store import get_store as get_gcs_store
    from localgcp.services.gcs.models import BucketModel
    gcs_store = get_gcs_store()
    bucket = BucketModel(name=GCS_BUCKET)
    gcs_store.set("buckets", GCS_BUCKET, bucket.model_dump())
    return gcs_store


# ---------------------------------------------------------------------------
# BigQuery subscription — basic
# ---------------------------------------------------------------------------

def test_create_bq_subscription(pubsub_client):
    _setup_topic(pubsub_client)
    r = pubsub_client.put(f"/v1/{BQ_SUB}", json={
        "name": BQ_SUB,
        "topic": TOPIC,
        "bigqueryConfig": {"table": BQ_TABLE_REF},
    })
    assert r.status_code == 200
    assert r.json()["bigqueryConfig"]["table"] == BQ_TABLE_REF


def test_create_bq_subscription_invalid_table_ref_returns_400(pubsub_client):
    _setup_topic(pubsub_client)
    r = pubsub_client.put(f"/v1/{BQ_SUB}", json={
        "name": BQ_SUB,
        "topic": TOPIC,
        "bigqueryConfig": {"table": "bad-ref"},
    })
    assert r.status_code == 400


def test_publish_writes_to_bq_table(pubsub_client):
    _setup_topic(pubsub_client)
    engine = _setup_bq()
    pubsub_client.put(f"/v1/{BQ_SUB}", json={
        "name": BQ_SUB,
        "topic": TOPIC,
        "bigqueryConfig": {"table": BQ_TABLE_REF},
    })
    _publish(pubsub_client, ["hello-bq"])
    rows = engine.list_rows(PROJECT, DATASET, TABLE)
    assert int(rows["totalRows"]) == 1
    # data column contains the base64-encoded message
    assert rows["rows"][0]["f"][0]["v"] == _b64("hello-bq")


def test_publish_multiple_messages_writes_multiple_rows(pubsub_client):
    _setup_topic(pubsub_client)
    engine = _setup_bq()
    pubsub_client.put(f"/v1/{BQ_SUB}", json={
        "name": BQ_SUB,
        "topic": TOPIC,
        "bigqueryConfig": {"table": BQ_TABLE_REF},
    })
    _publish(pubsub_client, ["msg1", "msg2", "msg3"])
    rows = engine.list_rows(PROJECT, DATASET, TABLE)
    assert int(rows["totalRows"]) == 3


def test_bq_subscription_messages_not_in_pull_queue(pubsub_client):
    """BQ subscriptions bypass the pull queue."""
    _setup_topic(pubsub_client)
    _setup_bq()
    pubsub_client.put(f"/v1/{BQ_SUB}", json={
        "name": BQ_SUB,
        "topic": TOPIC,
        "bigqueryConfig": {"table": BQ_TABLE_REF},
    })
    _publish(pubsub_client, ["should-not-be-pullable"])
    r = pubsub_client.post(f"/v1/{BQ_SUB}:pull", json={"maxMessages": 10})
    assert r.status_code == 200
    assert r.json().get("receivedMessages", []) == []


def test_bq_subscription_write_metadata(pubsub_client):
    _setup_topic(pubsub_client)
    engine = _setup_bq_with_metadata()
    pubsub_client.put(f"/v1/{BQ_SUB}", json={
        "name": BQ_SUB,
        "topic": TOPIC,
        "bigqueryConfig": {
            "table": f"{PROJECT}:{DATASET}.meta_tbl",
            "writeMetadata": True,
        },
    })
    _publish(pubsub_client, ["with-meta"])
    rows = engine.list_rows(PROJECT, DATASET, "meta_tbl")
    assert int(rows["totalRows"]) == 1
    fields = {f["name"]: rows["rows"][0]["f"][i]["v"]
              for i, f in enumerate(rows["schema"]["fields"])}
    assert fields["subscription_name"] == BQ_SUB
    assert fields["message_id"] != ""
    assert fields["publish_time"] != ""
    assert fields["data"] == _b64("with-meta")


def test_bq_subscription_use_topic_schema(pubsub_client):
    """useTopicSchema decodes message JSON and maps to table columns."""
    _setup_topic(pubsub_client)
    from localgcp.services.bigquery.engine import get_engine
    engine = get_engine()
    try:
        engine.create_dataset(PROJECT, "schema_ds", {})
    except ValueError:
        pass
    try:
        engine.create_table(PROJECT, "schema_ds", "events", {
            "schema": {"fields": [
                {"name": "id", "type": "INTEGER"},
                {"name": "name", "type": "STRING"},
            ]}
        })
    except ValueError:
        pass
    pubsub_client.put(f"/v1/{BQ_SUB}", json={
        "name": BQ_SUB,
        "topic": TOPIC,
        "bigqueryConfig": {
            "table": f"{PROJECT}:schema_ds.events",
            "useTopicSchema": True,
        },
    })
    payload = json.dumps({"id": 42, "name": "test-event"})
    pubsub_client.post(f"/v1/{TOPIC}:publish", json={"messages": [{"data": _b64(payload)}]})
    rows = engine.list_rows(PROJECT, "schema_ds", "events")
    assert int(rows["totalRows"]) == 1
    fields = {f["name"]: rows["rows"][0]["f"][i]["v"]
              for i, f in enumerate(rows["schema"]["fields"])}
    assert fields["name"] == "test-event"


def test_bq_subscription_drop_unknown_fields(pubsub_client):
    """dropUnknownFields strips columns not in the table schema."""
    _setup_topic(pubsub_client)
    from localgcp.services.bigquery.engine import get_engine
    engine = get_engine()
    try:
        engine.create_dataset(PROJECT, "drop_ds", {})
    except ValueError:
        pass
    try:
        engine.create_table(PROJECT, "drop_ds", "narrow", {
            "schema": {"fields": [{"name": "data", "type": "STRING"}]}
        })
    except ValueError:
        pass
    pubsub_client.put(f"/v1/{BQ_SUB}", json={
        "name": BQ_SUB,
        "topic": TOPIC,
        "bigqueryConfig": {
            "table": f"{PROJECT}:drop_ds.narrow",
            "writeMetadata": True,
            "dropUnknownFields": True,
        },
    })
    _publish(pubsub_client, ["drop-test"])
    rows = engine.list_rows(PROJECT, "drop_ds", "narrow")
    # Only "data" column — metadata columns were dropped
    assert int(rows["totalRows"]) == 1
    assert len(rows["schema"]["fields"]) == 1
    assert rows["schema"]["fields"][0]["name"] == "data"


def test_bq_subscription_missing_table_logs_and_does_not_fail_publish(pubsub_client):
    """If the BQ table doesn't exist, publish still returns 200."""
    _setup_topic(pubsub_client)
    pubsub_client.put(f"/v1/{BQ_SUB}", json={
        "name": BQ_SUB,
        "topic": TOPIC,
        "bigqueryConfig": {"table": f"{PROJECT}:nonexistent_ds.nonexistent_tbl"},
    })
    r = pubsub_client.post(f"/v1/{TOPIC}:publish", json={
        "messages": [{"data": _b64("resilient")}]
    })
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# Cloud Storage subscription — basic
# ---------------------------------------------------------------------------

def test_create_gcs_subscription(pubsub_client):
    _setup_topic(pubsub_client)
    r = pubsub_client.put(f"/v1/{GCS_SUB}", json={
        "name": GCS_SUB,
        "topic": TOPIC,
        "cloudStorageConfig": {"bucket": GCS_BUCKET},
    })
    assert r.status_code == 200
    assert r.json()["cloudStorageConfig"]["bucket"] == GCS_BUCKET


def test_create_gcs_subscription_missing_bucket_returns_400(pubsub_client):
    _setup_topic(pubsub_client)
    r = pubsub_client.put(f"/v1/{GCS_SUB}", json={
        "name": GCS_SUB,
        "topic": TOPIC,
        "cloudStorageConfig": {"bucket": ""},
    })
    assert r.status_code == 400


def test_publish_writes_gcs_object_text(pubsub_client):
    _setup_topic(pubsub_client)
    gcs_store = _setup_gcs()
    pubsub_client.put(f"/v1/{GCS_SUB}", json={
        "name": GCS_SUB,
        "topic": TOPIC,
        "cloudStorageConfig": {"bucket": GCS_BUCKET},
    })
    _publish(pubsub_client, ["hello-gcs"])
    objects = [o for o in gcs_store.list("objects") if o.get("bucket") == GCS_BUCKET]
    assert len(objects) == 1
    key = f"{GCS_BUCKET}/{objects[0]['name']}"
    body = gcs_store.get("bodies", key)
    assert body == b"hello-gcs"


def test_publish_multiple_messages_writes_multiple_objects(pubsub_client):
    _setup_topic(pubsub_client)
    gcs_store = _setup_gcs()
    pubsub_client.put(f"/v1/{GCS_SUB}", json={
        "name": GCS_SUB,
        "topic": TOPIC,
        "cloudStorageConfig": {"bucket": GCS_BUCKET},
    })
    _publish(pubsub_client, ["a", "b", "c"])
    objects = [o for o in gcs_store.list("objects") if o.get("bucket") == GCS_BUCKET]
    assert len(objects) == 3


def test_gcs_subscription_messages_not_in_pull_queue(pubsub_client):
    _setup_topic(pubsub_client)
    _setup_gcs()
    pubsub_client.put(f"/v1/{GCS_SUB}", json={
        "name": GCS_SUB,
        "topic": TOPIC,
        "cloudStorageConfig": {"bucket": GCS_BUCKET},
    })
    _publish(pubsub_client, ["should-not-be-pullable"])
    r = pubsub_client.post(f"/v1/{GCS_SUB}:pull", json={"maxMessages": 10})
    assert r.status_code == 200
    assert r.json().get("receivedMessages", []) == []


def test_gcs_subscription_filename_prefix_and_suffix(pubsub_client):
    _setup_topic(pubsub_client)
    gcs_store = _setup_gcs()
    pubsub_client.put(f"/v1/{GCS_SUB}", json={
        "name": GCS_SUB,
        "topic": TOPIC,
        "cloudStorageConfig": {
            "bucket": GCS_BUCKET,
            "filenamePrefix": "events/",
            "filenameSuffix": ".txt",
        },
    })
    _publish(pubsub_client, ["prefixed"])
    objects = [o for o in gcs_store.list("objects") if o.get("bucket") == GCS_BUCKET]
    assert len(objects) == 1
    assert objects[0]["name"].startswith("events/")
    assert objects[0]["name"].endswith(".txt")


def test_gcs_subscription_avro_config_writes_json_record(pubsub_client):
    _setup_topic(pubsub_client)
    gcs_store = _setup_gcs()
    pubsub_client.put(f"/v1/{GCS_SUB}", json={
        "name": GCS_SUB,
        "topic": TOPIC,
        "cloudStorageConfig": {
            "bucket": GCS_BUCKET,
            "avroConfig": {"writeMetadata": False},
        },
    })
    _publish(pubsub_client, ["avro-msg"])
    objects = [o for o in gcs_store.list("objects") if o.get("bucket") == GCS_BUCKET]
    assert len(objects) == 1
    key = f"{GCS_BUCKET}/{objects[0]['name']}"
    body = gcs_store.get("bodies", key)
    record = json.loads(body)
    assert record["data"] == _b64("avro-msg")
    assert "subscription_name" not in record


def test_gcs_subscription_avro_with_metadata(pubsub_client):
    _setup_topic(pubsub_client)
    gcs_store = _setup_gcs()
    pubsub_client.put(f"/v1/{GCS_SUB}", json={
        "name": GCS_SUB,
        "topic": TOPIC,
        "cloudStorageConfig": {
            "bucket": GCS_BUCKET,
            "avroConfig": {"writeMetadata": True},
        },
    })
    _publish(pubsub_client, ["with-meta"])
    objects = [o for o in gcs_store.list("objects") if o.get("bucket") == GCS_BUCKET]
    key = f"{GCS_BUCKET}/{objects[0]['name']}"
    record = json.loads(gcs_store.get("bodies", key))
    assert record["subscription_name"] == GCS_SUB
    assert record["message_id"] != ""
    assert record["publish_time"] != ""
    assert record["data"] == _b64("with-meta")


def test_gcs_subscription_missing_bucket_logs_and_does_not_fail_publish(pubsub_client):
    """If the GCS bucket doesn't exist, publish still returns 200."""
    _setup_topic(pubsub_client)
    pubsub_client.put(f"/v1/{GCS_SUB}", json={
        "name": GCS_SUB,
        "topic": TOPIC,
        "cloudStorageConfig": {"bucket": "nonexistent-bucket"},
    })
    r = pubsub_client.post(f"/v1/{TOPIC}:publish", json={
        "messages": [{"data": _b64("resilient")}]
    })
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# Mixed: BQ sub + normal pull sub on the same topic
# ---------------------------------------------------------------------------

def test_bq_sub_and_pull_sub_coexist(pubsub_client):
    """BQ subscription writes to BQ; pull subscription still receives messages."""
    _setup_topic(pubsub_client)
    engine = _setup_bq()
    pull_sub = f"{PARENT}/subscriptions/pull-sub"

    pubsub_client.put(f"/v1/{BQ_SUB}", json={
        "name": BQ_SUB, "topic": TOPIC,
        "bigqueryConfig": {"table": BQ_TABLE_REF},
    })
    pubsub_client.put(f"/v1/{pull_sub}", json={"name": pull_sub, "topic": TOPIC})

    _publish(pubsub_client, ["shared-message"])

    # BQ got it
    rows = engine.list_rows(PROJECT, DATASET, TABLE)
    assert int(rows["totalRows"]) == 1

    # Pull sub also got it
    r = pubsub_client.post(f"/v1/{pull_sub}:pull", json={"maxMessages": 10})
    assert len(r.json()["receivedMessages"]) == 1


def test_gcs_sub_and_pull_sub_coexist(pubsub_client):
    """GCS subscription writes to GCS; pull subscription still receives messages."""
    _setup_topic(pubsub_client)
    gcs_store = _setup_gcs()
    pull_sub = f"{PARENT}/subscriptions/pull-sub"

    pubsub_client.put(f"/v1/{GCS_SUB}", json={
        "name": GCS_SUB, "topic": TOPIC,
        "cloudStorageConfig": {"bucket": GCS_BUCKET},
    })
    pubsub_client.put(f"/v1/{pull_sub}", json={"name": pull_sub, "topic": TOPIC})

    _publish(pubsub_client, ["shared-message"])

    # GCS got it
    objects = [o for o in gcs_store.list("objects") if o.get("bucket") == GCS_BUCKET]
    assert len(objects) == 1

    # Pull sub also got it
    r = pubsub_client.post(f"/v1/{pull_sub}:pull", json={"maxMessages": 10})
    assert len(r.json()["receivedMessages"]) == 1
