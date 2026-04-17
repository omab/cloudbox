"""Tests for Pub/Sub schema management and validation."""
import base64
import json

PROJECT = "local-project"
PARENT = f"projects/{PROJECT}"
SCHEMA_NAME = f"{PARENT}/schemas/my-schema"
TOPIC = f"{PARENT}/topics/schema-topic"
SUB = f"{PARENT}/subscriptions/schema-sub"

AVRO_DEF = json.dumps({
    "type": "record",
    "name": "TestEvent",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "value", "type": "string"},
    ],
})

PROTO_DEF = """
syntax = "proto3";
message TestEvent {
  int32 id = 1;
  string value = 2;
}
"""


# ---------------------------------------------------------------------------
# Schema CRUD
# ---------------------------------------------------------------------------

def test_create_schema_avro(pubsub_client):
    r = pubsub_client.post(
        f"/v1/{PARENT}/schemas",
        params={"schemaId": "my-schema"},
        json={"name": SCHEMA_NAME, "type": "AVRO", "definition": AVRO_DEF},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["name"] == SCHEMA_NAME
    assert body["type"] == "AVRO"
    assert body["definition"] == AVRO_DEF
    assert body["revisionId"] == "1"


def test_create_schema_protobuf(pubsub_client):
    r = pubsub_client.post(
        f"/v1/{PARENT}/schemas",
        params={"schemaId": "proto-schema"},
        json={"name": f"{PARENT}/schemas/proto-schema", "type": "PROTOCOL_BUFFER", "definition": PROTO_DEF},
    )
    assert r.status_code == 200
    assert r.json()["type"] == "PROTOCOL_BUFFER"


def test_create_schema_duplicate_returns_409(pubsub_client):
    pubsub_client.post(
        f"/v1/{PARENT}/schemas",
        params={"schemaId": "my-schema"},
        json={"name": SCHEMA_NAME, "type": "AVRO", "definition": AVRO_DEF},
    )
    r = pubsub_client.post(
        f"/v1/{PARENT}/schemas",
        params={"schemaId": "my-schema"},
        json={"name": SCHEMA_NAME, "type": "AVRO", "definition": AVRO_DEF},
    )
    assert r.status_code == 409


def test_create_schema_invalid_avro_returns_400(pubsub_client):
    r = pubsub_client.post(
        f"/v1/{PARENT}/schemas",
        params={"schemaId": "bad-schema"},
        json={"name": f"{PARENT}/schemas/bad-schema", "type": "AVRO", "definition": "not-json{{{"},
    )
    assert r.status_code == 400


def test_create_schema_missing_schema_id_returns_400(pubsub_client):
    r = pubsub_client.post(
        f"/v1/{PARENT}/schemas",
        json={"name": "", "type": "AVRO", "definition": AVRO_DEF},
    )
    assert r.status_code == 400


def test_get_schema(pubsub_client):
    pubsub_client.post(
        f"/v1/{PARENT}/schemas",
        params={"schemaId": "my-schema"},
        json={"name": SCHEMA_NAME, "type": "AVRO", "definition": AVRO_DEF},
    )
    r = pubsub_client.get(f"/v1/{SCHEMA_NAME}")
    assert r.status_code == 200
    assert r.json()["name"] == SCHEMA_NAME


def test_get_schema_missing_returns_404(pubsub_client):
    r = pubsub_client.get(f"/v1/{PARENT}/schemas/no-such")
    assert r.status_code == 404


def test_list_schemas(pubsub_client):
    pubsub_client.post(
        f"/v1/{PARENT}/schemas",
        params={"schemaId": "schema-1"},
        json={"name": f"{PARENT}/schemas/schema-1", "type": "AVRO", "definition": AVRO_DEF},
    )
    pubsub_client.post(
        f"/v1/{PARENT}/schemas",
        params={"schemaId": "schema-2"},
        json={"name": f"{PARENT}/schemas/schema-2", "type": "PROTOCOL_BUFFER", "definition": PROTO_DEF},
    )
    r = pubsub_client.get(f"/v1/{PARENT}/schemas")
    assert r.status_code == 200
    names = [s["name"] for s in r.json()["schemas"]]
    assert f"{PARENT}/schemas/schema-1" in names
    assert f"{PARENT}/schemas/schema-2" in names


def test_delete_schema(pubsub_client):
    pubsub_client.post(
        f"/v1/{PARENT}/schemas",
        params={"schemaId": "my-schema"},
        json={"name": SCHEMA_NAME, "type": "AVRO", "definition": AVRO_DEF},
    )
    r = pubsub_client.delete(f"/v1/{SCHEMA_NAME}")
    assert r.status_code == 204
    assert pubsub_client.get(f"/v1/{SCHEMA_NAME}").status_code == 404


def test_delete_schema_missing_returns_404(pubsub_client):
    r = pubsub_client.delete(f"/v1/{PARENT}/schemas/no-such")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# ValidateSchema
# ---------------------------------------------------------------------------

def test_validate_schema_valid_avro(pubsub_client):
    r = pubsub_client.post(
        f"/v1/{PARENT}/schemas:validate",
        json={"schema": {"name": "", "type": "AVRO", "definition": AVRO_DEF}},
    )
    assert r.status_code == 200


def test_validate_schema_invalid_avro_returns_400(pubsub_client):
    r = pubsub_client.post(
        f"/v1/{PARENT}/schemas:validate",
        json={"schema": {"name": "", "type": "AVRO", "definition": "{invalid json"}},
    )
    assert r.status_code == 400


def test_validate_schema_protobuf_accepted(pubsub_client):
    r = pubsub_client.post(
        f"/v1/{PARENT}/schemas:validate",
        json={"schema": {"name": "", "type": "PROTOCOL_BUFFER", "definition": PROTO_DEF}},
    )
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# ValidateMessage
# ---------------------------------------------------------------------------

def _b64(data: str) -> str:
    return base64.b64encode(data.encode()).decode()


def test_validate_message_valid_json_encoding(pubsub_client):
    msg = json.dumps({"id": 1, "value": "hello"})
    r = pubsub_client.post(
        f"/v1/{PARENT}/schemas:validateMessage",
        json={
            "schema": {"name": "", "type": "AVRO", "definition": AVRO_DEF},
            "message": _b64(msg),
            "encoding": "JSON",
        },
    )
    assert r.status_code == 200


def test_validate_message_invalid_json_returns_400(pubsub_client):
    r = pubsub_client.post(
        f"/v1/{PARENT}/schemas:validateMessage",
        json={
            "schema": {"name": "", "type": "AVRO", "definition": AVRO_DEF},
            "message": _b64("not-json{{{"),
            "encoding": "JSON",
        },
    )
    assert r.status_code == 400


def test_validate_message_by_schema_name(pubsub_client):
    pubsub_client.post(
        f"/v1/{PARENT}/schemas",
        params={"schemaId": "my-schema"},
        json={"name": SCHEMA_NAME, "type": "AVRO", "definition": AVRO_DEF},
    )
    msg = json.dumps({"id": 42, "value": "test"})
    r = pubsub_client.post(
        f"/v1/{PARENT}/schemas:validateMessage",
        json={
            "name": SCHEMA_NAME,
            "message": _b64(msg),
            "encoding": "JSON",
        },
    )
    assert r.status_code == 200


def test_validate_message_missing_schema_returns_404(pubsub_client):
    r = pubsub_client.post(
        f"/v1/{PARENT}/schemas:validateMessage",
        json={
            "name": f"{PARENT}/schemas/no-such",
            "message": _b64("{}"),
            "encoding": "JSON",
        },
    )
    assert r.status_code == 404


def test_validate_message_no_schema_returns_400(pubsub_client):
    r = pubsub_client.post(
        f"/v1/{PARENT}/schemas:validateMessage",
        json={"message": _b64("{}"), "encoding": "JSON"},
    )
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# Topic with schemaSettings
# ---------------------------------------------------------------------------

def _create_schema(client):
    client.post(
        f"/v1/{PARENT}/schemas",
        params={"schemaId": "my-schema"},
        json={"name": SCHEMA_NAME, "type": "AVRO", "definition": AVRO_DEF},
    )


def test_create_topic_with_schema_settings(pubsub_client):
    _create_schema(pubsub_client)
    r = pubsub_client.put(
        f"/v1/{TOPIC}",
        json={"schemaSettings": {"schema": SCHEMA_NAME, "encoding": "JSON"}},
    )
    assert r.status_code == 200
    assert r.json()["schemaSettings"]["schema"] == SCHEMA_NAME
    assert r.json()["schemaSettings"]["encoding"] == "JSON"


def test_create_topic_unknown_schema_returns_404(pubsub_client):
    r = pubsub_client.put(
        f"/v1/{TOPIC}",
        json={"schemaSettings": {"schema": f"{PARENT}/schemas/no-such", "encoding": "JSON"}},
    )
    assert r.status_code == 404


def test_patch_topic_schema_settings(pubsub_client):
    _create_schema(pubsub_client)
    pubsub_client.put(f"/v1/{TOPIC}", json={})
    r = pubsub_client.patch(
        f"/v1/{TOPIC}",
        json={"schemaSettings": {"schema": SCHEMA_NAME, "encoding": "JSON"}},
    )
    assert r.status_code == 200
    assert r.json()["schemaSettings"]["schema"] == SCHEMA_NAME


def test_patch_topic_missing_returns_404(pubsub_client):
    r = pubsub_client.patch(f"/v1/{TOPIC}", json={})
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Publish with schema enforcement
# ---------------------------------------------------------------------------

def test_publish_valid_message_against_avro_schema(pubsub_client):
    _create_schema(pubsub_client)
    pubsub_client.put(
        f"/v1/{TOPIC}",
        json={"schemaSettings": {"schema": SCHEMA_NAME, "encoding": "JSON"}},
    )
    pubsub_client.put(f"/v1/{SUB}", json={"name": SUB, "topic": TOPIC})

    msg = json.dumps({"id": 1, "value": "hello"})
    r = pubsub_client.post(
        f"/v1/{TOPIC}:publish",
        json={"messages": [{"data": _b64(msg)}]},
    )
    assert r.status_code == 200
    assert len(r.json()["messageIds"]) == 1


def test_publish_invalid_json_message_against_avro_schema_returns_400(pubsub_client):
    _create_schema(pubsub_client)
    pubsub_client.put(
        f"/v1/{TOPIC}",
        json={"schemaSettings": {"schema": SCHEMA_NAME, "encoding": "JSON"}},
    )
    r = pubsub_client.post(
        f"/v1/{TOPIC}:publish",
        json={"messages": [{"data": _b64("not-json{{{{")}]},
    )
    assert r.status_code == 400


def test_publish_without_schema_settings_no_validation(pubsub_client):
    """Topics without schema settings accept any data."""
    pubsub_client.put(f"/v1/{TOPIC}", json={})
    r = pubsub_client.post(
        f"/v1/{TOPIC}:publish",
        json={"messages": [{"data": _b64("arbitrary bytes")}]},
    )
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# gRPC schema unit tests
# ---------------------------------------------------------------------------

async def test_grpc_create_and_get_schema(reset_stores):
    from cloudbox.services.pubsub.grpc_server import _create_schema, _get_schema_grpc
    from cloudbox.services.pubsub.store import get_store
    from google.pubsub_v1.types import schema as st

    store = get_store()

    class MockContext:
        async def abort(self, code, details):
            raise RuntimeError(details)

    schema_obj = st.Schema(
        name=SCHEMA_NAME,
        type_=st.Schema.Type.AVRO,
        definition=AVRO_DEF,
    )
    req = st.CreateSchemaRequest(parent=PARENT, schema_id="my-schema")
    req.schema = schema_obj
    resp = await _create_schema(req, MockContext())
    assert resp.name == SCHEMA_NAME

    get_req = st.GetSchemaRequest(name=SCHEMA_NAME)
    get_resp = await _get_schema_grpc(get_req, MockContext())
    assert get_resp.name == SCHEMA_NAME


async def test_grpc_validate_schema_valid(reset_stores):
    from cloudbox.services.pubsub.grpc_server import _validate_schema_grpc
    from google.pubsub_v1.types import schema as st

    class MockContext:
        async def abort(self, code, details):
            raise RuntimeError(details)

    schema_obj = st.Schema(name="", type_=st.Schema.Type.AVRO, definition=AVRO_DEF)
    req = st.ValidateSchemaRequest(parent=PARENT, schema=schema_obj)
    resp = await _validate_schema_grpc(req, MockContext())
    assert resp is not None


async def test_grpc_validate_schema_invalid_raises(reset_stores):
    from cloudbox.services.pubsub.grpc_server import _validate_schema_grpc
    from google.pubsub_v1.types import schema as st
    import pytest

    class MockContext:
        async def abort(self, code, details):
            raise RuntimeError(details)

    schema_obj = st.Schema(name="", type_=st.Schema.Type.AVRO, definition="not-json{{{")
    req = st.ValidateSchemaRequest(parent=PARENT, schema=schema_obj)
    with pytest.raises(RuntimeError, match="Invalid"):
        await _validate_schema_grpc(req, MockContext())


async def test_grpc_validate_message_valid(reset_stores):
    from cloudbox.services.pubsub.grpc_server import _validate_message_grpc
    from google.pubsub_v1.types import schema as st

    class MockContext:
        async def abort(self, code, details):
            raise RuntimeError(details)

    msg = json.dumps({"id": 1, "value": "hello"}).encode()
    schema_obj = st.Schema(name="", type_=st.Schema.Type.AVRO, definition=AVRO_DEF)
    req = st.ValidateMessageRequest(
        parent=PARENT,
        schema=schema_obj,
        message=msg,
        encoding=st.Encoding.JSON,
    )
    resp = await _validate_message_grpc(req, MockContext())
    assert resp is not None
