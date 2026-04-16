"""Cloud Pub/Sub emulator.

Implements the Pub/Sub REST API v1 used by google-cloud-pubsub.

Route design: use concrete path patterns (topics vs subscriptions) instead
of catch-alls so FastAPI can route correctly.
"""
from __future__ import annotations

import base64
import logging
import uuid
from datetime import datetime, timezone

import httpx
from fastapi import BackgroundTasks, FastAPI, Response
from fastapi.responses import JSONResponse

from localgcp.core.errors import GCPError, add_gcp_exception_handler
from localgcp.core.middleware import add_request_logging
from localgcp.services.pubsub import store as ps_store
from localgcp.services.pubsub.filter import matches as filter_matches
from localgcp.services.pubsub.models import (
    AcknowledgeRequest,
    CreateSnapshotRequest,
    CreateTopicBody,
    ModifyAckDeadlineRequest,
    PublishRequest,
    PublishResponse,
    PullRequest,
    PullResponse,
    PubsubMessage,
    ReceivedMessage,
    SchemaListResponse,
    SchemaModel,
    SeekRequest,
    SnapshotListResponse,
    SnapshotModel,
    SubscriptionListResponse,
    SubscriptionModel,
    TopicListResponse,
    TopicModel,
    ValidateMessageRequest,
    ValidateSchemaRequest,
    validate_message_against_schema,
    validate_schema_definition,
)

app = FastAPI(title="LocalGCP — Cloud Pub/Sub", version="v1")
add_gcp_exception_handler(app)
add_request_logging(app, "pubsub")

logger = logging.getLogger("localgcp.pubsub")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


async def _dispatch_push(push_endpoint: str, sub_name: str, ack_id: str, message: dict) -> None:
    """POST a message to a push subscription's endpoint.

    The payload matches the GCP Pub/Sub push message format:
        {"message": {...}, "subscription": "projects/.../subscriptions/..."}

    A 2xx response from the endpoint is treated as an acknowledgement (ack).
    Non-2xx responses and connection errors nack the message by setting its
    ack deadline to 0, making it immediately eligible for redelivery.
    """
    payload = {"message": message, "subscription": sub_name}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(push_endpoint, json=payload, timeout=10.0)
        if resp.status_code < 300:
            ps_store.acknowledge(sub_name, [ack_id])
        else:
            logger.warning("Push delivery to %s returned HTTP %d", push_endpoint, resp.status_code)
            ps_store.modify_ack_deadline(sub_name, [ack_id], 0)
    except Exception as exc:
        logger.warning("Push delivery to %s failed: %s", push_endpoint, exc)
        ps_store.modify_ack_deadline(sub_name, [ack_id], 0)


# ---------------------------------------------------------------------------
# Topics
# ---------------------------------------------------------------------------


@app.put("/v1/projects/{project}/topics/{topic_id}")
async def create_topic(project: str, topic_id: str, body: CreateTopicBody | None = None):
    full_name = f"projects/{project}/topics/{topic_id}"
    store = ps_store.get_store()
    existing = store.get("topics", full_name)
    if existing:
        return existing
    t = TopicModel(
        name=full_name,
        labels=(body.labels if body else {}),
        messageRetentionDuration=(body.messageRetentionDuration if body else "604800s"),
        schemaSettings=(body.schemaSettings if body else None),
    )
    if t.schemaSettings and t.schemaSettings.schema:
        if not store.exists("schemas", t.schemaSettings.schema):
            raise GCPError(404, f"Schema not found: {t.schemaSettings.schema}")
    store.set("topics", full_name, t.model_dump(exclude_none=False))
    return t.model_dump(exclude_none=True)


@app.patch("/v1/projects/{project}/topics/{topic_id}")
async def update_topic(project: str, topic_id: str, body: CreateTopicBody | None = None):
    full_name = f"projects/{project}/topics/{topic_id}"
    store = ps_store.get_store()
    data = store.get("topics", full_name)
    if data is None:
        raise GCPError(404, f"Topic not found: {full_name}")
    if body:
        if body.labels is not None:
            data["labels"] = body.labels
        if body.schemaSettings is not None:
            if body.schemaSettings.schema and not store.exists("schemas", body.schemaSettings.schema):
                raise GCPError(404, f"Schema not found: {body.schemaSettings.schema}")
            data["schemaSettings"] = body.schemaSettings.model_dump()
        data["messageRetentionDuration"] = body.messageRetentionDuration
    store.set("topics", full_name, data)
    return TopicModel(**data).model_dump(exclude_none=True)


@app.get("/v1/projects/{project}/topics/{topic_id}")
async def get_topic(project: str, topic_id: str):
    full_name = f"projects/{project}/topics/{topic_id}"
    store = ps_store.get_store()
    data = store.get("topics", full_name)
    if data is None:
        raise GCPError(404, f"Topic not found: {full_name}")
    return data


@app.get("/v1/projects/{project}/topics")
async def list_topics(project: str, pageSize: int = 100, pageToken: str = ""):
    store = ps_store.get_store()
    prefix = f"projects/{project}/topics/"
    items = [TopicModel(**v) for v in store.list("topics") if v["name"].startswith(prefix)]
    offset = int(pageToken) if pageToken else 0
    page = items[offset: offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(items) else None
    return TopicListResponse(topics=page, nextPageToken=next_token).model_dump(exclude_none=True)


@app.delete("/v1/projects/{project}/topics/{topic_id}", status_code=204)
async def delete_topic(project: str, topic_id: str):
    full_name = f"projects/{project}/topics/{topic_id}"
    store = ps_store.get_store()
    if not store.delete("topics", full_name):
        raise GCPError(404, f"Topic not found: {full_name}")
    # Remove all subscriptions pointing to this topic
    for sub in store.list("subscriptions"):
        if sub.get("topic") == full_name:
            ps_store.remove_queue(sub["name"])
            store.delete("subscriptions", sub["name"])
    return Response(status_code=204)


@app.post("/v1/projects/{project}/topics/{topic_id}:publish")
async def publish(
    project: str, topic_id: str, body: PublishRequest, background_tasks: BackgroundTasks
):
    full_name = f"projects/{project}/topics/{topic_id}"
    store = ps_store.get_store()
    if not store.exists("topics", full_name):
        raise GCPError(404, f"Topic not found: {full_name}")

    # If the topic has schema settings, validate each message before publishing
    topic_data = store.get("topics", full_name)
    schema_settings = topic_data.get("schemaSettings") if topic_data else None
    if schema_settings and schema_settings.get("schema"):
        schema_res = schema_settings["schema"]
        encoding = schema_settings.get("encoding", "ENCODING_UNSPECIFIED")
        schema_data = store.get("schemas", schema_res)
        if schema_data is not None:
            schema_type = schema_data.get("type", "TYPE_UNSPECIFIED")
            definition = schema_data.get("definition", "")
            for raw_msg in body.messages:
                raw_data = raw_msg.get("data", "")
                try:
                    msg_bytes = base64.b64decode(raw_data) if raw_data else b""
                except Exception:
                    raise GCPError(400, "Message data is not valid base64")
                err = validate_message_against_schema(schema_type, definition, msg_bytes, encoding)
                if err:
                    raise GCPError(400, f"Message failed schema validation: {err}")

    message_ids = []
    for raw_msg in body.messages:
        msg_id = str(uuid.uuid4())
        message_ids.append(msg_id)
        msg = {
            "data": raw_msg.get("data", ""),
            "attributes": raw_msg.get("attributes", {}),
            "messageId": msg_id,
            "publishTime": _now(),
            "orderingKey": raw_msg.get("orderingKey", ""),
        }
        ps_store.log_to_topic(full_name, msg)
        for sub in store.list("subscriptions"):
            if sub.get("topic") != full_name:
                continue
            sub_name = sub["name"]
            # Apply subscription-level message filter
            if not filter_matches(sub.get("filter", ""), msg):
                continue
            push_endpoint = (sub.get("pushConfig") or {}).get("pushEndpoint", "")
            ps_store.ensure_queue(sub_name)
            ps_store.enqueue(sub_name, msg)
            if push_endpoint:
                # Pull immediately to get an ack_id so _dispatch_push can ack/nack
                pulled = ps_store.pull(sub_name, 1)
                if pulled:
                    ack_id, pulled_msg, _ = pulled[0]
                    background_tasks.add_task(_dispatch_push, push_endpoint, sub_name, ack_id, pulled_msg)

    return PublishResponse(messageIds=message_ids).model_dump()


# ---------------------------------------------------------------------------
# Subscriptions
# ---------------------------------------------------------------------------


@app.put("/v1/projects/{project}/subscriptions/{sub_id}")
async def create_subscription(project: str, sub_id: str, body: SubscriptionModel):
    full_name = f"projects/{project}/subscriptions/{sub_id}"
    store = ps_store.get_store()
    existing = store.get("subscriptions", full_name)
    if existing:
        return existing

    if not store.exists("topics", body.topic):
        raise GCPError(404, f"Topic not found: {body.topic}")

    sub = SubscriptionModel(name=full_name, **{k: v for k, v in body.model_dump().items() if k != "name"})
    store.set("subscriptions", full_name, sub.model_dump())
    ps_store.ensure_queue(full_name)
    return sub.model_dump()


@app.get("/v1/projects/{project}/subscriptions/{sub_id}")
async def get_subscription(project: str, sub_id: str):
    full_name = f"projects/{project}/subscriptions/{sub_id}"
    store = ps_store.get_store()
    data = store.get("subscriptions", full_name)
    if data is None:
        raise GCPError(404, f"Subscription not found: {full_name}")
    return data


@app.get("/v1/projects/{project}/subscriptions")
async def list_subscriptions(project: str, pageSize: int = 100, pageToken: str = ""):
    store = ps_store.get_store()
    prefix = f"projects/{project}/subscriptions/"
    items = [SubscriptionModel(**v) for v in store.list("subscriptions") if v["name"].startswith(prefix)]
    offset = int(pageToken) if pageToken else 0
    page = items[offset: offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(items) else None
    return SubscriptionListResponse(subscriptions=page, nextPageToken=next_token).model_dump(exclude_none=True)


@app.delete("/v1/projects/{project}/subscriptions/{sub_id}", status_code=204)
async def delete_subscription(project: str, sub_id: str):
    full_name = f"projects/{project}/subscriptions/{sub_id}"
    store = ps_store.get_store()
    if not store.delete("subscriptions", full_name):
        raise GCPError(404, f"Subscription not found: {full_name}")
    ps_store.remove_queue(full_name)
    return Response(status_code=204)


@app.post("/v1/projects/{project}/subscriptions/{sub_id}:pull")
async def pull_messages(project: str, sub_id: str, body: PullRequest):
    full_name = f"projects/{project}/subscriptions/{sub_id}"
    store = ps_store.get_store()
    sub_data = store.get("subscriptions", full_name)
    if sub_data is None:
        raise GCPError(404, f"Subscription not found: {full_name}")

    push_endpoint = (sub_data.get("pushConfig") or {}).get("pushEndpoint", "")
    if push_endpoint:
        raise GCPError(400, f"Subscription {full_name} is a push subscription and cannot be pulled from directly")

    ps_store.ensure_queue(full_name)
    results = ps_store.pull(full_name, body.maxMessages)

    received = [
        ReceivedMessage(
            ackId=ack_id,
            message=PubsubMessage(**msg),
            deliveryAttempt=attempt,
        )
        for ack_id, msg, attempt in results
    ]
    return PullResponse(receivedMessages=received).model_dump()


@app.post("/v1/projects/{project}/subscriptions/{sub_id}:acknowledge")
async def acknowledge(project: str, sub_id: str, body: AcknowledgeRequest):
    full_name = f"projects/{project}/subscriptions/{sub_id}"
    store = ps_store.get_store()
    if not store.exists("subscriptions", full_name):
        raise GCPError(404, f"Subscription not found: {full_name}")
    ps_store.acknowledge(full_name, body.ackIds)
    return {}


@app.post("/v1/projects/{project}/subscriptions/{sub_id}:modifyAckDeadline")
async def modify_ack_deadline(project: str, sub_id: str, body: ModifyAckDeadlineRequest):
    full_name = f"projects/{project}/subscriptions/{sub_id}"
    store = ps_store.get_store()
    if not store.exists("subscriptions", full_name):
        raise GCPError(404, f"Subscription not found: {full_name}")
    ps_store.modify_ack_deadline(full_name, body.ackIds, body.ackDeadlineSeconds)
    return {}


@app.post("/v1/projects/{project}/subscriptions/{sub_id}:seek")
async def seek(project: str, sub_id: str, body: SeekRequest):
    full_name = f"projects/{project}/subscriptions/{sub_id}"
    store = ps_store.get_store()
    sub_data = store.get("subscriptions", full_name)
    if sub_data is None:
        raise GCPError(404, f"Subscription not found: {full_name}")

    topic = sub_data["topic"]

    if body.snapshot:
        snap = store.get("snapshots", body.snapshot)
        if snap is None:
            raise GCPError(404, f"Snapshot not found: {body.snapshot}")
        since_iso = snap["snapshotTime"]
    elif body.time:
        since_iso = body.time
    else:
        raise GCPError(400, "seek requires either 'time' or 'snapshot'")

    ps_store.seek_subscription(full_name, topic, since_iso)
    return {}


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------


@app.put("/v1/projects/{project}/snapshots/{snap_id}")
async def create_snapshot(project: str, snap_id: str, body: CreateSnapshotRequest):
    snap_name = f"projects/{project}/snapshots/{snap_id}"
    store = ps_store.get_store()
    if not store.exists("subscriptions", body.subscription):
        raise GCPError(404, f"Subscription not found: {body.subscription}")
    snap = ps_store.create_snapshot(snap_name, body.subscription)
    if snap is None:
        raise GCPError(404, f"Subscription not found: {body.subscription}")
    if body.labels:
        snap["labels"] = body.labels
        store.set("snapshots", snap_name, snap)
    return SnapshotModel(**snap).model_dump()


@app.get("/v1/projects/{project}/snapshots/{snap_id}")
async def get_snapshot(project: str, snap_id: str):
    snap_name = f"projects/{project}/snapshots/{snap_id}"
    store = ps_store.get_store()
    data = store.get("snapshots", snap_name)
    if data is None:
        raise GCPError(404, f"Snapshot not found: {snap_name}")
    return SnapshotModel(**data).model_dump()


@app.get("/v1/projects/{project}/snapshots")
async def list_snapshots(project: str, pageSize: int = 100, pageToken: str = ""):
    store = ps_store.get_store()
    prefix = f"projects/{project}/snapshots/"
    items = [SnapshotModel(**v) for v in store.list("snapshots") if v["name"].startswith(prefix)]
    offset = int(pageToken) if pageToken else 0
    page = items[offset: offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(items) else None
    return SnapshotListResponse(snapshots=page, nextPageToken=next_token).model_dump(exclude_none=True)


@app.patch("/v1/projects/{project}/snapshots/{snap_id}")
async def update_snapshot(project: str, snap_id: str, body: SnapshotModel):
    snap_name = f"projects/{project}/snapshots/{snap_id}"
    store = ps_store.get_store()
    data = store.get("snapshots", snap_name)
    if data is None:
        raise GCPError(404, f"Snapshot not found: {snap_name}")
    if body.labels is not None:
        data["labels"] = body.labels
    if body.expireTime:
        data["expireTime"] = body.expireTime
    store.set("snapshots", snap_name, data)
    return SnapshotModel(**data).model_dump()


@app.delete("/v1/projects/{project}/snapshots/{snap_id}", status_code=204)
async def delete_snapshot(project: str, snap_id: str):
    snap_name = f"projects/{project}/snapshots/{snap_id}"
    store = ps_store.get_store()
    if not store.delete("snapshots", snap_name):
        raise GCPError(404, f"Snapshot not found: {snap_name}")
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

@app.post("/v1/projects/{project}/schemas")
async def create_schema(project: str, body: SchemaModel, schemaId: str = ""):
    store = ps_store.get_store()
    schema_id = schemaId or body.name.split("/")[-1] if body.name else ""
    if not schema_id:
        raise GCPError(400, "schemaId query parameter or schema name is required")
    full_name = f"projects/{project}/schemas/{schema_id}"
    existing = store.get("schemas", full_name)
    if existing:
        raise GCPError(409, f"Schema already exists: {full_name}")
    err = validate_schema_definition(body.type, body.definition)
    if err:
        raise GCPError(400, f"Invalid schema: {err}")
    schema = SchemaModel(
        name=full_name,
        type=body.type,
        definition=body.definition,
        revisionId="1",
        revisionCreateTime=_now(),
    )
    store.set("schemas", full_name, schema.model_dump())
    return schema.model_dump()


@app.get("/v1/projects/{project}/schemas/{schema_id}")
async def get_schema(project: str, schema_id: str):
    full_name = f"projects/{project}/schemas/{schema_id}"
    store = ps_store.get_store()
    data = store.get("schemas", full_name)
    if data is None:
        raise GCPError(404, f"Schema not found: {full_name}")
    return data


@app.get("/v1/projects/{project}/schemas")
async def list_schemas(project: str, pageSize: int = 100, pageToken: str = ""):
    store = ps_store.get_store()
    prefix = f"projects/{project}/schemas/"
    items = [SchemaModel(**v) for v in store.list("schemas") if v["name"].startswith(prefix)]
    offset = int(pageToken) if pageToken else 0
    page = items[offset: offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(items) else None
    return SchemaListResponse(schemas=page, nextPageToken=next_token).model_dump(exclude_none=True)


@app.delete("/v1/projects/{project}/schemas/{schema_id}", status_code=204)
async def delete_schema(project: str, schema_id: str):
    full_name = f"projects/{project}/schemas/{schema_id}"
    store = ps_store.get_store()
    if not store.delete("schemas", full_name):
        raise GCPError(404, f"Schema not found: {full_name}")
    return Response(status_code=204)


@app.post("/v1/projects/{project}/schemas:validate")
async def validate_schema_endpoint(project: str, body: ValidateSchemaRequest):
    err = validate_schema_definition(body.schema.type, body.schema.definition)
    if err:
        raise GCPError(400, f"Invalid schema: {err}")
    return {}


@app.post("/v1/projects/{project}/schemas:validateMessage")
async def validate_message_endpoint(project: str, body: ValidateMessageRequest):
    store = ps_store.get_store()

    # Resolve schema: inline or by resource name
    if body.schema:
        schema_type = body.schema.type
        definition = body.schema.definition
    elif body.name:
        schema_data = store.get("schemas", body.name)
        if schema_data is None:
            raise GCPError(404, f"Schema not found: {body.name}")
        schema_type = schema_data["type"]
        definition = schema_data["definition"]
    else:
        raise GCPError(400, "Either 'schema' or 'name' must be provided")

    try:
        msg_bytes = base64.b64decode(body.message) if body.message else b""
    except Exception:
        raise GCPError(400, "message is not valid base64")

    err = validate_message_against_schema(schema_type, definition, msg_bytes, body.encoding)
    if err:
        raise GCPError(400, f"Message failed schema validation: {err}")
    return {}
