# Cloud Pub/Sub

Cloudbox emulates the Pub/Sub REST API (v1) and the gRPC streaming API used by
`google-cloud-pubsub`. Both transports run concurrently and share the same in-memory state.

## Connection

| Transport | Port | Environment variable |
|---|---|---|
| gRPC | `8085` | `PUBSUB_EMULATOR_HOST=localhost:8085` |
| REST (HTTP/1.1) | `8086` | `PUBSUB_REST_HOST=localhost:8086` |

```python
from google.auth.credentials import AnonymousCredentials
from google.cloud import pubsub_v1

# gRPC transport (default)
publisher = pubsub_v1.PublisherClient(
    credentials=AnonymousCredentials(),
    client_options={"api_endpoint": "localhost:8085"},
)

subscriber = pubsub_v1.SubscriberClient(
    credentials=AnonymousCredentials(),
    client_options={"api_endpoint": "localhost:8085"},
)
```

For REST transport (useful for environments that cannot use gRPC):

```python
publisher = pubsub_v1.PublisherClient(
    credentials=AnonymousCredentials(),
    client_options={"api_endpoint": "http://localhost:8086"},
    transport="rest",
)
```

Use `sdk_compat/clients.py` for pre-configured client factories with correct Cloudbox settings.

---

## Topics

### Create topic

```
PUT /v1/projects/{project}/topics/{topic}
```

```json
{
  "labels": { "env": "local" },
  "schemaSettings": {
    "schema": "projects/local-project/schemas/my-schema",
    "encoding": "JSON"
  },
  "messageRetentionDuration": "86400s",
  "messageStoragePolicy": { "allowedPersistenceRegions": ["us-central1"] }
}
```

All fields are optional. Returns the topic resource. If the topic already exists, returns the
existing resource (idempotent).

### Get topic

```
GET /v1/projects/{project}/topics/{topic}
```

Returns the topic resource. `404` if not found.

### Patch topic

```
PATCH /v1/projects/{project}/topics/{topic}
```

```json
{ "topic": { "labels": { "updated": "true" } }, "updateMask": "labels" }
```

Updates the fields named in `updateMask`. Returns the updated topic resource.

### List topics

```
GET /v1/projects/{project}/topics?pageSize=100&pageToken=
```

Returns `{ "topics": [...], "nextPageToken": "..." }`.

### Delete topic

```
DELETE /v1/projects/{project}/topics/{topic}
```

`204` on success. `404` if not found.

---

## Publishing

### Publish messages

```
POST /v1/projects/{project}/topics/{topic}:publish
```

```json
{
  "messages": [
    {
      "data": "SGVsbG8gV29ybGQ=",
      "attributes": { "key": "value" },
      "orderingKey": "user-123"
    }
  ]
}
```

`data` must be base64-encoded. Returns the assigned message IDs:

```json
{ "messageIds": ["msg-uuid-1", "msg-uuid-2"] }
```

Each published message is fanned out to all subscriptions on the topic. Filtering,
push dispatch, BigQuery writing, and Cloud Storage writing happen synchronously (BigQuery
and GCS) or as background tasks (push).

---

## Subscriptions

### Create subscription

```
PUT /v1/projects/{project}/subscriptions/{subscription}
```

```json
{
  "topic": "projects/local-project/topics/my-topic",
  "ackDeadlineSeconds": 60,
  "retainAckedMessages": false,
  "messageRetentionDuration": "604800s",
  "filter": "attributes.env = \"prod\"",
  "enableMessageOrdering": false,
  "deadLetterPolicy": {
    "deadLetterTopic": "projects/local-project/topics/dead-letter",
    "maxDeliveryAttempts": 5
  },
  "retryPolicy": {
    "minimumBackoff": "10s",
    "maximumBackoff": "600s"
  },
  "pushConfig": {
    "pushEndpoint": "http://localhost:8080/push"
  }
}
```

All configuration fields are optional except `topic`. Returns the subscription resource. If
the subscription already exists, returns the existing resource (idempotent).

### Get subscription

```
GET /v1/projects/{project}/subscriptions/{subscription}
```

### List subscriptions

```
GET /v1/projects/{project}/subscriptions?pageSize=100&pageToken=
```

### Delete subscription

```
DELETE /v1/projects/{project}/subscriptions/{subscription}
```

`204` on success. Removes the subscription's message queue.

---

## Pulling and acknowledging

### Pull messages

```
POST /v1/projects/{project}/subscriptions/{subscription}:pull
```

```json
{ "maxMessages": 10 }
```

Returns up to `maxMessages` unacknowledged messages:

```json
{
  "receivedMessages": [
    {
      "ackId": "ack-uuid",
      "message": {
        "data": "SGVsbG8=",
        "attributes": {},
        "messageId": "msg-uuid",
        "publishTime": "2024-01-01T00:00:00Z",
        "orderingKey": ""
      },
      "deliveryAttempt": 1
    }
  ]
}
```

Pull is only available on subscriptions without a `pushConfig.pushEndpoint` set.

### Acknowledge messages

```
POST /v1/projects/{project}/subscriptions/{subscription}:acknowledge
```

```json
{ "ackIds": ["ack-uuid-1", "ack-uuid-2"] }
```

Removes the acknowledged messages from the queue. Returns `{}`.

### Modify ack deadline

```
POST /v1/projects/{project}/subscriptions/{subscription}:modifyAckDeadline
```

```json
{ "ackIds": ["ack-uuid"], "ackDeadlineSeconds": 120 }
```

Extends the deadline for messages that need more processing time. Returns `{}`.

---

## Push subscriptions

When a subscription has `pushConfig.pushEndpoint` set, Cloudbox dispatches each message to
the endpoint as a POST request immediately after publishing. The body follows the Pub/Sub
push format:

```json
{
  "message": {
    "data": "SGVsbG8=",
    "attributes": {},
    "messageId": "msg-uuid",
    "publishTime": "2024-01-01T00:00:00Z"
  },
  "subscription": "projects/local-project/subscriptions/my-sub"
}
```

If the push endpoint returns a `2xx` status, the message is acknowledged automatically.
Non-`2xx` responses re-enqueue the message for retry.

---

## Message filtering

Subscriptions accept a `filter` expression that is evaluated against each incoming message.
Messages that do not match the filter are dropped for that subscription.

Filter syntax:

```
attributes.env = "prod"
attributes.type != "debug"
hasPrefix(attributes.source, "service-")
```

Supported operators: `=`, `!=`, `>`, `<`, `>=`, `<=`, `hasPrefix()`, `NOT`, `AND`, `OR`.

---

## Message ordering

When `enableMessageOrdering: true` is set on a subscription, messages published with the
same `orderingKey` are delivered in the order they were published. Messages without an
ordering key are delivered in an unspecified order.

---

## Dead-letter policies

Subscriptions support a `deadLetterPolicy`:

```json
{
  "deadLetterTopic": "projects/local-project/topics/dead-letter",
  "maxDeliveryAttempts": 5
}
```

After `maxDeliveryAttempts` failed delivery attempts, the message is forwarded to the
dead-letter topic. The `deliveryAttempt` field on each `ReceivedMessage` tracks the current
attempt count.

---

## Retry policies

```json
{
  "retryPolicy": {
    "minimumBackoff": "10s",
    "maximumBackoff": "600s"
  }
}
```

Retry policy configuration is stored and returned in the subscription resource. The local
emulator does not enforce actual backoff delays — messages are available for re-pull
immediately after ack deadline expiry.

---

## Seek

Reset a subscription to an earlier point to replay messages.

```
POST /v1/projects/{project}/subscriptions/{subscription}:seek
```

Seek to a snapshot:

```json
{ "snapshot": "projects/local-project/snapshots/my-snap" }
```

Seek to a timestamp (RFC 3339):

```json
{ "time": "2024-01-01T00:00:00Z" }
```

Messages published to the topic after the given time are replayed into the subscription
queue from the topic log. Returns `{}`.

---

## Snapshots

Snapshots capture the state of a subscription's unacknowledged messages at a point in time.
They can be used to seek a subscription back to that point.

### Create snapshot

```
PUT /v1/projects/{project}/snapshots/{snapshot}
```

```json
{ "subscription": "projects/local-project/subscriptions/my-sub", "labels": {} }
```

### Get snapshot

```
GET /v1/projects/{project}/snapshots/{snapshot}
```

### Patch snapshot

```
PATCH /v1/projects/{project}/snapshots/{snapshot}
```

```json
{ "snapshot": { "labels": { "updated": "true" } }, "updateMask": "labels" }
```

### List snapshots

```
GET /v1/projects/{project}/snapshots?pageSize=100&pageToken=
```

### Delete snapshot

```
DELETE /v1/projects/{project}/snapshots/{snapshot}
```

---

## Schemas

Schemas enforce message structure at publish time. When a topic has `schemaSettings` set,
every published message is validated against the schema before being accepted.

### Create schema

```
POST /v1/projects/{project}/schemas
```

```json
{
  "id": "my-schema",
  "type": "AVRO",
  "definition": "{\"type\": \"record\", \"name\": \"Msg\", \"fields\": [{\"name\": \"id\", \"type\": \"string\"}]}"
}
```

Supported schema types: `AVRO`, `PROTOCOL_BUFFER`.
Supported encodings (on `schemaSettings`): `JSON`, `BINARY`, `ENCODING_UNSPECIFIED`.

### Get schema

```
GET /v1/projects/{project}/schemas/{schema}
```

### List schemas

```
GET /v1/projects/{project}/schemas?pageSize=100&pageToken=
```

### Delete schema

```
DELETE /v1/projects/{project}/schemas/{schema}
```

### Validate schema

```
POST /v1/projects/{project}/schemas:validate
```

```json
{ "schema": { "type": "AVRO", "definition": "..." } }
```

Returns `{}` if valid, or `400` with an error message.

### Validate message against schema

```
POST /v1/projects/{project}/schemas:validateMessage
```

```json
{
  "name": "projects/local-project/schemas/my-schema",
  "encoding": "JSON",
  "message": { "data": "eyJpZCI6ICIxMjMifQ==" }
}
```

Returns `{}` if the message matches the schema, or `400` if validation fails.

---

## BigQuery subscriptions

Messages can be routed directly to a BigQuery table by setting `bigqueryConfig` on the
subscription:

```json
{
  "topic": "projects/local-project/topics/my-topic",
  "bigqueryConfig": {
    "table": "local-project:my_dataset.my_table",
    "writeMetadata": true,
    "useTopicSchema": false
  }
}
```

Messages are written to the BigQuery emulator (DuckDB) synchronously at publish time.
The table reference format is `project:dataset.table`.

---

## Cloud Storage subscriptions

Messages can be batched and written to GCS by setting `cloudStorageConfig` on the
subscription:

```json
{
  "topic": "projects/local-project/topics/my-topic",
  "cloudStorageConfig": {
    "bucket": "my-bucket",
    "filenamePrefix": "output/",
    "filenameSuffix": ".json",
    "maxDuration": "300s",
    "maxBytes": "10000000",
    "avroConfig": {}
  }
}
```

Messages are written to the GCS emulator synchronously at publish time. Avro and text
formats are supported via `avroConfig` and `textConfig` respectively.

---

## gRPC streaming

The gRPC server on port 8085 supports:

- `StreamingPull` — bidirectional streaming: the subscriber sends `StreamingPullRequest`
  (with ack IDs and deadline modifications) and receives `StreamingPullResponse` (with
  batches of messages) continuously over a single connection.

The gRPC surface is compatible with the `google-cloud-pubsub` SDK's default transport.

---

## Known limitations

| Feature | Notes |
|---|---|
| Exactly-once delivery | Duplicate redelivery within an ack deadline window is possible |
| Seek to timestamp (full fidelity) | Replay is approximate — only messages in the topic log after the target timestamp |
| OIDC / SAML push auth | Push requests are sent without authentication headers |
| Subscription-level IAM | Not enforced |

---

## Examples

```bash
# Run Pub/Sub examples (requires Cloudbox running on ports 8085 / 8086)
uv run python examples/pubsub/publish_subscribe.py
uv run python examples/pubsub/batch_publish.py
```

| Example | What it demonstrates |
|---|---|
| `publish_subscribe.py` | Create topic and subscription, publish, pull, acknowledge |
| `batch_publish.py` | Publishing many messages in batches; pull and acknowledge in pages |
