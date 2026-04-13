"""End-to-end smoke tests using the real google-cloud-* SDKs.

These tests require LocalGCP to be running. Start it first:
    uv run python -m localgcp.main
    # or: docker run -p 4443:4443 -p 8080:8080 -p 8085:8085 -p 8090:8090 -p 8123:8123 localgcp

Then run:
    uv run python sdk_compat/test_with_sdk.py
"""
from __future__ import annotations

import base64
import sys


def check(label: str, condition: bool) -> None:
    status = "✓" if condition else "✗"
    print(f"  {status} {label}")
    if not condition:
        sys.exit(1)


def test_gcs() -> None:
    print("\n[Cloud Storage]")
    from sdk_compat.clients import storage_client
    client = storage_client()

    bucket = client.create_bucket("sdk-test-bucket")
    check("create bucket", bucket.name == "sdk-test-bucket")

    blob = bucket.blob("hello.txt")
    blob.upload_from_string(b"Hello from SDK!")
    check("upload object", True)

    downloaded = blob.download_as_bytes()
    check("download object matches", downloaded == b"Hello from SDK!")

    blobs = list(client.list_blobs(bucket))
    check("list objects returns 1", len(blobs) == 1)

    blob.delete()
    bucket.delete()
    check("cleanup ok", True)


def test_pubsub() -> None:
    print("\n[Cloud Pub/Sub]")
    from sdk_compat.clients import pubsub_publisher, pubsub_subscriber

    publisher = pubsub_publisher()
    subscriber = pubsub_subscriber()
    project = "local-project"

    topic_path = publisher.topic_path(project, "sdk-topic")
    sub_path = subscriber.subscription_path(project, "sdk-sub")

    publisher.create_topic(request={"name": topic_path})
    check("create topic", True)

    subscriber.create_subscription(request={"name": sub_path, "topic": topic_path})
    check("create subscription", True)

    future = publisher.publish(topic_path, data=b"hello SDK pubsub")
    msg_id = future.result(timeout=5)
    check("publish message", bool(msg_id))

    response = subscriber.pull(request={"subscription": sub_path, "max_messages": 1})
    check("pull message", len(response.received_messages) == 1)
    msg = response.received_messages[0]
    check("message data matches", msg.message.data == b"hello SDK pubsub")

    subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": [msg.ack_id]})
    check("acknowledge ok", True)

    subscriber.delete_subscription(request={"subscription": sub_path})
    publisher.delete_topic(request={"topic": topic_path})
    check("cleanup ok", True)


def test_secret_manager() -> None:
    print("\n[Secret Manager]")
    from sdk_compat.clients import secret_manager_client

    client = secret_manager_client()
    project = "local-project"

    secret = client.create_secret(
        request={
            "parent": f"projects/{project}",
            "secret_id": "sdk-secret",
            "secret": {"replication": {"automatic": {}}},
        }
    )
    check("create secret", "sdk-secret" in secret.name)

    version = client.add_secret_version(
        request={
            "parent": secret.name,
            "payload": {"data": b"my-secret-value"},
        }
    )
    check("add version", "/versions/1" in version.name)

    response = client.access_secret_version(
        request={"name": f"{secret.name}/versions/latest"}
    )
    check("access latest", response.payload.data == b"my-secret-value")

    client.delete_secret(request={"name": secret.name})
    check("cleanup ok", True)


if __name__ == "__main__":
    print("LocalGCP SDK compatibility smoke tests")
    print("=" * 40)
    try:
        test_gcs()
        test_pubsub()
        test_secret_manager()
        print("\n✓ All tests passed!")
    except SystemExit:
        print("\n✗ Tests failed!")
        sys.exit(1)
