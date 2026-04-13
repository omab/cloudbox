"""Tests for Cloud Pub/Sub emulator."""
import base64


PROJECT = "projects/local-project"


def test_create_and_get_topic(pubsub_client):
    r = pubsub_client.put(f"/v1/{PROJECT}/topics/my-topic")
    assert r.status_code == 200
    assert r.json()["name"] == f"{PROJECT}/topics/my-topic"

    r = pubsub_client.get(f"/v1/{PROJECT}/topics/my-topic")
    assert r.status_code == 200


def test_list_topics(pubsub_client):
    pubsub_client.put(f"/v1/{PROJECT}/topics/t1")
    pubsub_client.put(f"/v1/{PROJECT}/topics/t2")
    r = pubsub_client.get(f"/v1/{PROJECT}/topics")
    assert r.status_code == 200
    names = [t["name"] for t in r.json()["topics"]]
    assert f"{PROJECT}/topics/t1" in names
    assert f"{PROJECT}/topics/t2" in names


def test_publish_and_pull(pubsub_client):
    topic = f"{PROJECT}/topics/events"
    sub = f"{PROJECT}/subscriptions/events-sub"

    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{sub}", json={"name": sub, "topic": topic})

    data = base64.b64encode(b"hello pubsub").decode()
    r = pubsub_client.post(f"/v1/{topic}:publish", json={"messages": [{"data": data}]})
    assert r.status_code == 200
    assert len(r.json()["messageIds"]) == 1

    r = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 10})
    assert r.status_code == 200
    msgs = r.json()["receivedMessages"]
    assert len(msgs) == 1
    assert msgs[0]["message"]["data"] == data


def test_acknowledge(pubsub_client):
    topic = f"{PROJECT}/topics/ack-topic"
    sub = f"{PROJECT}/subscriptions/ack-sub"

    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{sub}", json={"name": sub, "topic": topic})
    pubsub_client.post(f"/v1/{topic}:publish", json={"messages": [{"data": "dGVzdA=="}]})

    r = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 1})
    ack_id = r.json()["receivedMessages"][0]["ackId"]

    r = pubsub_client.post(f"/v1/{sub}:acknowledge", json={"ackIds": [ack_id]})
    assert r.status_code == 200

    # After ack, queue should be empty
    r = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 1})
    assert r.json()["receivedMessages"] == []


def test_publish_fanout(pubsub_client):
    topic = f"{PROJECT}/topics/fanout"
    sub1 = f"{PROJECT}/subscriptions/fan-sub1"
    sub2 = f"{PROJECT}/subscriptions/fan-sub2"

    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{sub1}", json={"name": sub1, "topic": topic})
    pubsub_client.put(f"/v1/{sub2}", json={"name": sub2, "topic": topic})

    pubsub_client.post(f"/v1/{topic}:publish", json={"messages": [{"data": "dA=="}]})

    r1 = pubsub_client.post(f"/v1/{sub1}:pull", json={"maxMessages": 1})
    r2 = pubsub_client.post(f"/v1/{sub2}:pull", json={"maxMessages": 1})
    assert len(r1.json()["receivedMessages"]) == 1
    assert len(r2.json()["receivedMessages"]) == 1


def test_delete_topic_removes_subscriptions(pubsub_client):
    topic = f"{PROJECT}/topics/del-topic"
    sub = f"{PROJECT}/subscriptions/del-sub"
    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{sub}", json={"name": sub, "topic": topic})
    pubsub_client.delete(f"/v1/{topic}")

    r = pubsub_client.get(f"/v1/{sub}")
    assert r.status_code == 404
