"""Cloud Tasks — create a queue, enqueue tasks, list and delete them.

    uv run python examples/tasks/tasks.py
"""
import sys
import os
import base64
import json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import TASKS_BASE, PROJECT, LOCATION, client, ok

QUEUE_ID = "example-queue"


def main():
    http = client()

    parent = f"projects/{PROJECT}/locations/{LOCATION}"
    queue_name = f"{parent}/queues/{QUEUE_ID}"

    # Create queue
    ok(http.post(
        f"{TASKS_BASE}/v2/{parent}/queues",
        json={"name": queue_name},
    ))
    print(f"Created queue: {queue_name}")

    # Enqueue tasks
    tasks_to_create = [
        {"body": {"job": "send-email",    "to": "alice@example.com"}},
        {"body": {"job": "resize-image",  "file": "photo.jpg"}},
        {"body": {"job": "send-email",    "to": "bob@example.com"}},
    ]
    created = []
    for t in tasks_to_create:
        payload = base64.b64encode(json.dumps(t["body"]).encode()).decode()
        r = ok(http.post(
            f"{TASKS_BASE}/v2/{queue_name}/tasks",
            json={"task": {"httpRequest": {
                "url": "http://localhost:8080/worker",
                "httpMethod": "POST",
                "body": payload,
            }}},
        ))
        created.append(r.json()["name"])
        print(f"  Enqueued: {r.json()['name'].split('/')[-1]}")

    # List tasks
    r = ok(http.get(f"{TASKS_BASE}/v2/{queue_name}/tasks"))
    tasks = r.json().get("tasks", [])
    print(f"\nQueue depth: {len(tasks)} tasks")

    # Delete one task
    http.delete(f"{TASKS_BASE}/v2/{created[0]}")
    r = ok(http.get(f"{TASKS_BASE}/v2/{queue_name}/tasks"))
    print(f"After delete: {len(r.json().get('tasks', []))} tasks remaining")

    # Cleanup
    http.delete(f"{TASKS_BASE}/v2/{queue_name}")
    print("Deleted queue")


if __name__ == "__main__":
    main()
