"""Tests for Cloud Tasks emulator."""

PROJECT = "local-project"
LOCATION = "us-central1"
BASE = f"/v2/projects/{PROJECT}/locations/{LOCATION}"


def test_create_and_get_queue(tasks_client):
    r = tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/my-queue"},
    )
    assert r.status_code == 200
    assert r.json()["name"].endswith("/queues/my-queue")

    r = tasks_client.get(f"{BASE}/queues/my-queue")
    assert r.status_code == 200


def test_list_queues(tasks_client):
    for qid in ("q1", "q2"):
        tasks_client.post(
            f"{BASE}/queues",
            json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/{qid}"},
        )
    r = tasks_client.get(f"{BASE}/queues")
    assert r.status_code == 200
    names = [q["name"].split("/")[-1] for q in r.json()["queues"]]
    assert {"q1", "q2"}.issubset(set(names))


def test_duplicate_queue_returns_409(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/dup-q"},
    )
    r = tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/dup-q"},
    )
    assert r.status_code == 409


def test_create_and_list_tasks(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/work"},
    )
    r = tasks_client.post(
        f"{BASE}/queues/work/tasks",
        json={
            "task": {
                "httpRequest": {"url": "http://example.com/task", "httpMethod": "POST"},
            }
        },
    )
    assert r.status_code == 200
    task_name = r.json()["name"]

    r = tasks_client.get(f"{BASE}/queues/work/tasks")
    assert r.status_code == 200
    names = [t["name"] for t in r.json()["tasks"]]
    assert task_name in names


def test_delete_task(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/del-q"},
    )
    r = tasks_client.post(
        f"{BASE}/queues/del-q/tasks",
        json={"task": {"httpRequest": {"url": "http://example.com"}}},
    )
    task_id = r.json()["name"].split("/tasks/")[1]

    r = tasks_client.delete(f"{BASE}/queues/del-q/tasks/{task_id}")
    assert r.status_code == 200

    r = tasks_client.get(f"{BASE}/queues/del-q/tasks/{task_id}")
    assert r.status_code == 404


def test_pause_and_resume_queue(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/pausable"},
    )
    r = tasks_client.post(f"{BASE}/queues/pausable:pause")
    assert r.status_code == 200
    assert r.json()["state"] == "PAUSED"

    r = tasks_client.post(f"{BASE}/queues/pausable:resume")
    assert r.status_code == 200
    assert r.json()["state"] == "RUNNING"


def test_purge_queue(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/purgeable"},
    )
    for _ in range(3):
        tasks_client.post(
            f"{BASE}/queues/purgeable/tasks",
            json={"task": {"httpRequest": {"url": "http://example.com"}}},
        )
    tasks_client.post(f"{BASE}/queues/purgeable:purge")
    r = tasks_client.get(f"{BASE}/queues/purgeable/tasks")
    assert r.json()["tasks"] == []


def test_delete_queue_removes_tasks(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/cascade-q"},
    )
    tasks_client.post(
        f"{BASE}/queues/cascade-q/tasks",
        json={"task": {"httpRequest": {"url": "http://example.com"}}},
    )
    tasks_client.delete(f"{BASE}/queues/cascade-q")
    r = tasks_client.get(f"{BASE}/queues/cascade-q")
    assert r.status_code == 404
