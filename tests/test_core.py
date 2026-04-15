"""Unit tests for localgcp core utilities."""
import json
import math
import threading
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# core/store.py
# ---------------------------------------------------------------------------


def test_store_basic_operations():
    from localgcp.core.store import NamespacedStore

    s = NamespacedStore("test")
    assert s.get("ns", "k") is None
    s.set("ns", "k", {"v": 1})
    assert s.get("ns", "k") == {"v": 1}
    assert s.exists("ns", "k")
    assert not s.exists("ns", "missing")
    s.delete("ns", "k")
    assert s.get("ns", "k") is None


def test_store_list_and_keys():
    from localgcp.core.store import NamespacedStore

    s = NamespacedStore("test")
    s.set("ns", "a", {"x": 1})
    s.set("ns", "b", {"x": 2})
    assert len(s.list("ns")) == 2
    assert set(s.keys("ns")) == {"a", "b"}


def test_store_clear_namespace():
    from localgcp.core.store import NamespacedStore

    s = NamespacedStore("test")
    s.set("ns1", "k", {"v": 1})
    s.set("ns2", "k", {"v": 2})
    s.clear_namespace("ns1")
    assert s.list("ns1") == []
    assert s.list("ns2") == [{"v": 2}]


def test_store_reset():
    from localgcp.core.store import NamespacedStore

    s = NamespacedStore("test")
    s.set("ns1", "k1", {"v": 1})
    s.set("ns2", "k2", {"v": 2})
    s.reset()
    assert s.list("ns1") == []
    assert s.list("ns2") == []


def test_store_stats():
    from localgcp.core.store import NamespacedStore

    s = NamespacedStore("test")
    s.set("buckets", "b1", {})
    s.set("buckets", "b2", {})
    s.set("objects", "o1", {})
    stats = s.stats()
    assert stats == {"buckets": 2, "objects": 1}


def test_store_delete_missing_returns_false():
    from localgcp.core.store import NamespacedStore

    s = NamespacedStore("test")
    assert s.delete("ns", "nonexistent") is False


def test_store_persistence_writes_and_loads(tmp_path):
    from localgcp.core.store import NamespacedStore

    s = NamespacedStore("svc", data_dir=str(tmp_path))
    s.set("ns", "key1", {"val": 42})
    s.set("ns", "key2", {"val": 99})

    # Data dir was created and data.json was written
    data_file = tmp_path / "svc" / "data.json"
    assert data_file.exists()
    on_disk = json.loads(data_file.read_text())
    assert on_disk["ns"]["key1"] == {"val": 42}

    # Loading from disk: new store instance reads the same data
    s2 = NamespacedStore("svc", data_dir=str(tmp_path))
    assert s2.get("ns", "key1") == {"val": 42}
    assert s2.get("ns", "key2") == {"val": 99}


def test_store_persistence_clear_namespace(tmp_path):
    from localgcp.core.store import NamespacedStore

    s = NamespacedStore("svc", data_dir=str(tmp_path))
    s.set("ns", "k", {"v": 1})
    s.clear_namespace("ns")

    data_file = tmp_path / "svc" / "data.json"
    on_disk = json.loads(data_file.read_text())
    assert "ns" not in on_disk


def test_store_persistence_reset(tmp_path):
    from localgcp.core.store import NamespacedStore

    s = NamespacedStore("svc", data_dir=str(tmp_path))
    s.set("ns", "k", {"v": 1})
    s.reset()

    data_file = tmp_path / "svc" / "data.json"
    on_disk = json.loads(data_file.read_text())
    assert on_disk == {}


def test_store_load_missing_file_is_noop(tmp_path):
    from localgcp.core.store import NamespacedStore

    # data_dir exists but no data.json — should start empty
    s = NamespacedStore("svc", data_dir=str(tmp_path))
    assert s.list("ns") == []


def test_store_thread_safety():
    from localgcp.core.store import NamespacedStore

    s = NamespacedStore("test")
    errors = []

    def worker(i):
        try:
            for j in range(100):
                s.set("ns", f"k{i}-{j}", {"i": i, "j": j})
                s.get("ns", f"k{i}-{j}")
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []


# ---------------------------------------------------------------------------
# core/errors.py
# ---------------------------------------------------------------------------


def test_gcp_error_known_status():
    from localgcp.core.errors import gcp_error

    resp = gcp_error(404, "not found")
    assert resp.status_code == 404
    body = json.loads(resp.body)
    assert body["error"]["code"] == 404
    assert body["error"]["status"] == "NOT_FOUND"
    assert body["error"]["message"] == "not found"


def test_gcp_error_unknown_status_falls_back():
    from localgcp.core.errors import gcp_error

    resp = gcp_error(418, "teapot")
    body = json.loads(resp.body)
    assert body["error"]["status"] == "UNKNOWN"


def test_gcp_error_explicit_status_override():
    from localgcp.core.errors import gcp_error

    resp = gcp_error(400, "bad request", status="MY_STATUS")
    body = json.loads(resp.body)
    assert body["error"]["status"] == "MY_STATUS"


def test_gcp_error_handler_registered():
    """GCPError raised in a route returns a GCP-format JSON response."""
    from localgcp.core.errors import GCPError, add_gcp_exception_handler

    app = FastAPI()
    add_gcp_exception_handler(app)

    @app.get("/fail")
    async def fail():
        raise GCPError(409, "already exists")

    client = TestClient(app, raise_server_exceptions=False)
    r = client.get("/fail")
    assert r.status_code == 409
    body = r.json()
    assert body["error"]["status"] == "ALREADY_EXISTS"
    assert body["error"]["message"] == "already exists"


def test_generic_exception_handler_registered():
    """Unhandled Exception raised in a route returns 500 GCP-format JSON."""
    from localgcp.core.errors import add_gcp_exception_handler

    app = FastAPI()
    add_gcp_exception_handler(app)

    @app.get("/boom")
    async def boom():
        raise ValueError("something broke")

    client = TestClient(app, raise_server_exceptions=False)
    r = client.get("/boom")
    assert r.status_code == 500
    body = r.json()
    assert body["error"]["code"] == 500
    assert body["error"]["status"] == "INTERNAL"
    assert "something broke" in body["error"]["message"]


# ---------------------------------------------------------------------------
# core/auth.py
# ---------------------------------------------------------------------------


def test_get_project_uses_path_param():
    from fastapi import Depends
    from localgcp.core.auth import get_project

    app = FastAPI()

    @app.get("/{project}/resource")
    async def handler(project_id: str = Depends(get_project)):
        return {"project": project_id}

    client = TestClient(app)
    r = client.get("/my-project/resource")
    assert r.json() == {"project": "my-project"}


def test_get_project_falls_back_to_default():
    from fastapi import Depends
    from localgcp.core.auth import get_project
    from localgcp.config import settings

    app = FastAPI()

    @app.get("/resource")
    async def handler(project_id: str = Depends(get_project)):
        return {"project": project_id}

    client = TestClient(app)
    r = client.get("/resource")
    assert r.json() == {"project": settings.default_project}


# ---------------------------------------------------------------------------
# core/middleware.py
# ---------------------------------------------------------------------------


def test_middleware_logs_normal_request():
    from localgcp.core.middleware import add_request_logging

    app = FastAPI()
    add_request_logging(app, "test-svc")

    @app.get("/ok")
    async def ok():
        return {"status": "ok"}

    client = TestClient(app)
    r = client.get("/ok")
    assert r.status_code == 200


def test_middleware_exception_reraises():
    """The middleware catches exceptions, logs them, and re-raises."""
    from localgcp.core.middleware import add_request_logging

    app = FastAPI()
    add_request_logging(app, "test-svc")

    @app.get("/crash")
    async def crash():
        raise RuntimeError("intentional crash")

    client = TestClient(app, raise_server_exceptions=False)
    r = client.get("/crash")
    # After re-raise Starlette turns the unhandled exception into a 500
    assert r.status_code == 500


def test_middleware_warning_level_for_4xx():
    from localgcp.core.middleware import add_request_logging

    app = FastAPI()
    add_request_logging(app, "test-svc")

    @app.get("/notfound")
    async def notfound():
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=404, content={"error": "nope"})

    client = TestClient(app)
    r = client.get("/notfound")
    assert r.status_code == 404
