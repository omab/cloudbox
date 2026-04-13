"""Shared test fixtures."""
import pytest
from fastapi.testclient import TestClient

from localgcp.services.gcs.app import app as gcs_app
from localgcp.services.pubsub.app import app as pubsub_app
from localgcp.services.firestore.app import app as firestore_app
from localgcp.services.secretmanager.app import app as secretmanager_app
from localgcp.services.tasks.app import app as tasks_app


@pytest.fixture(autouse=True)
def reset_stores():
    """Reset all in-memory stores before each test."""
    from localgcp.services.gcs.store import get_store as gcs_store
    from localgcp.services.pubsub.store import get_store as pubsub_store, _queues, _unacked
    from localgcp.services.firestore.store import get_store as fs_store
    from localgcp.services.secretmanager.store import get_store as sm_store
    from localgcp.services.tasks.store import get_store as tasks_store

    gcs_store().reset()
    pubsub_store().reset()
    _queues.clear()
    _unacked.clear()
    fs_store().reset()
    sm_store().reset()
    tasks_store().reset()
    yield


@pytest.fixture
def gcs_client():
    return TestClient(gcs_app)


@pytest.fixture
def pubsub_client():
    return TestClient(pubsub_app)


@pytest.fixture
def firestore_client():
    return TestClient(firestore_app)


@pytest.fixture
def sm_client():
    return TestClient(secretmanager_app)


@pytest.fixture
def tasks_client():
    return TestClient(tasks_app)
