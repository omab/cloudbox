"""Shared test fixtures."""
import pytest
from fastapi.testclient import TestClient

from cloudbox.services.gcs.app import app as gcs_app
from cloudbox.services.pubsub.app import app as pubsub_app
from cloudbox.services.firestore.app import app as firestore_app
from cloudbox.services.secretmanager.app import app as secretmanager_app
from cloudbox.services.tasks.app import app as tasks_app
from cloudbox.services.bigquery.app import app as bigquery_app
from cloudbox.services.spanner.app import app as spanner_app
from cloudbox.services.logging.app import app as logging_app
from cloudbox.services.scheduler.app import app as scheduler_app


@pytest.fixture(autouse=True)
def reset_stores():
    """Reset all in-memory stores before each test."""
    from cloudbox.services.gcs.store import get_store as gcs_store
    from cloudbox.services.pubsub.store import get_store as pubsub_store, _queues, _unacked, _inflight_keys, _topic_log
    from cloudbox.services.firestore.store import get_store as fs_store
    from cloudbox.services.secretmanager.store import get_store as sm_store
    from cloudbox.services.tasks.store import get_store as tasks_store
    from cloudbox.services.bigquery.engine import get_engine as bq_engine
    from cloudbox.services.spanner.engine import get_engine as spanner_engine
    from cloudbox.services.logging.store import get_store as logging_store
    from cloudbox.services.scheduler.store import get_store as scheduler_store

    gcs_store().reset()
    pubsub_store().reset()
    _queues.clear()
    _unacked.clear()
    _inflight_keys.clear()
    _topic_log.clear()
    fs_store().reset()
    sm_store().reset()
    tasks_store().reset()
    bq_engine().reset()
    spanner_engine().reset()
    logging_store().reset()
    scheduler_store().reset()
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


@pytest.fixture
def bq_client():
    return TestClient(bigquery_app)


@pytest.fixture
def spanner_client():
    return TestClient(spanner_app)


@pytest.fixture
def logging_client():
    return TestClient(logging_app)


@pytest.fixture
def scheduler_client():
    return TestClient(scheduler_app)
