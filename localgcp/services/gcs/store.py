"""GCS in-memory store.

Stores:
  buckets  → bucket_name → BucketModel dict
  objects  → "{bucket}/{object}" → ObjectModel dict
  bodies   → "{bucket}/{object}" → bytes
"""
from localgcp.config import settings
from localgcp.core.store import NamespacedStore

_store = NamespacedStore("gcs", settings.data_dir)


def get_store() -> NamespacedStore:
    return _store
