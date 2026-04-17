from cloudbox.core.store import NamespacedStore
from cloudbox.config import settings

_store = NamespacedStore("logging", data_dir=settings.data_dir)


def get_store() -> NamespacedStore:
    return _store
