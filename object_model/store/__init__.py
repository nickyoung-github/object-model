from .exception import (
    FailedUpdateError,
    NotFoundError,
    ObjectStoreError,
    WrongStoreError
)
from .object_result import ObjectResult
from .sql_store import MemoryStore, SqlStore, TempStore
from .union_store import UnionStore
from .web_client import WebStoreClient
