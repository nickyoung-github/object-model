from dataclasses import dataclass
import datetime as dt
from pathlib import Path
from typing import Any, Iterable, Protocol
from uuid import UUID

from .persistable import Persistable
        

@dataclass(frozen=True, kw_only=True)
class MetaData:
    effective_time: dt.datetime
    entry_time: dt.datetime
    host: str
    user: str
    transaction_id: UUID


@dataclass(frozen=True, kw_only=True)
class ObjectRecord:
    object_id: str
    registered_name: str
    path: Path
    metadata: MetaData
    data: dict[str, Any]
    
    
class ObjectStore(Protocol):
    def read(self, paths: Iterable[Path]) -> tuple[ObjectRecord]:
        ...

    def write(self, objects: Iterable[Persistable]):
        ...
