import datetime as dt
from pathlib import Path
from pydantic import BaseModel, TypeAdapter
from typing import Any, Iterable, Protocol
from uuid import UUID

from .persistable import Persistable
        
# These are pydantic classes for a reason - there are tools for converting pydantic to pyarrow

class MetaData(BaseModel):
    effective_time: dt.datetime
    entry_time: dt.datetime
    host: str
    user: str
    transaction_id: UUID


class ObjectRecord(BaseModel):  # There is a reason for this
    object_id: Path
    metadata: MetaData
    data: Any
    
    @property
    def registered_name(self) -> str:
        return str(self.object_id.parent)
    
    
class ObjectStore(Protocol):
    @classmethod
    def record_type(cls, typ: type[Persistable]) -> type[ObjectRecord]:
        ...

    @classmethod
    def type_adaptor(cls, typ: type[ObjectRecord]) -> TypeAdapter[ObjectRecord]:
        ...
    
    def read(self, paths: Iterable[Path]) -> dict[Path, Persistable]:
        ...

    def write(
        self,
        objects: Iterable[Persistable],
        effective_time: dt.datetime = dt.datetime.max
    ):
        ...
