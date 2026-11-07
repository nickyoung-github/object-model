import datetime as dt
from pathlib import Path
from pydantic import BaseModel, TypeAdapter
from typing import Any, Iterable, Protocol

from .persistable import Persistable
        
# These are pydantic classes for a reason - there are tools for converting pydantic to pyarrow

class MetaData(BaseModel):
    effective_time: dt.datetime
    entry_time: dt.datetime
    host: str
    user: str


class ObjectRecord(BaseModel):  # There is a reason for this
    object_id: str
    metadata: MetaData
    data: Any
    
    @property
    def registered_name(self) -> str:
        return str(Path(self.object_id).parent)
    
    
class ObjectStore(Protocol):
    def _read(self):
        ...

    def _write(self, records_by_path: dict[Path, list[dict[str, Any]]]):
        ...


    @classmethod
    def record_type(cls, typ: type[Persistable]) -> type[ObjectRecord]:
        ...

    @classmethod
    def type_adaptor(cls, typ: type[ObjectRecord]) -> TypeAdapter[ObjectRecord]:
        ...
    
    def read(
        self,
        object_ids: Iterable[Path],
        effective_time: dt.datetime = dt.datetime.max,
        entry_time: dt.datetime = dt.datetime.max
    ) -> dict[Path, Persistable]:
        ...

    def write(
        self,
        objects: Iterable[Persistable],
        effective_time: dt.datetime = dt.datetime.max
    ):
        ...
