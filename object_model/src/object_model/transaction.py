from dataclasses import dataclass
import datetime as dt
from pathlib import Path
from typing import Iterable

from .persistable import Persistable
from .store import ObjectRecord, ObjectStore
from .type_registry import TypeRegistry


@dataclass(frozen=True, kw_only=True)
class DeferredReadTarget:
    attribute: str
    target: Persistable

    def set_value(self, value: Persistable):
        setattr(self.target, self.attribute, value)
        

class Transaction:
    __record_types: dict[type[Persistable], type[ObjectRecord]] = {}

    def __init__(
        self,
        store: ObjectStore
     ):
        self.__store = store
        self.__type_registry = TypeRegistry[Persistable]()
        self.__deferred_writes: dict[dt.datetime, list[Persistable]] | None = None

    def __enter__(self):
        self.__deferred_writes = {}

    def __exit__(self):
        if self.__deferred_writes is not None:
            for effective_time, objects in self.__deferred_writes.items():
                self.__store.write(objects, effective_time=effective_time)
            self.__deferred_writes = None

    def read(self, typ: type[Persistable], object_ids: Iterable[str]) -> tuple[Persistable, ...]:
        ...

    def write(self, objects: Iterable[Persistable], effective_time: dt.datetime = dt.datetime.max):
        write_records = self.__deferred_writes if self.__deferred_writes is not None else {}

        for obj in objects:
            write_records.setdefault(effective_time, []).append(obj)

        if self.__deferred_writes is None:
            for effective_time, objects in write_records.items():
                self.__store.write(objects, effective_time=effective_time)
