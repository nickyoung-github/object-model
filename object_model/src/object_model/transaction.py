from dataclasses import dataclass
import datetime as dt
from pathlib import Path
from typing import Any, Iterable
from uuid import UUID, uuid4

from .persistable import Persistable
from .serialisation import dump, validate
from .store import ObjectStore
from .type_registry import TypeRegistry


@dataclass(frozen=True, kw_only=True)
class MetaData:
    effective_time: dt.datetime
    entry_time: dt.datetime
    host: str
    user: str
    transaction_id: UUID


@dataclass(frozen=True, kw_only=True)
class Record:
    object_id: str
    registered_name: str
    path: Path
    metadata: MetaData
    data: dict[str, Any]


@dataclass(frozen=True, kw_only=True)
class DeferredReadTarget:
    attribute: str
    target: Persistable

    def set_value(self, value: Persistable):
        setattr(self.target, self.attribute, value)
        

class Transaction:
    def __init__(self,
                 store: ObjectStore,
                 effective_time: dt.datetime = dt.datetime.max,
                 entry_time: dt.datetime = dt.datetime.max
                 ):
        self.__store = store
        self.__effective_time = effective_time
        self.__entry_time = entry_time
        self.__type_registry = TypeRegistry[Persistable]()
        self.__uuid = uuid4()
        self.__deferred_reads: dict[str, DeferredReadTarget] | None = None
        self.__deferred_writes: list[Persistable] | None = None

    def __enter__(self):
        self.__deferred_reads = {}
        self.__deferred_writes = {}

    def __exit__(self):
        self.__handle_writes(self.__deferred_writes)
        self.__write_records = None

        self.__handle_reads(self.__deferred_reads)
        self.__deferred_reads = None

    def __handle_reads(self, object_ids: Iterable[str]) -> dict[str, Persistable]:
        # ToDo: The query
        records: list[Record] = []
        ret = {}

        for r in records:
            typ = self.__type_registry.get_type(r.registered_name)
            obj = validate(typ, r.data, self.__store)

            try:
                self.__deferred_reads.pop(r.object_id).set_value(obj)
            except KeyError:
                pass

            ret[r.object_id] = obj

        missing = [i for i in object_ids if i not in ret]
        if missing:
            raise RuntimeError(f"Failed to load: {missing}")

        return ret

    def __handle_writes(self, write_records: list[Persistable]):
        entry_time = dt.datetime.now(dt.UTC)
        metadata = MetaData(
            effective_time=self.__effective_time if self.__effective_time != dt.datetime.max else entry_time,
            entry_time=entry_time,
            host="nick-host",
            user="nick",
            transaction_id=self.__uuid
        )

        records = []

        for obj in write_records:
            typ = type(obj)
            record = Record(
                object_id=obj.object_id,
                registered_name=self.__type_registry.registered_name(typ),
                path=self.__type_registry.registered_path(typ),
                metadata=metadata,
                data=dump(obj, self.__store)
            )
            records.append(record)

    def read(self, object_ids: Iterable[str]) -> dict[str, Persistable]:
        return self.__handle_reads(object_ids)

    def write(self, objects: Iterable[Persistable]):
        deferred = self.__deferred_writes is not None
        write_records = self.__deferred_writes if deferred else {}

        for obj in objects:
            write_records.append(obj)

        if not deferred:
            self.__handle_writes(write_records)
