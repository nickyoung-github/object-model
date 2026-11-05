from dataclasses import dataclass
import datetime as dt
from pathlib import Path
from typing import Iterable
from uuid import uuid4

from .persistable import Persistable
from .reference import ReferenceResolver
from .store import MetaData, ObjectRecord, ObjectStore
from .type_registry import TypeRegistry


@dataclass(frozen=True, kw_only=True)
class DeferredReadTarget:
    attribute: str
    target: Persistable

    def set_value(self, value: Persistable):
        setattr(self.target, self.attribute, value)
        

class Transaction:
    def __init__(
        self,
        store: ObjectStore,
        effective_time: dt.datetime = dt.datetime.max,
        entry_time: dt.datetime = dt.datetime.max
     ):
        self.__store = store
        self.__effective_time = effective_time
        self.__entry_time = entry_time
        self.__type_registry = TypeRegistry[Persistable]()
        self.__uuid = uuid4()
        self.__deferred_writes: list[Persistable] | None = None

    def __enter__(self):
        self.__deferred_writes = []

    def __exit__(self):
        if self.__deferred_writes is not None:
            self.__handle_writes(self.__deferred_writes)
            self.__deferred_writes = None

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

        def add_record(o: Persistable):
            record = ObjectRecord(
                object_id=obj.object_id,
                registered_name=self.__type_registry.registered_name(type(obj)),
                path=self.__type_registry.root(type(obj)),
                metadata=metadata,
                data=o.type_adapter().dump_python(o, context={"resolver": resolver})
            )
            records.append(record)

        for obj in write_records:
            resolver = ReferenceResolver(self.__store)
            add_record(obj)
            for nested in resolver.to_write:
                add_record(nested)
                
        self.__store.write(records)

    def read(self, typ: type[Persistable], object_ids: Iterable[str]) -> tuple[Persistable, ...]:
        paths = set(
            Path(self.__type_registry.path(typ, i, include_root=True)).joinpath(i)
            for i in object_ids
        )
        records = self.__store.read(paths)
        ret = ()

        for r in records:
            typ = self.__type_registry.get_type(r.registered_name)
            resolver = ReferenceResolver(self.__store)
            obj = typ.type_adapter().validate_python(r.data, context={"resolver": resolver})
            path = self.__type_registry.path(typ, r.object_id)
            paths.remove(path)
            ret += (obj,)

        if paths:
            raise RuntimeError(f"Failed to load: {paths}")

        return ret

    def write(self, objects: Iterable[Persistable]):
        write_records = self.__deferred_writes if self.__deferred_writes is not None else []

        for obj in objects:
            write_records.append(obj)

        if self.__deferred_writes is None:
            self.__handle_writes(write_records)
