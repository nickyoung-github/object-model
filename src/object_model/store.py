from abc import ABC, abstractmethod
import datetime as dt
from functools import cache
from pathlib import Path
from pydantic import BaseModel, TypeAdapter, create_model
from typing import Any, Iterable

from .persistable import Persistable
from .type_registry import TypeRegistry
        
# These are pydantic classes because it's easy to use create_model to create the per-type
# derived ObjectRecord classes we need

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


class ReferenceResolver:
    def __init__(self, store: ObjectStore):
        self.__store = store
        self.__discovering = True
        self.__pending_reads: set[Path] = set()
        self.__pending_writes: dict[Path, Persistable] = {}
        self.__resolved: dict[Path, Persistable] = {}

    def take_pending_writes(self) -> tuple[Persistable, ...]:
        pending_writes = tuple(self.__pending_writes.values())
        self.__pending_writes = {}
        return pending_writes

    def get(self, object_id: Path):
        if object_id in self.__resolved:
            return self.__resolved[object_id]
        elif self.__discovering:
            self.__pending_reads.add(object_id)
            return str(object_id)
        raise RuntimeError("Failed to resolve {object_id")

    def write(self, obj: Persistable):
        self.__pending_writes[obj.object_id] = obj

    def resolve(self) -> bool:
        self.__discovering = False

        if not self.__pending_reads:
            return False

        loaded = self.__store.read(self.__pending_reads)
        if len(loaded) < len(self.__pending_reads):
            missing = self.__pending_reads.difference(loaded.keys())
            raise RuntimeError(f"Failed to load {missing!r}")

        self.__resolved.update(loaded)
        self.__discovering = False
        self.__pending_reads = set()

        return True


class ObjectStore(ABC):
    @abstractmethod
    def _read(
        self,
        by_directory: dict[Path, list[Path]],
        effective_time: dt.datetime,
        entry_time: dt.datetime
    ) -> dict[Path, list[dict[str, Any]]]:
        ...

    @abstractmethod
    def _write(
        self,
        records_by_path: dict[Path, list[dict[str, Any]]]
    ):
        ...

    @classmethod
    @cache
    def record_type(cls, typ: type[Persistable]) -> type[ObjectRecord]:
        record_typ = create_model(f"{typ.__qualname__}Record", __base__=ObjectRecord, data=(typ, ...))
        return record_typ

    @classmethod
    @cache
    def type_adaptor(cls, typ: type[ObjectRecord]) -> TypeAdapter[ObjectRecord]:
        return TypeAdapter(typ)

    @classmethod
    def type_registry(cls) -> TypeRegistry[Persistable]:
        return TypeRegistry[Persistable]()

    def read(
        self,
        object_ids: Iterable[Path],
        effective_time: dt.datetime = dt.datetime.max,
        entry_time: dt.datetime = dt.datetime.max
    ) -> dict[Path, Persistable]:
        ret: dict[Path, Persistable] = {}
        by_directory: dict[Path, list[Path]] = {}
        resolver_cache: dict[Path, Persistable] = {}
        type_registry = self.type_registry()

        for oid in object_ids:
            # oid.parent is the registered_name
            if oid in resolver_cache:
                ret[oid] = resolver_cache[oid]
                continue

            typ = type_registry.get_type(str(oid.parent))
            directory = type_registry.root(typ).joinpath(oid.parent)
            by_directory.setdefault(directory, []).append(oid)

        all_results = self._read(by_directory, effective_time, entry_time)
        for directory, results in all_results.items():
            if not results:
                continue

            typ = type_registry.get_type(Path(results[0]["object_id"]).parent.name)
            type_adaptor = self.type_adaptor(self.record_type(typ))

            for result in results:
                resolver = ReferenceResolver(self)
                context = {"resolver": resolver}
                record = type_adaptor.validate_python(result, context=context)

                if resolver.resolve():
                    record = type_adaptor.validate_python(result, context=context)

                object_id = Path(record.object_id)
                resolver_cache[object_id] = ret[object_id] = record.data

        return ret

    def write(
        self,
        objects: Iterable[Persistable],
        effective_time: dt.datetime = dt.datetime.max
    ):
        type_registry = self.type_registry()
        by_path: dict[Path, list[dict[str, Any]]] = {}
        entry_time = dt.datetime.now(dt.UTC)
        filename = Path(f"{entry_time.isoformat()}")
        metadata = MetaData(
            effective_time=effective_time if effective_time != dt.datetime.max else entry_time,
            entry_time=entry_time,
            host="nick-host",
            user="nick",
        )

        all_objects = list(objects)
        while all_objects:
            obj = all_objects.pop()
            object_id = obj.object_id
            typ = type(obj)
            directory = type_registry.root(typ).joinpath(object_id.parent)
            path = directory.joinpath(filename)
            record_type = self.record_type(typ)
            type_adapter = self.type_adaptor(record_type)
            record = record_type(
                object_id=str(object_id),
                metadata=metadata,
                data=obj
            )
            resolver = ReferenceResolver(self)
            data = type_adapter.dump_python(record, context={"resolver": resolver})

            all_objects.extend(resolver.take_pending_writes())
            by_path.setdefault(path, []).append(data)

        self._write(by_path)
