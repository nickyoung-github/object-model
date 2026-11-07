from anyschema import AnySchema
import datetime as dt
import duckdb
from enum import Enum, auto
from fsspec import url_to_fs
from orjson import dumps
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import TypeAdapter, create_model
from typing import Any, Iterable

from .store import MetaData, ObjectRecord, ObjectStore
from .persistable import Persistable
from .reference import ReferenceResolver
from .type_registry import TypeRegistry
from .typed_cache import typed_cache


class LocalStoreFormat(Enum):
    Json = auto()
    Parquet = auto()
    
    
class LocalStore(ObjectStore):
    def __init__(self, url: str, record_format: LocalStoreFormat = LocalStoreFormat.Parquet):
        self.__fs, root = url_to_fs(url)
        self.__root = Path(root)
        self.__record_format = record_format
        self.__conn = duckdb.connect()
        self.__type_registry = TypeRegistry[Persistable]()
        
    @classmethod
    @typed_cache
    def __schema(cls, typ: type[ObjectRecord]) -> pa.Schema:
        schema = AnySchema(spec=typ)
        return schema.to_arrow()

    @classmethod
    @typed_cache
    def record_type(cls, typ: type[Persistable]) -> type[ObjectRecord]:
        record_typ = create_model(f"{typ.__qualname__}Record", __base__=ObjectRecord, data=(typ, ...))
        return record_typ

    @classmethod
    @typed_cache
    def type_adaptor(cls, typ: type[ObjectRecord]) -> TypeAdapter[ObjectRecord]:
        return TypeAdapter(typ)
    
    def read(
        self,
        object_ids: Iterable[Path],
        effective_time: dt.datetime = dt.datetime.max,
        entry_time: dt.datetime = dt.datetime.max
    ) -> dict[Path, Persistable]:
        ret: dict[Path, Persistable] = {}
        by_root: dict[Path, tuple[TypeAdapter, list[Path]]] = {}
        resolver_cache: dict[Path, Persistable] = {}

        for oid in object_ids:
            # oid.parent is the registered_name
            if oid in resolver_cache:
                ret[oid] = resolver_cache[oid]
                continue
                
            typ = self.__type_registry.get_type(str(oid.parent))
            root = self.__type_registry.root(typ)
            directory = self.__root.joinpath(root).joinpath(oid.parent)
            type_adaptor = self.type_adaptor(self.record_type(typ))
            
            by_root.setdefault(directory, (type_adaptor, []))[1].append(oid)
            
        for directory, (type_adaptor, object_ids) in by_root.items():
            from_spec = f"""
            FROM read_parquet('{directory!s}/**/*.parquet', union_by_name = true)
            """ if self.__record_format is LocalStoreFormat.Parquet else f"""
            FROM read_json('{directory!s}/**/*.json', union_by_name = true)
            """
            
            table = self.__conn.sql(
                f"""
                SELECT *
                {from_spec}
                WHERE object_id IN (SELECT unnest(?))
                AND metadata.effective_time <= ?
                AND metadata.entry_time <= ?
                QUALIFY row_number() OVER (PARTITION BY object_id ORDER BY metadata.entry_time DESC) = 1
                """,
                params=[[str(i) for i in object_ids], effective_time, entry_time],
            ).to_arrow_table()
            
            if table is None:
                continue

            for row in table.to_pylist():
                resolver = ReferenceResolver(self)
                context = {"resolver": resolver}
                record = type_adaptor.validate_python(row, context=context)

                if resolver.resolve():
                    record = type_adaptor.validate_python(row, context=context)

                object_id = Path(record.object_id)
                resolver_cache[object_id] = ret[object_id] = record.data
            
        return ret
            
    def write(self, objects: Iterable[Persistable], effective_time: dt.datetime = dt.datetime.max):
        by_path: dict[Path, list[dict[str, Any]]] = {}
        entry_time = dt.datetime.now(dt.UTC)
        filename = Path(f"{entry_time.isoformat()}")
        
        match self.__record_format:
            case LocalStoreFormat.Parquet:
                filename = filename.with_suffix(".parquet")
            case LocalStoreFormat.Json:
                filename = filename.with_suffix(".json")
            case _:
                raise RuntimeError(f"Unsupported format {self.__record_format}")

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
            directory = self.__root.joinpath(self.__type_registry.root(typ)).joinpath(object_id.parent)
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

    def _write(self, records_by_path: dict[Path, list[dict[str, Any]]]):
        for path, records in records_by_path.items():
            if not records:
                continue

            typ = self.__type_registry.get_type(Path(records[0]["object_id"]).parent.name)
            schema = self.__schema(self.record_type(typ))
            self.__fs.makedirs(path.parent, exist_ok=True)

            match self.__record_format:
                case LocalStoreFormat.Parquet:
                    table = pa.Table.from_pylist(records, schema=schema)
                    with pq.ParquetWriter(str(path), schema, filesystem=self.__fs) as writer:
                        writer.write_table(table)
                case LocalStoreFormat.Json:
                    self.__fs.write_bytes(path, dumps(records))
                case _:
                    raise RuntimeError(f"Unsupported format {self.__record_format}")