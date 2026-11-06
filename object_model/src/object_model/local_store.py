import datetime as dt
import duckdb
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import TypeAdapter, create_model
from pydantic_to_pyarrow import get_pyarrow_schema
from typing import Any, Iterable
from uuid import uuid4

from .store import MetaData, ObjectRecord, ObjectStore
from .persistable import Persistable
from .reference import ReferenceResolver
from .type_registry import TypeRegistry
from .typed_cache import typed_cache


class LocalStore(ObjectStore):
    def __init__(self, root: Path):
        self.__root = root
        self.__conn = duckdb.connect()
        self.__type_registry = TypeRegistry[Persistable]()
        
    @classmethod
    @typed_cache
    def __schema(cls, typ: type[ObjectRecord]) -> pa.Schema:
        return get_pyarrow_schema(typ)
    
    @classmethod
    @typed_cache
    def record_type(cls, typ: type[Persistable]) -> type[ObjectRecord]:
        return create_model(typ.__qualname__, __base__=ObjectRecord, data=(typ, ...))

    @classmethod
    @typed_cache
    def type_adaptor(cls, typ: type[ObjectRecord]) -> TypeAdapter[ObjectRecord]:
        return TypeAdapter(typ)
    
    def read(self, paths: Iterable[Path]) -> dict[Path, Persistable]:
        ret: dict[Path, Persistable] = {}
        by_root: dict[Path, list[Path]] = {}
        
        for path in paths:
            root = path.parent
            object_id = Path(path.parent.name).joinpath(path.name)
            by_root.setdefault(root, []).append(object_id)
            
        for root, object_ids in by_root.items():
            table = self.__conn.execute(
                f"""
                SELECT *
                FROM read_parquet('{root!s}/**/*.parquet', union_by_name = true)
                WHERE object_id IN (SELECT unnest(?))
                """,
                [object_ids],
            ).to_arrow_table()
            
            if table is None:
                continue
                
            typ = self.__type_registry.get_type(root.name)
            record_type = self.record_type(typ)
            type_adaptor = self.type_adaptor(record_type)
            resolver = ReferenceResolver(self)
            
            for row in table.to_pylist():
                record = type_adaptor.validate_python(row, context={"resoler": resolver})
                if resolver.resolve():
                    record = type_adaptor.validate_python(row, context={"resoler": resolver})
                    
                ret[root.parent.joinpath(record.object_id)] = record.data
            
        return ret
            
    def write(self, objects: Iterable[Persistable], effective_time: dt.datetime = dt.datetime.max):
        by_path: dict[Path, tuple[pa.Schema, list[dict[str, Any]]]] = {}
        entry_time = dt.datetime.now(dt.UTC)

        metadata = MetaData(
            effective_time=effective_time if effective_time != dt.datetime.max else entry_time,
            entry_time=entry_time,
            host="nick-host",
            user="nick",
            transaction_id=uuid4()
        )
        
        all_objects = list(objects)
        while all_objects:
            obj = all_objects.pop()
            typ = type(obj)
            object_id = self.__type_registry.path(typ, obj.object_id)
            record_type = self.record_type(typ)
            schema = self.__schema(record_type)
            type_adapter = self.type_adaptor(record_type)
            resolver = ReferenceResolver(self)
            record = record_type(
                object_id=object_id,
                metadata=metadata,
                data=obj
            )
            path = self.__root.joinpath(self.__type_registry.path(typ, include_root=True))
            path = path.joinpath(f"{entry_time.isoformat()}.parquet")
            data = type_adapter.dump_python(record, context={"resolver": resolver})

            all_objects.extend(resolver.to_write)
            by_path.setdefault(path, (schema, []))[1].append(data)
            
        for path, (schema, records) in by_path.items():
            with pq.ParquetWriter(str(path), schema) as writer:
                writer.write_table(pa.Table.from_pylist(records, schema=schema))
            