from pathlib import Path
from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema
from pydantic_core.core_schema import(
    SerializationInfo,
    ValidationInfo,
    with_info_plain_validator_function,
    wrap_serializer_function_ser_schema
)
from typing import Annotated, Any, TypeVar
from typing_extensions import TypeAliasType

from .persistable import Persistable
from .store import ObjectRecord, ObjectStore
from .type_registry import TypeRegistry


class _DISCOVERY_SENTINEL: ...

    
class ReferenceResolver:
    class ReadContext:
        def __init__(self, resolver):
            self.resolver = resolver
            self.pending = set()
            self.previous_pending = None
            
        def __enter__(self):
            self.previous_pending = self.resolver.__pending
            self.resolver.__pending = self.pending
            return self
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            self.resolver.__pending = self.previous_pending
            
            
    def __init__(self, store: ObjectStore):
        self.__store = store
        self.__type_registry = TypeRegistry[Persistable]()
        self.__pending: set[Path] = set()
        self.__loaded: set[Path] = set()
        self.__resolved: dict[Path, Persistable] = {}
        self.__to_write: set[Persistable] = set()
    
    @property
    def to_write(self) -> set[Persistable]:
        return self.__to_write
    
    def get(self, path: Path):
        if path in self.__resolved:
            return self.__resolved[path]
        elif path in self.__loaded:
            raise ValueError(f"Reference cycle on {path!r}")
        else:
            self.__pending.add(path)
            return _DISCOVERY_SENTINEL

    def write(self, obj: Persistable):
        self.__to_write.add(obj)
    
    def resolve(self):
        def type_path_from_record(r: ObjectRecord):
            typ_ = self.__type_registry.get_type(r.registered_name)
            path_ = self.__type_registry.path(typ_, r.object_id)
            return typ_, path_
            
        to_load: list[tuple[type[Persistable], Path, ObjectRecord]] = []
        
        while self.__pending: 
            records = self.__store.read(self.__pending)
            if len(records) < len(self.__pending):
                missing = self.__pending.difference(type_path_from_record(r)[1] for r in records)
                raise RuntimeError(f"Failed to load {missing!r}")
        
            for record in records:
                typ, path = type_path_from_record(record)
                to_load.append((typ, path, record))
                self.__loaded.add(path)
                self.__pending.remove(path)

        for typ, path, record in reversed(to_load):
            obj = typ.type_adapter().validate_python(record.data, context={"resolver": self})
            self.__resolved[path] = obj
            

class _Reference:
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: type[Persistable],
        handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        schema = handler(source_type)
        type_registry = TypeRegistry[Persistable]()

        def resolver(info: SerializationInfo | ValidationInfo) -> ReferenceResolver:
            ret = (info.context or {}).get("resolver")
            if ret is None:
                raise RuntimeError("Need a context to dump or validate Reference")

            return ret
        
        def validate(value: Any, info: ValidationInfo) -> Any:
            if isinstance(value, source_type):
                return value
            elif not isinstance(value, Path):
                raise ValueError(f"Expected an object id or {source_type!s}, got {type(value)!s}")
            return resolver(info).get(value)              

        def serialise(value: Persistable, _serialiser, info: SerializationInfo) -> Any:
            try:
                resolver(info).write(value)
                return type_registry.path(type(value), value.object_id)
            except RuntimeError:
                return value

        return with_info_plain_validator_function(
            validate,
            serialization=wrap_serializer_function_ser_schema(
                serialise,
                schema=schema,
                info_arg=True
            )
        )

__REFERENCE = _Reference()

T = TypeVar("T")
Reference = TypeAliasType("Reference", Annotated[T, __REFERENCE], type_params=(T,))
