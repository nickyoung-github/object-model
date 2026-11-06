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
from .store import ObjectStore
from .type_registry import TypeRegistry


class _DISCOVERY_SENTINEL: ...

    
class ReferenceResolver:
    def __init__(self, store: ObjectStore):
        self.__store = store
        self.__discovering = False
        self.__type_registry = TypeRegistry[Persistable]()
        self.__pending: set[Path] = set()
        self.__resolved: dict[Path, Persistable] = {}
        self.__to_write: set[Persistable] = set()
    
    @property
    def to_write(self) -> set[Persistable]:
        return self.__to_write
    
    def get(self, object_id: Path):
        if object_id in self.__resolved:
            return self.__resolved[object_id]
        elif self.__discovering:
            self.__pending.add(object_id)
            return _DISCOVERY_SENTINEL
        raise RuntimeError("Failed to resolve {object_id")

    def write(self, obj: Persistable):
        self.__to_write.add(obj)
    
    def resolve(self) -> bool:
        self.__discovering = False
        
        if not self.__pending:
            return False
    
        loaded = self.__store.read(self.__pending)
        if len(loaded) < len(self.__pending):
            missing = self.__pending.difference(loaded.keys())
            raise RuntimeError(f"Failed to load {missing!r}")
        
        self.__resolved.update(loaded)
            
        return True
            

class _Reference:
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: type[Persistable],
        handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        schema = handler(source_type)

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
                return value.object_id
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
