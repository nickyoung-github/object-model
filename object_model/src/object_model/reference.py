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


class _DISCOVERY_SENTINEL: ...

    
class ReferenceResolver:
    def __init__(self, store: ObjectStore):
        self.discovering = True
        self.__store = store
        self.__pending: set[str] = set()
        self.__loaded: dict[str, Persistable] = {}
        self.__resolved: dict[str, Persistable] = {}
    
    def get(self, object_id: str):
        if object_id in self.__resolved:
            return self.__resolved[object_id]
        elif self.discovering:
            self.__pending.add(object_id)
            return _DISCOVERY_SENTINEL
        elif object_id in self.__loaded:
            raise ValueError("Reference cycle")
        raise LookupError(f"Unresolvable id: {object_id}")
    
    def write(self, obj: Persistable):
        self.__store.write([obj])
    
    def resolve(self) -> bool:
        if not self.__pending:
            return False
        
        while self.__pending:
            results = self.__store.read(self.__pending)
            for object_id, obj in results.items():
                self.__loaded[object_id] = obj
                self.__pending.remove(object_id)
                
            if self.__pending:
                raise LookupError(f"Failed to load objects with ids: {self.__pending!r}")
            
            for object_id, obj in results.items():
                obj.type_adapter().validate_python(obj, context={"resolver": self})
                if not self.__pending:
                    self.__resolved[object_id] = self.__loaded.pop(object_id)
                    
        return True

class _Reference:
    @classmethod
    def __get_pydantic_core_schema__(cls,
                                     source_type: type[Persistable],
                                     handler: GetCoreSchemaHandler) -> CoreSchema:
        schema = handler(source_type)

        def resolver(info: SerializationInfo | ValidationInfo) -> ReferenceResolver:
            ret = (info.context or {}).get("resolver")
            if ret is None:
                raise RuntimeError("Need a context to dump or validate Reference")

            return ret
        
        def validate(value: Any, info: ValidationInfo) -> Any:
            if isinstance(value, source_type):
                return value
            elif not isinstance(value, str):
                raise ValueError(f"Expected an object id or {source_type!s}, got {type(value)!s}")
            return resolver(info).get(value)              

        def serialise(value: Persistable, _serialiser, info: SerializationInfo) -> Any:
            if "by_reference" in (info.context or {}):
                resolver(info).write(value)
                return value.object_id
            else:
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
