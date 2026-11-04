from pydantic import (
    BeforeValidator,
    Discriminator,
    GetJsonSchemaHandler,
    Tag,
    WrapSerializer
)
from pydantic_core import CoreSchema
from typing import _SpecialForm, Annotated, Any, Union

from .persistable import Persistable
from .type_registry import TypeRegistry


class _TagJsonSchema:
    def __init__(self, tag_field: str, tag: str):
        self.__tag_field = tag_field
        self.__tag = tag
        
    def __get_pydantic_json_schema__(
        self, schema: CoreSchema, handler: GetJsonSchemaHandler
    ):
        json_schema = handler(schema)
        resolved = dict(handler.resolve_ref_schema(json_schema))
        properties = resolved["properties"] = dict(resolved.get("properties", {}))
        required = resolved["required"] = list(resolved.setdefault("required", []))
        
        properties[self.__tag_field] = {"const": self.__tag}
        if self.__tag_field not in required:
            required.append(self.__tag_field)
            
        return resolved
    
        
@_SpecialForm
def DiscriminatedUnion(_cls, params: tuple[type[Persistable], ...]):
    if (
        not isinstance(params, tuple | list)
        or len(params) == 1
        or not all (issubclass(p, Persistable) for p in params)
    ):
        raise TypeError("Can only use with multiple Persistable types")
    
    tag_field = "_regsitered_name"
    type_registry = TypeRegistry[Persistable]()
    
    def strip_tag(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: v for k, v in value.items() if k != tag_field}
        return value
    
    def dicriminate(value: Any) -> str | None:
        if isinstance(value, dict):
            return value[tag_field]
        elif isinstance(value, Persistable):
            return type_registry.registered_name(type(value))
        
    def serialise(value: Any, handler: Any) -> Any:
        data = handler(Any)
        if isinstance(data, dict):
            return {tag_field: type_registry.registered_name(type(value)), **data}
        return data
    
    union_members = tuple(
        Annotated[
            p,
            BeforeValidator(strip_tag),
            Tag(type_registry.registered_name(p)),
            _TagJsonSchema(tag_field, type_registry.registered_name(p))
        ]
        for p in params
    )
    
    return Annotated[
        Union[union_members],
        WrapSerializer(serialise),
        Discriminator(dicriminate)
    ]
            
        