from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, TypeVar, Union, _SpecialForm

from pydantic import BeforeValidator, GetJsonSchemaHandler, Tag, WrapSerializer
from pydantic import Discriminator as _Discriminator
from pydantic_core import CoreSchema

from .persistable import Persistable
from .type_registry import TypeRegistry

T = TypeVar("T", bound=type, default=type)

@dataclass
class Discriminator[T]:
    field: str
    discriminator: str | Callable[[T], str]

    def __call__(self, value: T) -> str:
        return getattr(value, self.discriminator) if isinstance(self.discriminator, str) else\
            self.discriminator(value)


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
    
        
@_SpecialForm # pyright: ignore[reportCallIssue]
def DiscriminatedUnion(_, params: tuple[type | Discriminator, ...]):
    if not isinstance(params, tuple | list) or len(params) < 2:
        raise TypeError("Can only use with multiple Persistable types")

    if isinstance(params[-1], Discriminator):
        types = params[:-1]
        if len(types) < 2:
            raise TypeError("Can only use with multiple Persistable types")
        discriminator = params[-1]
    else:
        types = params
        discriminator = Discriminator(
            field="_registered_name",
            discriminator=lambda t: TypeRegistry[Persistable]().registered_name(t)
        )

    def strip_tag(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: v for k, v in value.items() if k != discriminator.field}
        return value
    
    def dicriminate(value: Any) -> str | None:
        if isinstance(value, dict):
            return value[discriminator.field]
        elif isinstance(value, types):
            return discriminator(type(value))
        return None
        
    def serialise(value: Any, handler: Any) -> Any:
        data = handler(Any)
        if isinstance(data, dict):
            return {discriminator.field: discriminator(type(value)), **data}
        return data
    
    union_members = tuple(
        Annotated[
            t,
            BeforeValidator(strip_tag),
            Tag(discriminator(t)),
            _TagJsonSchema(discriminator.field, discriminator(t))
        ]
        for t in types
    )
    
    return Annotated[
        Union[union_members],  # noqa: UP007
        WrapSerializer(serialise),
        _Discriminator(dicriminate)
    ]


@_SpecialForm # pyright: ignore[reportCallIssue]
def Discriminated(_, params: tuple[type[Persistable | Path], ...]):
    if len(params) != 1:
        raise TypeError("Must supply a single root or Persistable type")

    type_registry = TypeRegistry()
    if isinstance(params[0], Path):
        types = type_registry.types_for_root(params[0])
    elif issubclass(params[0], Persistable):
        # ToDo: should we force the TypeRegistry to resolve all entry points first ?
        types = params[0].__subclasses__()
    else:
        raise TypeError("Must supply a single root or Persistable type")

    return DiscriminatedUnion[types]
