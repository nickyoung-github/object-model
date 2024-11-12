from frozendict import frozendict
from pydantic import Field, GetCoreSchemaHandler
from pydantic_core import core_schema
from typing import _SpecialForm, Annotated, Any, Iterable, TypeVar, Union, get_args

from ._json import TYPE_KEY

__classes_with_type = {}


@_SpecialForm
def DiscriminatedUnion(_cls, types: Iterable[type]):
    args = ()
    for typ in types:
        if TYPE_KEY not in typ.__annotations__:
            raise TypeError(f"{typ} is missing {TYPE_KEY} Literal")
        args += (typ,)

    return Annotated[Union[args], Field(discriminator=TYPE_KEY)]


@_SpecialForm
def Subclass(_cls, param_typ: type):
    subclasses = set()
    stack = [param_typ]
    while stack:
        subclass = stack.pop()
        subclasses.add(subclass)
        stack.extend(subclass.__subclasses__())

    return DiscriminatedUnion[tuple(subclasses)]


class __PydanticFrozenDictAnnotation:
    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> core_schema.CoreSchema:
        def validate_from_dict(d: dict | frozendict) -> frozendict:
            return frozendict(d)

        frozendict_schema = core_schema.chain_schema(
            [
                handler.generate_schema(dict[*get_args(source_type)]),
                core_schema.no_info_plain_validator_function(validate_from_dict),
                core_schema.is_instance_schema(frozendict)
            ]
        )
        return core_schema.json_or_python_schema(
            json_schema=frozendict_schema,
            python_schema=frozendict_schema,
            serialization=core_schema.plain_serializer_function_ser_schema(dict)
        )


_K = TypeVar('_K')
_V = TypeVar('_V')
FrozenDict = Annotated[frozendict[_K, _V], __PydanticFrozenDictAnnotation]
