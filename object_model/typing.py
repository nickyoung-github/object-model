from frozendict import frozendict
from pydantic import Field, GetCoreSchemaHandler
from pydantic_core import core_schema as pyd_core_schema
from typing import _SpecialForm, Annotated, Any, TypeVar, Union, get_args

from .json import TYPE_KEY


@_SpecialForm
def Subclass(_cls, param_typ: type):
    if TYPE_KEY not in param_typ.__annotations__:
        raise TypeError(f"Usage: Subclass[Type], where Type provides a {TYPE_KEY} Literal")

    subclasses = set()
    stack = [param_typ]
    while stack:
        subclass = stack.pop()
        subclasses.add(subclass)
        stack.extend(subclass.__subclasses__())

    return Annotated[Union[tuple(subclasses)], Field(..., discriminator=TYPE_KEY)]


class __PydanticFrozenDictAnnotation:
    @classmethod
    def __get_pydantic_core_schema__(
            cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> pyd_core_schema.CoreSchema:
        def validate_from_dict(d: dict | frozendict) -> frozendict:
            return frozendict(d)

        frozendict_schema = pyd_core_schema.chain_schema(
            [
                handler.generate_schema(dict[*get_args(source_type)]),
                pyd_core_schema.no_info_plain_validator_function(validate_from_dict),
                pyd_core_schema.is_instance_schema(frozendict),
            ]
        )
        return pyd_core_schema.json_or_python_schema(
            json_schema=frozendict_schema,
            python_schema=frozendict_schema,
            serialization=pyd_core_schema.plain_serializer_function_ser_schema(dict),
        )


_K = TypeVar('_K')
_V = TypeVar('_V')
FrozenDict = Annotated[frozendict[_K, _V], __PydanticFrozenDictAnnotation]
