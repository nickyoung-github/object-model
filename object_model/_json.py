from dataclasses import MISSING, is_dataclass, fields
from orjson import loads as __loads, dumps as __dumps
from pydantic import BaseModel, ConfigDict, TypeAdapter
from pydantic.alias_generators import to_camel
from typing import Any


from ._type_registry import TYPE_KEY, get_type


__type_adaptors: dict[str, TypeAdapter] = {}


def get_type_adaptor(typ: type) -> TypeAdapter:
    # This madness is because TypeAdapter ignores the class's config when generating json but respects it
    # when generating jsonn schema

    type_adaptor = __type_adaptors.get(typ)
    if type_adaptor is None:
        type_adaptor = __type_adaptors[typ] = TypeAdapter(typ)
        type_adaptor._config = ConfigDict(frozen=True, populate_by_name=True, alias_generator=to_camel)
        with type_adaptor._with_frame_depth(1):
            type_adaptor._init_core_attrs(rebuild_mocks=False)

    return type_adaptor


def dump(data: Any) -> dict[str, Any]:
    if isinstance(data, BaseModel):
        return data.model_dump(include={*data.model_fields_set, TYPE_KEY}, by_alias=True)
    elif is_dataclass(data):
        flds = [f.name for f in fields(data) if f.default is MISSING or f.default != getattr(data, f.name)]
        return get_type_adaptor(type(data)).dump_python(data, include=flds)
    else:
        raise RuntimeError("Unsupported type")


def dumps(data: Any) -> bytes:
    if isinstance(data, BaseModel):
        return data.model_dump_json(include={*data.model_fields_set, TYPE_KEY}, by_alias=True).encode("utf-8")
    elif is_dataclass(data):
        flds = [f.name for f in fields(data) if f.default is MISSING or f.default != getattr(data, f.name)]
        return get_type_adaptor(type(data)).dump_json(data, by_alias=True, include=flds)
    else:
        return __dumps(data)


def load(data: dict[str, Any]) -> Any:
    type_name = data.get(TYPE_KEY)
    if type_name is None:
        raise RuntimeError("No type in data")

    typ = get_type(type_name)

    return typ.model_validate(data) if issubclass(typ, BaseModel) else get_type_adaptor(typ).validate_python(data)


def loads(data: bytes | str, typ: type | str | None = None) -> Any:
    if typ is None:
        ret = __loads(data)
        # SUPER ineffecient ...
        if TYPE_KEY in ret:
            typ = ret[TYPE_KEY]
        else:
            return ret

    if isinstance(typ, str):
        typ = get_type(typ)

    return typ.model_validate_json(data) if issubclass(typ, BaseModel) else get_type_adaptor(typ).validate_json(data)


def schema(typ: type) -> dict[str, Any]:
    if issubclass(typ, BaseModel):
        return typ.model_json_schema(by_alias=True)

    return get_type_adaptor(typ).json_schema()
