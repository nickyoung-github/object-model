from dataclasses import MISSING, is_dataclass, fields
from orjson import loads as __loads, dumps as __dumps
from pydantic import BaseModel, TypeAdapter
from typing import Any


from ._type_registry import TYPE_KEY, get_type


def dump(data: Any) -> dict[str, Any]:
    if isinstance(data, BaseModel):
        return data.model_dump(include={*data.model_fields_set, TYPE_KEY}, by_alias=True)
    elif is_dataclass(data):
        flds = [f.name for f in fields(data) if f.default is MISSING or f.default != getattr(data, f.name)]
        return TypeAdapter(type(data)).dump_python(data, include=flds)
    else:
        raise RuntimeError("Unsupported type")


def dumps(data: Any) -> str:
    if isinstance(data, BaseModel):
        return data.model_dump_json(include={*data.model_fields_set, TYPE_KEY}, by_alias=True)
    elif is_dataclass(data):
        flds = [f.name for f in fields(data) if f.default is MISSING or f.default != getattr(data, f.name)]
        return TypeAdapter(type(data)).dump_json(data, by_alias=True, include=flds)
    else:
        return __dumps(data).decode("UTF-8")


def load(data: dict[str, Any]) -> Any:
    type_name = data.get(TYPE_KEY)
    if type_name is None:
        raise RuntimeError("No type in data")

    typ = get_type(type_name)

    return typ.model_validate(data) if issubclass(typ, BaseModel) else TypeAdapter(typ).validate_python(data)


def loads(data: bytes | str, typ: type | str | None = None) -> Any:
    if typ is None or isinstance(typ, str):
        if typ is None:
            # SUPER inefficient
            type_name = __loads(data.encode("UTF-8") if isinstance(data, str) else data).get(TYPE_KEY)
            if type_name is None:
                raise RuntimeError("type_path not specified or found in the json")
        else:
            type_name = typ

        typ = get_type(type_name)

    return typ.model_validate_json(data) if issubclass(typ, BaseModel) else TypeAdapter(typ).validate_json(data)


def schema(typ: type) -> dict[str, Any]:
    if issubclass(typ, BaseModel):
        return typ.model_json_schema(by_alias=True)

    return TypeAdapter(typ).json_schema()
