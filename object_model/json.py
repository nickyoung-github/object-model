from dataclasses import MISSING, is_dataclass, fields
from importlib import import_module
from orjson import loads as __loads, dumps as __dumps
from pydantic import BaseModel, ConfigDict, TypeAdapter
from pydantic.alias_generators import to_camel
from sys import modules
from typing import Any


TYPE_KEY = "t_"


class __TypeRegistry:
    __instance = None

    def __new__(cls, *args, **kwargs):
        # Make this a singleton
        if cls.__instance is None:
            cls.__instance = super().__new__(cls, *args, **kwargs)
            cls.__instance.__types: dict[str, type] = {}
            cls.__instance.__type_adaptors: dict[type, TypeAdapter] = {}

        return cls.__instance

    def __call__(self, type_path: str) -> type:
        typ = self.__types.get(type_path)
        if typ is None:
            try:
                module_name, _, type_name = type_path.rpartition(".")

                if module_name not in modules:
                    import_module(module_name)

                module = modules[module_name]
                typ = self.__types[type_path] = getattr(module, type_name)
            except (ModuleNotFoundError, KeyError) as e:
                raise TypeError(f"Attempt to dynamically load type {type_path} failed: {e}")

        return typ

    def type_adaptor(self, typ: type) -> TypeAdapter | None:
        if not is_dataclass(typ):
            return None

        type_adaptor = self.__type_adaptors.get(typ)
        if not type_adaptor:
            type_adaptor = self.__type_adaptors[typ] = TypeAdapter(typ)
            type_adaptor._config = ConfigDict(frozen=True, populate_by_name=True, alias_generator=to_camel)
            with type_adaptor._with_frame_depth(1):
                type_adaptor._init_core_attrs(rebuild_mocks=False)

        return type_adaptor


def dump(data: Any) -> dict[str, Any]:
    if isinstance(data, BaseModel):
        return data.model_dump(include={*data.model_fields_set, TYPE_KEY}, by_alias=True)
    elif is_dataclass(data):
        flds = [f.name for f in fields(data) if f.default is MISSING or f.default != getattr(data, f.name)]
        return __TypeRegistry().type_adaptor(type(data)).dump_python(data, include=flds)
    else:
        raise RuntimeError("Unsupported type")


def dumps(data: Any) -> str:
    if isinstance(data, BaseModel):
        return data.model_dump_json(include={*data.model_fields_set, TYPE_KEY}, by_alias=True)
    elif is_dataclass(data):
        flds = [f.name for f in fields(data) if f.default is MISSING or f.default != getattr(data, f.name)]
        return __TypeRegistry().type_adaptor(type(data)).dump_json(data, by_alias=True, include=flds)
    else:
        return __dumps(data).decode("UTF-8")


def load(data: dict[str, Any]) -> Any:
    type_registry = __TypeRegistry()

    type_path = data.get(TYPE_KEY)
    if not type_path:
        raise RuntimeError("No type in data")

    typ = type_registry(type_path)
    if not typ:
        raise RuntimeError(f"Cannot resolve type for {type_path}")

    return typ.model_validate(data) if issubclass(typ, BaseModel) else \
        type_registry.type_adaptor(typ).validate_python(data)


def loads(data: bytes | str, type_path: str | None = None) -> Any:
    type_registry = __TypeRegistry()

    if type_path is None:
        # SUPER inefficient
        type_path = __loads(data.encode("UTF-8") if isinstance(data, str) else data).get(TYPE_KEY)
        if not type_path:
            raise RuntimeError("type_path not specified or found in the json")

    typ = type_registry(type_path)
    if not typ:
        raise RuntimeError(f"Cannot resolve type for {type_path}")

    return typ.model_validate_json(data) if issubclass(typ, BaseModel) else\
        type_registry.type_adaptor(typ).validate_json(data)


def schema(typ: type) -> dict[str, Any]:
    if issubclass(typ, BaseModel):
        return typ.model_json_schema(by_alias=True)

    return __TypeRegistry().type_adaptor(typ).json_schema()
