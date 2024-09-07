from dataclasses import MISSING, asdict, fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from importlib import import_module
from orjson import loads as __loads, dumps as __dumps
from pydantic import BaseModel, ConfigDict, TypeAdapter
from pydantic.alias_generators import to_camel, to_snake
from sys import modules
from typing import Any, Callable


TYPE_KEY = "t_"


class __TypeRegistry:
    __instance = None

    def __new__(cls, *args, **kwargs):
        # Make this a singleton
        if cls.__instance is None:
            cls.__instance = super().__new__(cls, *args, **kwargs)

        return cls.__instance

    def __init__(self):
        self.__types: dict[str, tuple[type, TypeAdapter]] = {}

    def __call__(self, type_path: str) -> tuple[type, TypeAdapter]:
        typ, type_adaptor = self.__types.get(type_path, (None, None))
        if typ is None:
            try:
                module_name, _, type_name = type_path.rpartition(".")

                if module_name not in modules:
                    import_module(module_name)

                module = modules[module_name]
                typ = getattr(module, type_name)
                type_adaptor = TypeAdapter(typ) if is_dataclass(typ) else None

                self.__types[type_path] = (typ, type_adaptor)
            except (ModuleNotFoundError, KeyError) as e:
                raise TypeError(f"Attempt to dynamically load type {type_path} failed: {e}")

        return typ, type_adaptor


def __noop_alias_generator(data: str) -> str:
    return data


def dump(data: Any,
         alias_generator: Callable[[str], str] = __noop_alias_generator,
         convert_builtins: bool = False) -> Any:
    # Scan for members of untyped collections which either don't serialise or serialise amibiguously to JSON,
    # e.g. date, datetime, decimal. Assume that if such data is a direct attribute of an object, the object itself
    # will handle the conversion

    if isinstance(data, BaseModel):
        return dump(data.model_dump(include={*data.model_fields_set, TYPE_KEY}, by_alias=True),
                    alias_generator, False)
    elif is_dataclass(data):
        # Remove fields whose value is their default
        raw = asdict(data)
        for fld in fields(data):
            if fld.default is not MISSING and raw[fld.name] == fld.default:
                raw.pop(fld.name)

        return dump(raw, alias_generator, True)
    elif isinstance(data, dict):
        is_object = TYPE_KEY in data
        return {alias_generator(k) if is_object or isinstance(k, str) else dump(k, alias_generator, convert_builtins):
                dump(v, alias_generator, convert_builtins or not is_object) for k, v in data.items()}
    elif isinstance(data, (list, tuple)):
        return type(data)(dump(i, alias_generator, convert_builtins) for i in data)
    elif convert_builtins:
        if isinstance(data, datetime):
            return {TYPE_KEY: "_dt", "v": data.isoformat()}
        elif isinstance(data, date):
            return {TYPE_KEY: "_d", "v": data.isoformat()}
        elif isinstance(data, Decimal):
            return {TYPE_KEY: "_dc", "v": str(data)}
        else:
            return data
    else:
        return data


def dumps(data: Any, alias_generator=to_camel) -> bytes:
    return __dumps(dump(data, alias_generator))


def load(data: Any, alias_generator: Callable[[str], str] = __noop_alias_generator) -> Any:
    if isinstance(data, dict):
        type_path = data.pop(TYPE_KEY, None)
        if type_path:
            if type_path == "_d":
                return date.fromisoformat(data["v"])
            elif type_path == "_dt":
                return datetime.fromisoformat(data["v"])
            elif type_path == "_dc":
                return Decimal(data["v"])
            else:
                data = {alias_generator(k): load(v, alias_generator) for k, v in data.items()}
                typ, type_adaptor = __TypeRegistry()(type_path)
                if type_adaptor:
                    return type_adaptor.validate_python(data)
                elif typ:
                    return typ(**data)
                else:
                    return data
        else:
            return {alias_generator(k) if isinstance(k, str) else load(k, alias_generator): load(v, alias_generator)
                    for k, v in data.items()}
    elif isinstance(data, (list, tuple)):
        return type(data)(load(i, alias_generator) for i in data)
    else:
        return data


def loads(data: bytes, alias_generator=to_snake) -> Any:
    return load(__loads(data), alias_generator)


def schema(typ: type) -> dict[str, Any]:
    if issubclass(typ, BaseModel):
        return typ.model_json_schema(by_alias=True)
    elif is_dataclass(typ):
        adaptor = TypeAdapter(typ)
        adaptor._config = ConfigDict(frozen=True, populate_by_name=True, alias_generator=to_camel)
        with adaptor._with_frame_depth(1):
            adaptor._init_core_attrs(rebuild_mocks=False)

        return adaptor.json_schema()
    else:
        raise RuntimeError("Can only handle pydantic models or dataclasses")
