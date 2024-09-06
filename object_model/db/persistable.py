from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
import datetime as dt
from importlib import import_module
from orjson import dumps, loads
from pydantic import TypeAdapter, ValidationError
from typing import Any

from ..typing import _one_of


def type_name(typ: type) -> str:
    return f"{typ.__module__}.{typ.__name__}"


class UseDerived:
    ...


@dataclass
class DBRecord:
    object_type: str
    object_id: bytes
    contents: bytes
    effective_version: int
    entry_version: int
    effective_time: dt.datetime
    entry_time: dt.datetime


class Id:
    def __init__(self, *args, typ=None):
        self.__fields = args
        self.__type = typ

    def __get__(self, obj, objtype=None) -> tuple[type, tuple[Any, ...]]:
        if obj is None:
            return self.__type, self.__fields

        return self.__type, tuple(getattr(obj, f) for f in self.__fields)

    def __set_name__(self, owner, name):
        if not self.__type:
            self.__type = owner


class PersistableMixin:
    def __init__(self):
        # This mixin is used by frozen dataclasses, which stop you setting private members (annoyingly)

        object.__setattr__(self, "_PersistableMixin__effective_time", dt.datetime.max)
        object.__setattr__(self, "_PersistableMixin__entry_time", dt.datetime.max)
        object.__setattr__(self, "_PersistableMixin__effective_version", 0)
        object.__setattr__(self, "_PersistableMixin__entry_version", 0)

    @property
    def effective_time(self) -> dt.datetime:
        return self.__effective_time

    @property
    def entry_time(self) -> dt.datetime:
        return self.__entry_time

    @property
    def effective_version(self) -> int:
        return self.__effective_version

    @property
    def entry_version(self) -> int:
        return self.__entry_version

    @property
    @abstractmethod
    def id(self) -> tuple[type, tuple[str, ...]]:
        ...

    @property
    @abstractmethod
    def json_contents(self) -> bytes:
        ...

    @classmethod
    @abstractmethod
    def model_validate_json(cls, data: str | bytes) -> PersistableMixin:
        ...

    @property
    def object_type(self) -> str:
        return type_name(self.id[0])

    @property
    def object_id(self) -> bytes:
        return dumps(self.id[1])

    @classmethod
    def from_db_record(cls, record: DBRecord) -> PersistableMixin:
        typ = _one_of(cls) if cls.__subclasses__() else cls

        try:
            ret = TypeAdapter(typ).validate_json(record.contents)
        except ValidationError as e:
            if "does not match any of the expected tags" not in str(e):
                raise e

            # We are attempting to deserialise a class that has not been imported, attempt to import dynamically
            # This is slow

            dict_contents = loads(record.contents)
            typ = dict_contents.get("type_")
            if typ is None:
                raise RuntimeError(f"Failed to load {record.object_type} with id {record.object_id}: no type_ present")

            # ToDo: Add a warning

            module_name, _, _ = typ.rpartition(".")
            import_module(module_name)

            ret = TypeAdapter(typ).validate_python(dict_contents)

        object.__setattr__(ret, "_PersistableMixin__effective_time", record.effective_time)
        object.__setattr__(ret, "_PersistableMixin__entry_time", record.entry_time)
        object.__setattr__(ret, "_PersistableMixin__effective_version", record.effective_version)
        object.__setattr__(ret, "_PersistableMixin__entry_version", record.entry_version)

        return ret

    @classmethod
    def make_id(cls, *args, **kwargs) -> tuple[str, bytes]:
        id_type, id_fields = cls.id
        if id_type == UseDerived:
            raise RuntimeError("Can only be called on a persistable class")

        ret = ()

        keywords = {**{n: v for n, v in zip(id_fields[:len(args)], args)}, **kwargs}

        for name in id_fields:
            if name in keywords:
                ret += (keywords[name],)
            else:
                raise ValueError(f"Missing ID field {name}")

        return type_name(id_type), dumps(ret)

    @classmethod
    def check_persistable_class(cls, persistable: type, fields: tuple[str, ...]):
        bases: list[persistable] = [b for b in cls.__bases__ if issubclass(b, persistable)]
        if len(bases) > 1:
            raise TypeError(f"{cls} derives from multiple Persistable classes: {bases}")

        has_id = "id" in cls.__annotations__
        id_type, flds = bases[0].id
        if flds and id_type is not cls:
            # A base class defines id already

            if has_id:
                raise TypeError(f"{cls} cannot override id defined on {id_type}")

            if id_type == UseDerived:
                # Pull the id from the abstract base onto our type
                setattr(cls, "id", Id(*flds, typ=cls))
        elif not has_id and id_type != UseDerived:
            raise TypeError(f"{cls} must define an id")
        else:
            # Check the id fields all exist in our type
            _, flds = cls.id
            missing = [f for f in flds if f not in fields and f not in cls.__annotations__ and f not in cls.__dict__]

            if missing:
                raise TypeError(f"{missing} specified as id field(s) but not model field(s) of {cls}")
