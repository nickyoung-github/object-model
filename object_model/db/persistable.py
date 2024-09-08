from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass, field
import datetime as dt
from functools import cached_property
from hashlib import sha3_512
from orjson import dumps, loads
from typing import Any, Literal

from ..json import TYPE_KEY, loads


def type_name(typ: type) -> str:
    return f"{typ.__module__}.{typ.__name__}"


def add_type_to_namespace(cls_name: str, namespace: dict[str, Any]):
    type_ = f"{namespace['__module__']}.{cls_name}"
    annotations_ = namespace.setdefault("__annotations__", {})
    if TYPE_KEY in annotations_:
        raise AttributeError(f"Cannot used reserved word {TYPE_KEY} as a field name")

    annotations_[TYPE_KEY] = Literal[type_]
    namespace[TYPE_KEY] = field(default_factory=lambda: type_, init=False)  # Needed to forcibly set in __init__


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
    def object_type(self) -> str:
        return type_name(self.id[0])

    @property
    def object_id(self) -> bytes:
        return dumps(self.id[1])

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
    def _check_persistable_class(cls, persistable: type, fields: tuple[str, ...]):
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
            missing = [f for f in flds if f not in fields and f not in cls.__annotations__ and not hasattr(cls, f)]
            if missing:
                raise TypeError(f"{missing} specified as id field(s) but not model field(s) of {cls}")

    @classmethod
    def from_db_record(cls, record: DBRecord) -> PersistableMixin:
        ret: PersistableMixin = loads(record.contents)
        ret.set_db_info(record)
        return ret

    def set_db_info(self, record: DBRecord):
        object.__setattr__(self, "_PersistableMixin__effective_time", record.effective_time)
        object.__setattr__(self, "_PersistableMixin__entry_time", record.entry_time)
        object.__setattr__(self, "_PersistableMixin__effective_version", record.effective_version)
        object.__setattr__(self, "_PersistableMixin__entry_version", record.entry_version)


class ImmutableMixin:
    @cached_property
    def content_hash(self) -> bytes:
        # ToDo: Yes, I know this is wrong !!!
        return sha3_512(dumps(self)).digest()
