from __future__ import annotations

from abc import abstractmethod
from datetime import datetime
from functools import cached_property
from hashlib import sha3_512
from uuid import UUID

from .object_record import ObjectRecord
from .._descriptors import Id
from .._json import dumps, loads
from .._type_registry import CLASS_TYPE_KEY, TYPE_KEY


class UseDerived:
    ...


class PersistableMixin:
    def __init__(self):
        if self.id[0] is UseDerived:
            raise RuntimeError(f"Class {type(self)} cannot be constructed")

        # This mixin is used by frozen dataclasses, which stop you setting private members (annoyingly)
        self.__init(datetime.max, datetime.max, 0, 0)

    @property
    def effective_time(self) -> datetime:
        return self.__effective_time

    @property
    def entry_time(self) -> datetime:
        return self.__entry_time

    @property
    def effective_version(self) -> int:
        return self.__effective_version

    @property
    def entry_version(self) -> int:
        return self.__entry_version

    @property
    def object_store_id(self) -> UUID | None:
        return self.__object_store_id

    @property
    @abstractmethod
    def id(self) -> tuple[type, tuple[str, ...]]:
        ...

    @property
    def object_type(self) -> str:
        return getattr(self, TYPE_KEY)

    @property
    def object_id_type(self) -> str:
        return getattr(self.id[0], CLASS_TYPE_KEY)

    @property
    def object_id(self) -> bytes:
        return dumps(self.id[1])

    @property
    def object_contents(self) -> bytes:
        return dumps(self)

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

        return getattr(id_type, CLASS_TYPE_KEY), dumps(ret)

    @classmethod
    def _check_persistable_class(cls):
        bases: list[type[PersistableMixin]] = [b for b in cls.__bases__ if issubclass(b, PersistableMixin)]
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
            all_fields = cls._fields()
            missing = [f for f in flds if f not in all_fields and f not in cls.__annotations__ and not hasattr(cls, f)]
            if missing:
                raise TypeError(f"{missing} specified as id field(s) but not model field(s) of {cls}")

    @classmethod
    def from_object_record(cls, record: ObjectRecord) -> PersistableMixin:
        ret: PersistableMixin = loads(record.object_contents, record.object_type)
        ret.init_from_record(record)
        return ret

    def init_from_record(self, record: ObjectRecord):
        self.__init(record.effective_time, record.entry_time, record.effective_version, record.entry_version,
                    record.object_store_id)

    def __init(self, effective_time: datetime, entry_time: datetime, effective_version: int, entry_version: int,
               object_store_id: UUID | None = None):
        object.__setattr__(self, "_PersistableMixin__effective_time", effective_time)
        object.__setattr__(self, "_PersistableMixin__entry_time", entry_time)
        object.__setattr__(self, "_PersistableMixin__effective_version", effective_version)
        object.__setattr__(self, "_PersistableMixin__entry_version", entry_version)
        object.__setattr__(self, "_PersistableMixin__object_store_id", object_store_id)


class ImmutableMixin:
    @cached_property
    def content_hash(self) -> bytes:
        # ToDo: Yes, I know this is wrong !!!
        return sha3_512(dumps(self)).digest()
