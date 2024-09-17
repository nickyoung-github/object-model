from __future__ import annotations as __annotations

from dataclasses import dataclass, fields, replace
from inspect import currentframe, getframeinfo
from pydantic.alias_generators import to_camel
from typing import ClassVar

from .db.persistable import ImmutableMixin, PersistableMixin, UseDerived
from .descriptors import Id
from .replace import ReplaceMixin
from .type_checking import add_type_to_namespace


class __BaseMetaClass(type):
    def __new__(cls, cls_name, bases, namespace, **kwargs):
        add_type_to_namespace(cls_name, namespace)
        return super().__new__(cls, cls_name, bases, namespace, **kwargs)


@dataclass(frozen=True)
class Base(ReplaceMixin, metaclass=__BaseMetaClass):
    class Config:
        alias_generator = to_camel

    def __getattribute__(self, item):
        # TODO: This is probably a bad idea and may be retired
        ret = super().__getattribute__(item)
        if isinstance(ret, ReplaceMixin):
            caller = getframeinfo(currentframe().f_back)
            self._post_getattribute(item, ret, (caller.positions.end_lineno, caller.filename))

        return ret

    def _replace(self, /, **changes):
        return replace(self, **changes)


@dataclass(frozen=True)
class Persistable(Base, PersistableMixin):
    id: ClassVar[Id] = Id()

    def __init_subclass__(cls, **kwargs):
        if "__init_subclass__" in cls.__dict__:
            raise RuntimeError(f"Redefinition of __init_subclass__ by {cls} is not allowed")

        cls._check_persistable_class(Persistable, tuple(f.name for f in fields(cls)))

    def __post_init__(self):
        PersistableMixin.__init__(self)


@dataclass(frozen=True)
class NamedPersistable(Persistable):
    id: ClassVar[Id] = Id("name", typ=UseDerived)
    name: str


@dataclass(frozen=True)
class Immutable(Persistable, ImmutableMixin):
    id: ClassVar[Id] = Id("content_hash")
