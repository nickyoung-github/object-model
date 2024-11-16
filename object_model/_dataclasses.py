from __future__ import annotations as __annotations

from dataclasses import dataclass, fields, replace
from functools import cached_property
from pydantic.alias_generators import to_camel
from typing import ClassVar

from .store.persistable import ImmutableMixin, PersistableMixin, UseDerived
from ._descriptors import Id
from ._replace import ReplaceMixin
from ._type_checking import TypeCheckMixin


class __BaseMetaClass(TypeCheckMixin, type):
    pass


@dataclass(frozen=True)
class Base(ReplaceMixin, metaclass=__BaseMetaClass):
    class Config:
        alias_generator = to_camel

    @cached_property
    def _fields(self) -> set[str]:
        return set(f.name for f in fields(self))

    def _replace(self, /, **changes):
        return replace(self, **changes)


@dataclass(frozen=True)
class Persistable(Base, PersistableMixin):
    id: ClassVar[Id] = Id()

    def __init_subclass__(cls, **kwargs):
        if "__init_subclass__" in cls.__dict__:
            raise RuntimeError(f"Redefinition of __init_subclass__ by {cls} is not allowed")

        cls._check_persistable_class(tuple(f.name for f in fields(cls)))

    def __post_init__(self):
        PersistableMixin.__init__(self)


@dataclass(frozen=True)
class NamedPersistable(Persistable):
    id: ClassVar[Id] = Id("name", typ=UseDerived)
    name: str


@dataclass(frozen=True)
class Immutable(Persistable, ImmutableMixin):
    id: ClassVar[Id] = Id("content_hash")
