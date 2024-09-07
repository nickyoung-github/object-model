from __future__ import annotations as __annotations

from dataclasses import dataclass, fields, replace
from typing import ClassVar

from .db.persistable import Id, ImmutableMixin, PersistableMixin, UseDerived, add_type_to_namespace


class __BaseMetaClass(type):
    def __new__(cls, cls_name, bases, namespace, **kwargs):
        add_type_to_namespace(cls_name, namespace)
        return super().__new__(cls, cls_name, bases, namespace, **kwargs)


@dataclass(frozen=True)
class Base(metaclass=__BaseMetaClass):
    def replace(self, /, **changes):
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
