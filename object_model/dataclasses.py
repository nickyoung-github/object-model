from __future__ import annotations as __annotations

from dataclasses import dataclass, field, fields, replace
from functools import cached_property
from hashlib import sha3_512
from typing import ClassVar, Literal

from .db.persistable import Id, PersistableMixin, UseDerived
from .json import TYPE_KEY, dumps


class __BaseMetaClass(type):
    def __new__(cls, cls_name, bases, namespace, **kwargs):
        type_ = f"{namespace['__module__']}.{cls_name}"
        annotations = namespace.setdefault("__annotations__", {})
        annotations[TYPE_KEY] = Literal[type_]
        namespace[TYPE_KEY] = field(default_factory=lambda: type_, init=False)  # Needed to forcibly set in __init__

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
class Immutable(Persistable):
    id: ClassVar[Id] = Id("content_hash")

    @cached_property
    def content_hash(self) -> bytes:
        # ToDo: Yes, I know this is wrong !!!
        return sha3_512(dumps(self)).digest()
