from __future__ import annotations as __annotations

from dataclasses import dataclass, field, fields
from functools import cached_property, lru_cache
from hashlib import sha3_512
from pydantic import ConfigDict, TypeAdapter
from pydantic.alias_generators import to_camel
from typing import Any, ClassVar, Literal

from .db.persistable import Id, PersistableMixin, UseDerived


class __BaseMetaClass(type):
    def __new__(cls, cls_name, bases, namespace, **kwargs):
        type_ = f"{namespace['__module__']}.{cls_name}"
        annotations = namespace.setdefault("__annotations__", {})
        annotations["type_"] = Literal[type_]
        namespace["type_"] = field(default_factory=lambda: type_, init=False)  # Needed to forcibly set in __init__

        return super().__new__(cls, cls_name, bases, namespace, **kwargs)


@dataclass(frozen=True)
class Base(metaclass=__BaseMetaClass):
    # Using pyantic stuff here is unfortunate. It's just quite painful to make some bits like json schema generation
    # and field aliasing work

    # Pydantic has done some quite evil-looking things under the covers, which confuses the type checking on PyCharm

    class Config:
        alias_generator = to_camel

    @classmethod
    @lru_cache(maxsize=1)
    def __type_adaptor(cls) -> TypeAdapter:
        # This madness is required to get aliases to work in the json schema. It seems to ignore the class-level
        # Config for building the schema. Naturally, for actually generating the JSON, it uses that Config and ignores
        # the one supplied to TypeAdapter.

        ret = TypeAdapter(cls)
        ret._config = ConfigDict(frozen=True, populate_by_name=True, alias_generator=to_camel)
        with ret._with_frame_depth(1):
            ret._init_core_attrs(rebuild_mocks=False)

        return ret

    @property
    def json_contents(self) -> bytes:
        return self.__type_adaptor().dump_json(self, by_alias=True)

    @classmethod
    def model_json_schema(cls) ->\
            tuple[dict[tuple[Any, Literal['validation', 'serialization']], dict[str, Any]], dict[str, Any]]:
        return cls.__type_adaptor().json_schema()

    @classmethod
    def model_validate(cls, data: dict[str, Any]) -> Base:
        return cls.__type_adaptor().validate_python(data)

    @classmethod
    def model_validate_json(cls, data: str | bytes) -> Base:
        return cls.__type_adaptor().validate_json(data)


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
        return sha3_512(self.json_contents).digest()
