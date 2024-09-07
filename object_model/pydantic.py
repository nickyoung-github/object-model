from __future__ import annotations as __annotations

from functools import cached_property
from hashlib import sha3_512
from pydantic import BaseModel as PydanticBaseModel, ConfigDict
from pydantic.alias_generators import to_camel
from pydantic._internal._model_construction import ModelMetaclass as PydanticModelMetaclass, PydanticGenericMetadata
from typing import Any, ClassVar, Literal


from .db.persistable import Id, PersistableMixin, UseDerived
from .json import TYPE_KEY, dumps


class __ModelMetaclass(PydanticModelMetaclass):
    def __new__(
        mcs,
        cls_name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        __pydantic_generic_metadata__: PydanticGenericMetadata | None = None,
        __pydantic_reset_parent_namespace__: bool = True,
        **kwargs: Any,
    ) -> type:
        annotations = namespace.setdefault("__annotations__", {})
        if TYPE_KEY in annotations:
            raise AttributeError(f"Cannot used reserved word {TYPE_KEY} as a field name")

        type_ = f"{namespace['__module__']}.{cls_name}"
        annotations[TYPE_KEY] = Literal[type_]
        namespace[TYPE_KEY] = type_

        return super().__new__(mcs, cls_name, bases, namespace, **kwargs)


class BaseModel(PydanticBaseModel, metaclass=__ModelMetaclass):
    model_config = ConfigDict(frozen=True, populate_by_name=True, alias_generator=to_camel)

    def replace(self, /, **changes):
        return self.model_copy(update=changes)


class PersistableModel(BaseModel, PersistableMixin):
    id: ClassVar[Id] = Id()

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        if "__pydantic_init_subclass__" in cls.__dict__:
            raise RuntimeError(f"Redefinition of __pydantic_init_subclass__ by {cls} is not allowed")

        cls._check_persistable_class(PersistableModel, tuple(cls.model_fields.keys()))

    def model_post_init(self, __context: Any) -> None:
        PersistableMixin.__init__(self)


class NamedPersistableModel(PersistableModel):
    id: ClassVar[Id] = Id("name", typ=UseDerived)

    name: str


class ImmutableModel(PersistableModel):
    id: ClassVar[Id] = Id("content_hash")

    @cached_property
    def content_hash(self) -> bytes:
        # ToDo: Yes, I know this is wrong !!!
        return sha3_512(dumps(self)).digest()
