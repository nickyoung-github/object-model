from __future__ import annotations as __annotations

from pydantic import BaseModel as PydanticBaseModel, ConfigDict
from pydantic.alias_generators import to_camel
from pydantic._internal._model_construction import ModelMetaclass as PydanticModelMetaclass
from typing import Any, ClassVar


from .store.persistable import ImmutableMixin, PersistableMixin, UseDerived
from ._descriptors import Id
from ._replace import ReplaceMixin
from ._type_checking import TypeCheckMixin


class __ModelMetaclass(TypeCheckMixin, PydanticModelMetaclass):
    pass


class BaseModel(PydanticBaseModel, ReplaceMixin, metaclass=__ModelMetaclass):
    model_config = ConfigDict(frozen=True, populate_by_name=True, alias_generator=to_camel)

    def _replace(self, /, **changes):
        return self.model_copy(update=changes)


class PersistableModel(BaseModel, PersistableMixin):
    id: ClassVar[Id] = Id()

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        if "__pydantic_init_subclass__" in cls.__dict__:
            raise RuntimeError(f"Redefinition of __pydantic_init_subclass__ by {cls} is not allowed")

        cls._check_persistable_class(tuple(cls.model_fields.keys()))

    def model_post_init(self, __context: Any) -> None:
        PersistableMixin.__init__(self)


class NamedPersistableModel(PersistableModel):
    id: ClassVar[Id] = Id("name", typ=UseDerived)
    name: str


class ImmutableModel(PersistableModel, ImmutableMixin):
    id: ClassVar[Id] = Id("content_hash")
