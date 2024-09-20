from __future__ import annotations as __annotations

from inspect import currentframe, getframeinfo
from pydantic import BaseModel as PydanticBaseModel, ConfigDict
from pydantic.alias_generators import to_camel
from pydantic._internal._model_construction import ModelMetaclass as PydanticModelMetaclass
from typing import Any, ClassVar


from .db.persistable import ImmutableMixin, PersistableMixin, UseDerived
from .descriptors import Id
from .replace import ReplaceMixin
from .type_checking import add_type_to_namespace


class __ModelMetaclass(PydanticModelMetaclass):
    def __new__(mcs, cls_name: str, bases: tuple[type[Any], ...], namespace: dict[str, Any], **kwargs):
        add_type_to_namespace(cls_name, namespace)
        return super().__new__(mcs, cls_name, bases, namespace, **kwargs)


class BaseModel(PydanticBaseModel, ReplaceMixin, metaclass=__ModelMetaclass):
    model_config = ConfigDict(frozen=True, populate_by_name=True, alias_generator=to_camel)

    def __getattribute__(self, item):
        # TODO: This is probably a bad idea and may be retired

        ret = super().__getattribute__(item)

        if isinstance(ret, ReplaceMixin):
            caller = getframeinfo(currentframe().f_back)
            self._post_getattribute(item, ret, (caller.positions.end_lineno, caller.filename))

        return ret

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
