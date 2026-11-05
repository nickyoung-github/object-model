from abc import abstractmethod
from functools import lru_cache
from hashlib import sha512
from orjson import OPT_SORT_KEYS, dumps
from pydantic import TypeAdapter
from typing import Annotated, ClassVar, get_origin, get_type_hints


class _IdFieldIndicator: ...
    
type IdField[T] = Annotated[T, _IdFieldIndicator]


class Persistable:
    # ToDo: Replace this by nobbling the compare attribute in dataclass field?
    def __eq__(self, other):
        if not isinstance(other, type(self)):
            raise NotImplemented
        return all(getattr(self, f) == getattr(other, f) for f in self.id_fields())
    
    def __hash__(self):
        return int(self.object_id, 16)
    
    @classmethod
    @lru_cache
    def type_adapter(cls) -> TypeAdapter:
        # TypeAdapter construction is quite expensive, so cache it
        return TypeAdapter(cls)

    @classmethod
    @abstractmethod
    def id_fields(cls) -> set[str]:
        ...
    
    @property
    def object_id(self) -> str:
        values = self.type_adapter().dump_python(self, include=self.id_fields(), mode="json")
        payload = dumps(values, option=OPT_SORT_KEYS)
        return sha512(payload).hexdigest()
    

class Identifiable(Persistable):
    def __init_subclass__(cls, **kwargs):
        if not cls.id_fields():
            raise TypeError(f"{cls!r} does not define any id fields")
        
    @classmethod
    def id_fields(cls) -> set[str]:
        return set(
            name
            for name, hint in get_type_hints(cls, include_extras=True)
            if get_origin(hint) is IdField
        )


class Unique(Persistable):
    @classmethod
    def id_fields(cls) -> set[str]:
        return set(
            name
            for name, hint in get_type_hints(cls)
            if get_origin(hint) is not ClassVar
        )
