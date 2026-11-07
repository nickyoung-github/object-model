from abc import abstractmethod
from functools import lru_cache
from hashlib import sha512
from orjson import OPT_SORT_KEYS, dumps
from pathlib import Path
from pydantic import TypeAdapter
from typing import Annotated, ClassVar, TypeVar, get_origin, get_type_hints, get_args

from .type_registry import TypeRegistry
from .typed_cache import typed_cache


class _IdFieldIndicator: ...

T = TypeVar("T")
IdField = Annotated[T, _IdFieldIndicator]


class Persistable:
    # ToDo: Replace this by nobbling the compare attribute in dataclass field?
    def __eq__(self, other):
        if not isinstance(other, type(self)):
            raise NotImplemented
        return all(getattr(self, f) == getattr(other, f) for f in self.id_fields())
    
    def __hash__(self):
        return int(self.object_id.name, 16)

    @classmethod
    @abstractmethod
    def id_fields(cls) -> set[str]:
        ...
    
    @classmethod
    def make_object_id(cls, **kwargs) -> Path:
        # ToDo: Is there any type hinting magic we can do here to give us the signature of the
        #       id fields? Not in the current iterations of python, without codegen?
        id_fields = cls.id_fields()
        if len(kwargs) != len(id_fields) and not id_fields.issubset(kwargs.keys()):
            raise RuntimeError(f"Must supply id fields only")

        values = TypeAdapter(dict).dump_python(kwargs, mode="json")
        payload = dumps(values, option=OPT_SORT_KEYS)
        name = sha512(payload).hexdigest()
        
        return Path(TypeRegistry().registered_name(cls)).joinpath(name)
        
    @classmethod
    @lru_cache
    def type_adapter(cls) -> TypeAdapter:
        # TypeAdapter construction is quite expensive, so cache it
        return TypeAdapter(cls)

    @property
    def object_id(self) -> Path:
        values = {f: getattr(self, f) for f in self.id_fields()}
        return self.make_object_id(**values)
    

class Identifiable(Persistable):
    def __init_subclass__(cls, **kwargs):
        if not cls.id_fields():
            raise TypeError(f"{cls!r} does not define any id fields")
        
    @classmethod
    @typed_cache
    def id_fields(cls) -> set[str]:
        return set(
            name
            for name, hint in get_type_hints(cls, include_extras=True).items()
            if get_origin(hint) is Annotated and _IdFieldIndicator in get_args(hint)
        )


class Unique(Persistable):
    @classmethod
    @typed_cache
    def id_fields(cls) -> set[str]:
        return set(
            name
            for name, hint in get_type_hints(cls)
            if get_origin(hint) is not ClassVar
        )
