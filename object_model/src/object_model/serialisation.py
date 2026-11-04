from typing import Any, Callable

from .persistable import Persistable
from .reference import ReferenceResolver
from .store import ObjectStore


def __dump(obj: Persistable, store: ObjectStore, fn: Callable):
    resolver = ReferenceResolver(store)
    context = {"resolver": resolver, "by_reference": True}
    return fn(obj, context=context)


def dump_python(obj: Persistable, store: ObjectStore) -> dict[str, Any]:
    adapter = obj.type_adapter()
    return __dump(obj, store, adapter.dump_python)


def dump_json(obj: Persistable, store: ObjectStore) -> bytes:
    return __dump(obj, store, obj.type_adapter().dump_json)


def __validate(data: dict[str, Any] | bytes, store: ObjectStore, fn: Callable) -> Persistable:
    resolver = ReferenceResolver(store)
    context = {"resolver": resolver, "by_reference": True}
    ret = fn(data, context=context)  # Discover dependencies
    
    if resolver.resolve():
        resolver.discovering = False
        ret = fn(data, context=context)
        
    return ret


def validate_python(typ: type[Persistable], data: dict[str, Any], store: ObjectStore) -> Persistable:
    return __validate(data, store, typ.type_adapter().validate_python)


def validate_json(typ: type[Persistable], data: bytes, store: ObjectStore) -> Persistable:
    return __validate(data, store, typ.type_adapter().validate_json)
