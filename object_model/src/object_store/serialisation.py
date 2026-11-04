from typing import Any

from .persistable import Persistable
from .reference import ReferenceResolver
from .store import ObjectStore


def dump(obj: Persistable, store: ObjectStore, json: bool = False):
    resolver = ReferenceResolver(store)
    adapter = obj.type_adapter()
    dump = adapter.dump_json if json else adapter.dump_python
    context = {"resolver": resolver, "by_reference": True}

    dump(obj, context=context)


def validate(typ: type[Persistable], data: dict[str, Any], store: ObjectStore, json: bool = False) -> Persistable:
    resolver = ReferenceResolver(store)
    adapter = typ.type_adapter()
    validate = adapter.validate_json if json else adapter.validate_python
    context = {"resolver": resolver, "by_reference": True}

    ret = validate(data, context=context)  # Discover dependencies
    
    if resolver.resolve():
        resolver.discovering = False
        ret = validate(data, context=context)
        
    return ret