from pydantic._internal._config import ConfigWrapper
from pydantic_gubbins.typing import FrozenDict, Union
from typing import Any, Callable, Union as _Union, get_origin, get_args

from ._type_registry import register_type


def check_type(fld: str, typ: Any, immutable_collections: bool) -> Any:
    # Check that we have no non-serialisable or ambiguously serialisable types

    if typ in (object, Any, Callable):
        raise TypeError(f"{typ} is not a persistable type for {fld}")

    args = get_args(typ)
    origin = get_origin(typ) or typ

    if not args:
        if origin in (dict, list, set, tuple):
            raise TypeError(f"Cannot use untyped collection for {fld}")

    for arg in args:
        check_type(fld, arg, immutable_collections)

    if origin is _Union:
        return Union[args]

    if immutable_collections:
        if origin is set:
            return frozenset[args]
        elif origin is list:
            return tuple[args + (...,)]
        elif origin is dict:
            return FrozenDict[args]

    return typ


class TypeCheckMixin:
    def __new__(cls, cls_name: str, bases: tuple[type[Any], ...], namespace: dict[str, Any], **kwargs):
        model_config = ConfigWrapper.for_model(bases, namespace, kwargs)
        immutable_collections = model_config.frozen if model_config else False
        annotations = namespace.get("__annotations__", {})

        for name, typ in namespace.setdefault("__annotations__", {}).items():
            annotations[name] = check_type(name, typ, immutable_collections)

        ret = super().__new__(cls, cls_name, bases, namespace, **kwargs)
        register_type(ret)

        return ret
