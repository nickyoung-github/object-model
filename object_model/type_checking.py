from dataclasses import field, is_dataclass
from datetime import date, datetime
from pydantic import BaseModel, Field
from typing import Annotated, Any, Callable, Literal, Union, get_origin, get_args

from .json import TYPE_KEY
from .typing import FrozenDict


__base_type_order = {datetime: -2, date: -1, int: -1}


def check_type(fld: str, typ: Any) -> Any:
    # Check that we have no non-serialisable or ambiguously serialisable types
    # Also, rewrite a couple of types to avoid common problems

    if typ in (object, Any, Callable):
        raise TypeError(f"{typ} is not a persistable type for {fld}")
    else:
        args = get_args(typ)
        origin = get_origin(typ) or typ

        if not args:
            if origin in (dict, list, set, tuple):
                raise TypeError(f"Cannot use untyped collection for {field}")
        else:
            for arg in args:
                check_type(fld, arg)

            if origin is set:
                return frozenset[args]
            elif origin is list:
                return tuple[args + (...,)]
            elif origin is dict:
                return FrozenDict[args]
            elif origin is Union:
                # Re-order the args of unions so that e.g. datetime comes before str

                object_types = tuple(t for t in args if issubclass(t, BaseModel) or is_dataclass(t))
                base_types = set(args).difference(object_types) if object_types else args
                base_types = tuple(sorted(base_types, key=lambda x: __base_type_order.get(x, 0)))

                # If we have e.g. Union[date, MyClass, MyOtherClass] we need to use a discriminated union for the
                # classes, so we need
                # Union[date, Annotated[Union[MyClass, MyOtherClass], Field(..., discriminator=TYPE_KEY)]

                if len(object_types) > 1:
                    disriminated_union = Annotated[Union[object_types], Field(..., discriminator=TYPE_KEY)]
                    return Union[base_types + (disriminated_union,)] if base_types else disriminated_union
                elif base_types:
                    return Union[base_types]

        return typ


def validate_types(cls_name: str, namespace: dict[str, Any]):
    type_path = f"{namespace['__module__']}.{cls_name}"
    annotations = namespace.setdefault("__annotations__", {})
    if TYPE_KEY in annotations:
        raise AttributeError(f"Cannot used reserved word {TYPE_KEY} as a field name")

    for name, typ in annotations.items():
        annotations[name] = check_type(name, typ)

    annotations[TYPE_KEY] = Literal[type_path]
    namespace[TYPE_KEY] = field(default_factory=lambda: type_path, init=False)
