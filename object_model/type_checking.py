from dataclasses import field, is_dataclass
from datetime import date, datetime
from pydantic import BaseModel, Field
from typing import Annotated, Any, Literal, Union, get_origin, get_args

from .json import TYPE_KEY


__base_type_order = {datetime: -2, date: -1, int: -1}


def check_type(fld: str, typ: Any) -> Any:
    # Check that we have no non-serialisable or ambiguously serialisable types
    # Also, rewrite a couple of types to avoid common problems

    if typ in (object, Any):
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

            if origin is Union:
                # Ro-order the args of unions so that e.g. datetime comes before str

                object_types = tuple(t for t in args if issubclass(t, BaseModel) or is_dataclass(t))
                base_types = set(args).difference(object_types) if object_types else args
                base_types = tuple(sorted(base_types, key=lambda x: __base_type_order.get(x, 0)))

                # If we have e.g. Union[date, MyClass, MyOtherClass] we need to use a discriminated union for the
                # classes, so we need
                # Union[date, Annotated[Union[MyClass, MyOtherClass], Field(..., discriminator=TYPE_KEY)]

                if object_types:
                    disriminated_union = Annotated[Union[object_types], Field(..., discriminator=TYPE_KEY)]
                    return Union[base_types + (disriminated_union,)] if base_types else disriminated_union
                elif base_types:
                    return Union[base_types]

        return typ


def add_type_to_namespace(cls_name: str, namespace: dict[str, Any]):
    type_path = f"{namespace['__module__']}.{cls_name}"
    annotations_ = namespace.setdefault("__annotations__", {})
    if TYPE_KEY in annotations_:
        raise AttributeError(f"Cannot used reserved word {TYPE_KEY} as a field name")

    for name, typ in annotations_.items():
        annotations_[name] = check_type(name, typ)

    annotations_[TYPE_KEY] = Literal[type_path]
    namespace[TYPE_KEY] = field(default_factory=lambda: type_path, init=False)
