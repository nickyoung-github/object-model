from pydantic import Field
from typing import _SpecialForm, Annotated, Union


def _one_of(param_typ: type):
    if "type_" not in param_typ.__annotations__:
        raise TypeError(f"Usage: OneOf[Type], where Type provides a type_ Literal")

    subclasses = set()
    stack = [param_typ]
    while stack:
        subclass = stack.pop()
        subclasses.add(subclass)
        stack.extend(subclass.__subclasses__())

    return Annotated[Union[tuple(subclasses)], Field(..., discriminator="type_")]


@_SpecialForm
def OneOf(_cls, param_typ: type):
    return _one_of(param_typ)
