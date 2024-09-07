from pydantic import Field
from typing import Annotated, Union

from .json import TYPE_KEY


class _SpecialForm:
    # Copied from typing but does not cache __get_item__
    # This is to avoid hard-to-debug issues for e.g. Subclass

    __slots__ = ('_name', '__doc__', '_getitem')
    __iter__ = None

    def __init__(self, getitem):
        self._getitem = getitem
        self._name = getitem.__name__
        self.__doc__ = getitem.__doc__

    def __init_subclass__(cls, /, *args, **kwds):
        if '_root' not in kwds:
            raise TypeError("Cannot subclass special typing classes")

    def __getattr__(self, item):
        if item in {'__name__', '__qualname__'}:
            return self._name

        raise AttributeError(item)

    def __mro_entries__(self, bases):
        raise TypeError(f"Cannot subclass {self!r}")

    def __repr__(self):
        return 'typing.' + self._name

    def __reduce__(self):
        return self._name

    def __call__(self, *args, **kwds):
        raise TypeError(f"Cannot instantiate {self!r}")

    def __or__(self, other):
        return Union[self, other]

    def __ror__(self, other):
        return Union[other, self]

    def __instancecheck__(self, obj):
        raise TypeError(f"{self} cannot be used with isinstance()")

    def __subclasscheck__(self, cls):
        raise TypeError(f"{self} cannot be used with issubclass()")

    def __getitem__(self, parameters):
        return self._getitem(self, parameters)


def _one_of(param_typ: type):
    if TYPE_KEY not in param_typ.__annotations__:
        raise TypeError(f"Usage: OneOf[Type], where Type provides a {TYPE_KEY} Literal")

    subclasses = set()
    stack = [param_typ]
    while stack:
        subclass = stack.pop()
        subclasses.add(subclass)
        stack.extend(subclass.__subclasses__())

    return Annotated[Union[tuple(subclasses)], Field(..., discriminator=TYPE_KEY)]


@_SpecialForm
def Subclass(_cls, param_typ: type):
    return _one_of(param_typ)
