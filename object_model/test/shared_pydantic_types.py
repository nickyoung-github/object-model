from datetime import date

from object_model import NamedPersistableModel, Subclass


class Container(NamedPersistableModel):
    contents: dict[str, date | str | float | int]


class Container2(Container):
    rank: int


class Nested(NamedPersistableModel):
    container: Subclass[Container]


class Outer(NamedPersistableModel):
    the_nested: Nested
    date: date | str


class Container3(Container2):
    date: date
