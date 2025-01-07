from datetime import date
from pydantic_gubbins.typing import SubclassOf

from object_model import NamedPersistableModel


class Container(NamedPersistableModel):
    contents: dict[str, date | str | float | int]


class Container2(Container):
    rank: int


class Nested(NamedPersistableModel):
    container: SubclassOf[Container]


class Outer(NamedPersistableModel):
    the_nested: Nested
    date: date | str


class Container3(Container2):
    date: date
