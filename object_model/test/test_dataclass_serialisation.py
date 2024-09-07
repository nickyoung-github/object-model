from dataclasses import dataclass
from datetime import date
from typing import Any

from object_model import NamedPersistable, Subclass
from object_model.json import dumps, loads


@dataclass(frozen=True)
class Container(NamedPersistable):
    contents: dict[str, Any]


@dataclass(frozen=True)
class Container2(Container):
    rank: int


@dataclass(frozen=True)
class Nested(NamedPersistable):
    container: Subclass[Container]


@dataclass(frozen=True)
class Outer(NamedPersistable):
    the_nested: Nested
    date: date


@dataclass(frozen=True)
class Container3(Container2):
    date: date


def test_one_of():
    def test_container(container: Container):
        o = Outer(name="outer", the_nested=Nested(name="nested", container=container), date=date(1970, 1, 1))

        buffer = dumps(o)
        o_from_json = loads(buffer)

        assert o_from_json == o

    test_container(Container(name="container", contents={"foo": 1, "date": date.today()}))
    test_container(Container2(name="container", contents={"foo": 1}, rank=1))


def test_camel_case():
    c = Container2(name="container", contents={"foo": 1}, rank=1)
    o = Outer(name="outer", the_nested=Nested(name="nested", container=c), date=date(1970, 1, 1))

    buffer = dumps(o).decode("UTF-8")

    assert "theNested" in buffer
    assert "the_nested" not in buffer


def test_replace():
    c = Container2(name="container", contents={"foo": 1}, rank=1)
    c2 = c.replace(rank=2)

    assert c2.rank == 2

