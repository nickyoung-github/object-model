from dataclasses import dataclass
from datetime import date
from typing import Any

from object_model import Base, NamedPersistable, Subclass
from object_model.json import dumps, loads


@dataclass(frozen=True)
class Container(NamedPersistable):
    contents: dict[str, date | str | float | int]


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
    the_list: list[date | str | float | int]
    tuple: tuple[date | str | float | int, ...]


@dataclass(frozen=True)
class Container3(Container2):
    date: date


def test_one_of():
    def test_container(container: Container):
        o = Outer(name="outer",
                  the_nested=Nested(name="nested", container=container),
                  date=date(1970, 1, 1),
                  the_list=[1, 3.0, date(1984, 1, 1)],
                  tuple=(1, 3.0, date(1984, 1, 1)))

        buffer = dumps(o)
        o_from_json = loads(buffer)

        # We expect the collections to converted to immutable versions

        assert o_from_json != o
        assert o_from_json.the_list == tuple(o.the_list)

    test_container(Container(name="container", contents={"foo": 1, "date": date.today()}))
    test_container(Container2(name="container", contents={"foo": 1}, rank=1))


def test_camel_case():
    c = Container2(name="container", contents={"foo": 1, "date": date.today()}, rank=1)
    o = Outer(name="outer",
              the_nested=Nested(name="nested", container=c),
              date=date(1970, 1, 1),
              the_list=[1, 3.0, date(1984, 1, 1)],
              tuple=(1, 3.0, date(1984, 1, 1)))

    buffer = dumps(o).decode("utf-8")

    assert "theNested" in buffer
    assert "the_nested" not in buffer


def test_replace():
    c = Container2(name="container", contents={"foo": 1}, rank=1)
    c2 = c.replace(rank=2)

    assert c2.rank == 2


def test_unsupported_types():
    try:
        @dataclass(frozen=True)
        class Bad(Base):
            foo: Any

        assert False
    except TypeError:
        assert True

    try:
        @dataclass(frozen=True)
        class BadCollectiobn(Base):
            foo: dict[str, Any]

        assert False
    except TypeError:
        assert True
