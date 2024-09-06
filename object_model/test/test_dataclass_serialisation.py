from dataclasses import dataclass
import datetime as dt
from typing import Any

from object_model import NamedPersistable, OneOf


@dataclass(frozen=True)
class Container(NamedPersistable):
    contents: dict[str, Any]


@dataclass(frozen=True)
class Container2(Container):
    rank: int


@dataclass(frozen=True)
class Nested(NamedPersistable):
    container: OneOf[Container]


@dataclass(frozen=True)
class Outer(NamedPersistable):
    the_nested: Nested


@dataclass(frozen=True)
class Container3(Container2):
    date: dt.date


def test_one_of():
    def test_container(container: Container):
        o = Outer(name="outer", the_nested=Nested(name="nested", container=container))

        buffer = o.json_contents
        o_from_json = Outer.model_validate_json(buffer)

        assert o_from_json == o

    test_container(Container(name="container", contents={"foo": 1}))
    test_container(Container2(name="container", contents={"foo": 1}, rank=1))


def test_camel_case():
    c = Container2(name="container", contents={"foo": 1}, rank=1)
    o = Outer(name="outer", the_nested=Nested(name="nested", container=c))

    buffer = o.json_contents.decode("UTF-8")

    assert "theNested" in buffer
    assert "the_nested" not in buffer
