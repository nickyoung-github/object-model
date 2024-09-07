import datetime as dt
from pydantic import ValidationError
from typing import Any

from object_model import NamedPersistableModel, OneOf


class Container(NamedPersistableModel):
    contents: dict[str, Any]


class Container2(Container):
    rank: int


class Nested(NamedPersistableModel):
    container: OneOf[Container]


class Outer(NamedPersistableModel):
    the_nested: Nested


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

    as_dict = o.model_dump(by_alias=True)

    assert "theNested" in as_dict
    assert "the_nested" not in as_dict


def test_invalid_one_of_fails():
    c3 = Container3(name="container", contents={"foo": 1}, rank=2, date=dt.date.today())

    try:
        # This should fail as Container3 was declared after Nested and the OneOf will not see it
        Outer(name="outer", the_nested=Nested(name="nested", container=c3))
    except ValidationError:
        assert True
    else:
        assert False


def test_replace():
    c = Container2(name="container", contents={"foo": 1}, rank=1)
    c2 = c.replace(rank=2)

    assert c2.rank == 2
