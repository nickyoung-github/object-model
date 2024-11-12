from datetime import date
from pydantic import ValidationError
from typing import Any

from object_model import BaseModel, NamedPersistableModel, Subclass
from object_model._json import dumps, loads


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


def test_one_of():
    def test_container(container: Container):
        o = Outer(name="outer", the_nested=Nested(name="nested", container=container), date=date(1970, 1, 1))

        buffer = dumps(o)
        o_from_json = loads(buffer, typ=Outer)

        assert o_from_json == o

    test_container(Container(name="container", contents={"foo": 1, "date": date(1970, 1, 1)}))
    test_container(Container2(name="container", contents={"foo": 1}, rank=1))


def test_camel_case():
    c = Container2(name="container", contents={"foo": 1}, rank=1)
    o = Outer(name="outer", the_nested=Nested(name="nested", container=c), date=date(1970, 1, 1))

    as_dict = o.model_dump(by_alias=True)

    assert "theNested" in as_dict
    assert "the_nested" not in as_dict


def test_invalid_one_of_fails():
    c3 = Container3(name="container", contents={"foo": 1}, rank=2, date=date.today())

    try:
        # This should fail as Container3 was declared after Nested and the OneOf will not see it
        Outer(name="outer", the_nested=Nested(name="nested", container=c3), date=date.today())
    except ValidationError:
        assert True
    else:
        assert False


def test_replace():
    c = Container2(name="container", contents={"foo": 1}, rank=1)
    c2 = c.replace(rank=2)

    assert c2.rank == 2


def test_unsupported_types():
    try:
        class Bad(BaseModel):
            foo: Any

        assert False
    except TypeError:
        assert True

    try:
        class BadCollectiobn(BaseModel):
            foo: dict[str, Any]

        assert False
    except TypeError:
        assert True


def test_immutable_collections():
    class MyCollections(BaseModel):
        my_list: list[str]
        my_dict: dict[str, str]
        my_set: set[str]

    c = MyCollections(my_list=["a", "b"], my_dict={"a": "A", "b": "B"}, my_set={"a", "b"})

    try:
        c.my_list.append("c")
        assert False
    except AttributeError:
        assert True

    try:
        c.my_dict["c"] = "C"
        assert False
    except TypeError:
        assert True

    try:
        c.my_set.add("c")
        assert False
    except AttributeError:
        assert True
