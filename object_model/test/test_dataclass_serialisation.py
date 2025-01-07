from dataclasses import dataclass
from datetime import date
from typing import Any

from object_model import Base
from object_model._json import dumps, loads, schema


from .shared_dataclass_types import ContainerDC, Container2DC, NestedDC, OuterDC


def test_one_of():
    def test_container(container: ContainerDC):
        o = OuterDC(name="outer",
                    the_nested=NestedDC(name="nested", container=container),
                    date=date(1970, 1, 1),
                    the_list=[1, 3.0, date(1984, 1, 1)],
                    tuple=(1, 3.0, date(1984, 1, 1)))

        buffer = dumps(o)
        o_from_json = loads(buffer)

        # We expect the collections to converted to immutable versions

        assert o_from_json != o
        assert o_from_json.the_list == tuple(o.the_list)

    test_container(ContainerDC(name="container", contents={"foo": 1, "date": date.today()}))
    test_container(Container2DC(name="container", contents={"foo": 1}, rank=1))


def test_camel_case():
    c = Container2DC(name="container", contents={"foo": 1, "date": date.today()}, rank=1)
    o = OuterDC(name="outer",
                the_nested=NestedDC(name="nested", container=c),
                date=date(1970, 1, 1),
                the_list=[1, 3.0, date(1984, 1, 1)],
                tuple=(1, 3.0, date(1984, 1, 1)))

    buffer = dumps(o).decode()
    assert "theNested" in buffer
    assert "the_nested" not in buffer

    object_schema = schema(OuterDC)
    assert "theNested" in object_schema["properties"]
    assert "the_nested" not in object_schema["properties"]


def test_replace():
    c = Container2DC(name="container", contents={"foo": 1}, rank=1)
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
