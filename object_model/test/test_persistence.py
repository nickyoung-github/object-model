from time import sleep
from typing import Any

from object_model import NamedPersistableModel, OneOf
from object_model.db import DBFailedUpdateError, DBUnknownError, SqliteContext


class Container(NamedPersistableModel):
    contents: dict[str, Any]


class Container2(Container):
    rank: int


class Nested(NamedPersistableModel):
    container: OneOf[Container]


class Outer(NamedPersistableModel):
    the_nested: Nested
    the_version: int = 0


def test_roundtrip():
    c = Container2(name="container", contents={"foo": 1}, rank=1)
    o = Outer(name="outer", the_nested=Nested(name="nested", container=c))

    db = SqliteContext()
    result = db.write(o)

    assert result.result()

    oo = db.read(Outer, "outer").value
    assert oo == o


def test_update():
    c = Container2(name="container", contents={"foo": 1}, rank=1)
    o = Outer(name="outer", the_nested=Nested(name="nested", container=c))

    db = SqliteContext()

    result = db.write(o)
    assert result.result()

    oo = o.model_copy(update={"the_version": 1})

    try:
        # Will fail as the object has not been read
        db.write(oo).result()
    except DBFailedUpdateError:
        assert True
    else:
        assert False

    oo: Outer = db.read(Outer, "outer").value
    assert oo == o

    # Save the time so we can re-read the original version
    effective_time = oo.effective_time

    sleep(1)

    oo = oo.model_copy(update={"the_version": 1})
    assert oo != o

    result = db.write(oo)
    assert result.result()

    # Read the old version
    o_v1 = db.read(Outer, "outer", effective_time=effective_time).value
    assert o_v1 == o
    assert o_v1.effective_version == 1

    # Read the latest version
    o_v2 = db.read(Outer, "outer").value
    assert o_v2 == oo
    assert o_v2.effective_version == 2

    # Now update v1
    o_v11 = o_v1.model_copy(update={"the_version": 11})
    result = db.write(o_v11, as_of_effective_time=True)
    assert result.result()

    # Check that with just effective time we get the latest version ...
    o_v1_latest = db.read(Outer, "outer", effective_time=o_v1.effective_time).value
    assert o_v1_latest == o_v11

    # ... but that when we specify entry time too, we get the original
    o_v1_orig = db.read(Outer, "outer", effective_time=o_v1.effective_time, entry_time=o_v1.entry_time).value
    assert o_v1_orig == o_v1
