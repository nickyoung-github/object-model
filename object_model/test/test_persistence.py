from datetime import date
from time import sleep

from object_model.store import FailedUpdateError, MemoryStore, WrongStoreError

from .shared_pydantic_types import Container, Container2, Container3, Nested, Outer


def test_roundtrip():
    c = Container2(name="container", contents={"foo": 1}, rank=1)
    o = Outer(name="outer", the_nested=Nested(name="nested", container=c), date=date.today())

    db = MemoryStore()

    assert db.write(o).result()
    assert o == db.read(Outer, "outer").value


def test_update():
    class Storable(Outer):
        the_version: int = 0

    c = Container2(name="container", contents={"foo": 1}, rank=1)
    o = Storable(name="outer", the_nested=Nested(name="nested", container=c), date=date.today())

    db = MemoryStore()

    assert db.write(o).result()

    sleep(0.2)

    oo = o.replace(the_version=1)
    assert db.write(oo).result()
    assert oo == db.read(Storable, "outer").value

    # Read the old version
    o_v1 = db.read(Storable, "outer", effective_time=o.effective_time).value
    assert o_v1 == o
    assert o_v1.effective_version == 1
    assert o_v1.the_version == 0

    # Read the latest version
    o_v2 = db.read(Storable, "outer").value
    assert o_v2 == oo
    assert o_v2.effective_version == 2
    assert o_v2.the_version == 1

    # Now update v1
    o_v11 = o_v1.replace(the_version=11)
    assert db.write(o_v11, as_of_effective_time=True).result()

    # Check that with just effective time we get the latest version ...
    o_v1_latest = db.read(Storable, "outer", effective_time=o_v1.effective_time).value
    assert o_v1_latest == o_v11
    assert o_v1_latest.effective_version == 1
    assert o_v1_latest.entry_version == 2

    # ... but that when we specify entry time too, we get the original
    o_v1_orig = db.read(Storable, "outer", effective_time=o_v1.effective_time, entry_time=o_v1.entry_time).value
    assert o_v1_orig == o_v1
    assert o_v1_orig.effective_version == 1
    assert o_v1_orig.entry_version == 1


def test_load_via_base():
    c3 = Container3(name="container3", contents={"foo": 1}, rank=2, date=date.today())
    db = MemoryStore()

    assert db.write(c3).result()

    c = db.read(Container, "container3").value
    assert c == c3


def test_version_checking():
    c3 = Container3(name="container3", contents={"foo": 1}, rank=2, date=date.today())
    db = MemoryStore()

    assert db.write(c3).result()
    assert db.write(c3).result()

    c3_1 = Container3(name="container3", contents={"foo": 1}, rank=2, date=date.today())

    try:
        # This should fail due to a version and resulting integrity check
        db.write(c3_1).result()
    except FailedUpdateError:
        assert True
    else:
        assert False


def test_multiple_store_write():
    c3 = Container3(name="container3", contents={"foo": 1}, rank=2, date=date.today())
    db1 = MemoryStore()
    db2 = MemoryStore()

    assert db1.write(c3).result()
    assert db1.write(c3).result()

    try:
        db2.write(c3).result()
    except WrongStoreError:
        assert True
    else:
        assert False
