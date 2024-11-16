from typing import ClassVar

from object_model import Id, PersistableModel


def test_id():
    class MyPersistable1(PersistableModel):
        id: ClassVar[Id] = Id("my_string", "my_int")

        my_string: str
        my_int: int

    assert MyPersistable1.id == (MyPersistable1, ("my_string", "my_int"))

    p = MyPersistable1(my_string="foo", my_int=1)
    assert p.id == (MyPersistable1, ("foo", 1))


def test_missing_id():
    try:
        class MyPersistable2(PersistableModel):
            my_string: str
            my_int: int

        _ = MyPersistable2
    except TypeError:
        assert True
    else:
        assert False


def test_override_id():
    try:
        class MyPersistable3(PersistableModel):
            id: ClassVar[Id] = Id("my_string", "my_int")

            my_string: str
            my_int: int

        class DerivedPersistable(MyPersistable3):
            id: ClassVar[Id] = Id("my_string", "my_int", "my_float")

            my_float: float

        _ = DerivedPersistable
    except TypeError:
        assert True
    else:
        assert False


def test_missing_field():
    try:
        class BrokenPersistable(PersistableModel):
            id: ClassVar[Id] = Id("my_string", "my_int")

            my_string: str

        _ = BrokenPersistable
    except TypeError:
        assert True
    else:
        assert False


def test_multiple_persistable_bases():
    try:
        class Base1(PersistableModel):
            id: ClassVar[Id] = Id("my_string")

            my_string: str

        class Base2(PersistableModel):
            id: ClassVar[Id] = Id("my_int")

            my_int: int

        class MyPersistable4(Base1, Base2):
            my_float: float

        _ = MyPersistable4
    except TypeError:
        assert True
    else:
        assert False
