from object_model import BaseModel


class Inner(BaseModel):
    my_int: int
    my_string: str


class Middle2(BaseModel):
    inner: Inner
    name: str = "middle2"


class Middle1(BaseModel):
    middle: Middle2
    name: str = "middle1"


class Outer(BaseModel):
    middle: Middle1
    name: str = "outer"


def test_replace():
    outer = Outer(middle=Middle1(middle=Middle2(inner=Inner(my_int=123, my_string="123"))))

    outer_new = outer\
        .middle.middle.inner.replace(my_int=321, my_string="321")
    assert isinstance(outer_new, Outer)
    assert isinstance(outer.
                      middle.middle.inner.replace(my_int=321, my_string="321"), Outer)

    assert isinstance(outer.middle.middle.inner.replace(my_int=321, my_string="321", copy_root=False), Inner)

    m1 = outer.middle
    m1_new = m1.middle.inner.replace(my_int=1)

    assert isinstance(m1_new, Middle1)

    m2 = outer.middle.middle
    m2_new = m2.inner.replace(my_int=2)
    assert isinstance(m2_new, Middle2)

    i = outer.middle.middle.inner
    i_new = i.replace(my_int=3)
    assert isinstance(i_new, Inner)
