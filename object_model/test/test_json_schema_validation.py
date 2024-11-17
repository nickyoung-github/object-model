from datetime import date
from jsonschema import validate
from orjson import loads

from object_model._json import dumps, schema
from .shared_pydantic_types import Container2, Nested, Outer


def test_json_schema_validation():
    c = Container2(name="container", contents={"foo": 1}, rank=1)
    o = Outer(name="outer", the_nested=Nested(name="nested", container=c), date=date(1970, 1, 1))

    validate(schema=schema(Outer), instance=loads(dumps(o)))
