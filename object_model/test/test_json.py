from datetime import date, datetime
from decimal import Decimal
from pydantic.alias_generators import to_camel, to_snake

from object_model.json import dump, dumps, load, loads


def test_nested_untyped_dict():
    data = {"date": date.today(),
            "time": datetime.now(),
            "int": 666,
            "float": 3.124,
            "decimal": Decimal(42),
            "list": [date(1970, 1, 1), 1.0, "foo", Decimal(1.23)],
            "tuple": (date(1971, 1, 1), 3.0, "foo", Decimal(1.25)),
            "dict": {"date": date(1984, 1, 1),
                     "list": [date(1971, 1, 1), 3.0, "foo", Decimal(1.25)],
                     "tuple": (date(1970, 1, 1), 1.0, "foo", Decimal(1.23))}}

    roundtrip_data = load(dump(data))
    assert data == roundtrip_data

    roundtrip_json_data = loads(dumps(data))

    # tuple will be returned as a list and there's nothing we can do about that ...
    roundtrip_json_data["tuple"] = tuple(roundtrip_data["tuple"])
    roundtrip_json_data["dict"]["tuple"] = tuple(roundtrip_json_data["dict"]["tuple"])
    assert data == roundtrip_json_data


def test_alias_generator():
    data = {"my_int": 123, "str": "foo"}

    dumped = dump(data, alias_generator=to_camel)
    assert "myInt" in dumped
    assert "my_int" not in dumped

    loaded = load(dumped, alias_generator=to_snake)
    assert data == loaded

