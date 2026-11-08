from collections.abc import Callable
from functools import lru_cache
from typing import ParamSpec, TypeVar, cast

P = ParamSpec("P")
R = TypeVar("R")

def typed_cache[**P, R](func: Callable[P, R], maxsize: int | None = None) -> Callable[P, R]:
    return cast(Callable[P, R], lru_cache(maxsize=maxsize)(func))
