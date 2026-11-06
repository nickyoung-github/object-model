from functools import lru_cache
from typing import Callable, ParamSpec, TypeVar, cast

P = ParamSpec("P")
R = TypeVar("R")

def typed_cache(func: Callable[P, R], maxsize: int | None = None) -> Callable[P, R]:
    return cast(Callable[P, R], lru_cache(maxsize=maxsize)(func))
