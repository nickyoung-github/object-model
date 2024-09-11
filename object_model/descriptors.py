from __future__ import annotations

from typing import Any


class Id:
    def __init__(self, *args, typ=None):
        self.__fields = args
        self.__type = typ

    def __get__(self, obj, objtype=None) -> tuple[type, tuple[Any, ...]]:
        if obj is None:
            return self.__type, self.__fields

        return self.__type, tuple(getattr(obj, f) for f in self.__fields)

    def __set_name__(self, owner, name):
        if not self.__type:
            self.__type = owner


class Value:
    def __init__(self, value):
        self.__value = value

    def __get__(self, instance, owner):
        if instance is not None:
            return instance.__value or self.__value

    def __set__(self, instance, value):
        instance.__value = value
        return instance
