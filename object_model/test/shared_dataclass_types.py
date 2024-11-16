from dataclasses import dataclass
from datetime import date

from object_model import NamedPersistable, Subclass


@dataclass(frozen=True)
class ContainerDC(NamedPersistable):
    contents: dict[str, date | str | float | int]


@dataclass(frozen=True)
class Container2DC(ContainerDC):
    rank: int


@dataclass(frozen=True)
class NestedDC(NamedPersistable):
    container: Subclass[ContainerDC]


@dataclass(frozen=True)
class OuterDC(NamedPersistable):
    the_nested: NestedDC
    date: date
    the_list: list[date | str | float | int]
    tuple: tuple[date | str | float | int, ...]


@dataclass(frozen=True)
class Container3DC(Container2DC):
    date: date
