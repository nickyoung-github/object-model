from dataclasses import dataclass
from pathlib import Path
from importlib.metadata import EntryPoint
from typing import Self, cast


class RegistrationError(Exception):
    def __init__(self, name: str, typ: type):
        super().__init__(f"{typ!r} already registered as {name}")


class UnregisteredType(Exception):
    def __init__(self, item: str | type | Path):
        super().__init__(f"Unregistered type {item!r}")


@dataclass(frozen=True)
class TypeInfo[T]:
    name: str
    path: Path
    typ: type[T] | EntryPoint
    
    @property
    def resolved_type(self) -> type[T]:
        return cast(type[T], self.typ.load()) if isinstance(self.typ, EntryPoint) else self.typ


class TypeRegistry[T]:
    __instance: Self | None = None
    __initialised: bool = False

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)

        return cls.__instance

    def __init__(self):
        if not self.__initialised:
            self.__by_name: dict[str, TypeInfo[T]] = {}
            self.__by_type: dict[type, TypeInfo[T]] = {}
            self.__by_type_name: dict[tuple[str, str], TypeInfo[T]] = {}
            self.__by_path: dict[Path, list[TypeInfo[T]]] = {}
            self.__initialised = True

    def registered_name(self, typ: type[T]) -> str:
        try:
            try:
                return self.__by_type[typ].name
            except KeyError:
                return self.__by_type_name[(typ.__module__, typ.__qualname__)].name
        except KeyError:
            raise UnregisteredType(typ)

    def registered_path(self, typ: type[T]) -> Path:
        return self.__by_type[typ].path

    def get_type(self, name: str) -> type[T]:
        try:
            return self.__by_name[name].resolved_type
        except KeyError:
            raise UnregisteredType(name)

    def types_for_path(self, path: Path) -> tuple[type[T], ...]:
        try:
            return tuple(t.resolved_type for t in self.__by_path[path])
        except KeyError:
            raise UnregisteredType(path)
        
    def register(self, name: str, typ: type[T] | EntryPoint, path=Path()):
        type_info = self.__by_name.get(name)
        if type_info is not None:
            raise RegistrationError(name, type_info.resolved_type)

        new_type_info = TypeInfo(name, path, typ)

        if isinstance(typ, EntryPoint):
            module, qual_name = typ.value.split(":")
            type_info = self.__by_type_name.get((module, qual_name))
            if type_info is not None:
                raise RegistrationError(type_info.name, type_info.resolved_type)
            self.__by_type_name[(module, qual_name)] = new_type_info
        else:
            type_info = self.__by_type.get(typ)
            if type_info is not None:
                raise RegistrationError(type_info.name, type_info.resolved_type)
            self.__by_type[typ] = new_type_info

        self.__by_name[name] = new_type_info
        self.__by_path.setdefault(path, []).append(new_type_info)
