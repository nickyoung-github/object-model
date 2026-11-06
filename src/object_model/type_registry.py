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
    root: Path
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
            self.__by_root: dict[Path, list[TypeInfo[T]]] = {}
            self.__initialised = True

    def __type_info(self, typ: type[T] | EntryPoint) -> TypeInfo[T]:
        try:
            if isinstance(typ, EntryPoint):
                return self.__by_type_name[(typ.__module__, typ.__qualname__)]
            else:
                return self.__by_type[typ]
        except KeyError:
            raise UnregisteredType(typ.value if isinstance(typ, EntryPoint) else typ)
        
    def registered_name(self, typ: type[T]) -> str:
        return self.__type_info(typ).name

    def root(self, typ: type[T]) -> Path:
        return self.__type_info(typ).root
    
    def get_type(self, name: str) -> type[T]:
        try:
            return self.__by_name[name].resolved_type
        except KeyError:
            raise UnregisteredType(name)

    def types_for_root(self, root: Path) -> tuple[type[T], ...]:
        try:
            return tuple(t.resolved_type for t in self.__by_root[root])
        except KeyError:
            raise UnregisteredType(root)
        
    def register(self, name: str, typ: type[T] | EntryPoint, root=Path()):
        type_info = self.__by_name.get(name)
        if type_info is not None:
            raise RegistrationError(name, type_info.resolved_type)

        type_info = self.__type_info(typ)
        if type_info is not None:
            raise RegistrationError(type_info.name, type_info.resolved_type)
        
        type_info = TypeInfo(name, root, typ)
        self.__by_name[name] = type_info
        self.__by_root.setdefault(root, []).append(type_info)
        
        if isinstance(typ, EntryPoint):
            module, qual_name = typ.value.split(":")
            self.__by_type_name[(module, qual_name)] = type_info
        else:
            self.__by_type[typ] = type_info
