import importlib.metadata as md

TYPE_KEY = "t_"
CLASS_TYPE_KEY = "t__"


class __TypeRegistry:
    __instance = None

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls, *args, **kwargs)
            cls.__instance.__types = {}

        return cls.__instance

    def __getitem__(self, item) -> type:
        typ, _is_temporary = self.__types.get(item, (None, False))
        if not typ:
            entry_point = md.entry_points(group="object-store", name=item)
            if not entry_point:
                raise KeyError(f"{item} not registered")

            typ, _is_temporary = self.__types[item] = entry_point[0].load(), False

        return typ

    def is_temporary_type(self, type_name: str) -> bool:
        typ, is_temporary = self.__types.get(type_name, (None, False))
        if typ is None:
            raise RuntimeError(f"{type_name} not registered")

        return is_temporary

    def register_type(self, typ: type):
        type_name = getattr(typ, CLASS_TYPE_KEY, None)
        if type_name is None:
            raise RuntimeError(f"{typ} is missing attribute {TYPE_KEY}")

        if type_name in self.__types:
            raise RuntimeError(f"Attemped to register duplicate class {type_name}")

        self.__types[type_name] = typ, True


def get_type(type_name: str) -> type:
    return __TypeRegistry()[type_name]


def is_temporary_type(typ: str | type) -> bool:
    if isinstance(typ, type):
        type_name = getattr(typ, CLASS_TYPE_KEY, None)
        if type_name is None:
            raise RuntimeError(f"{typ} is missing attribute {TYPE_KEY}")
    else:
        type_name = typ

    return __TypeRegistry().is_temporary_type(type_name)


def register_type(typ: type):
    __TypeRegistry().register_type(typ)
