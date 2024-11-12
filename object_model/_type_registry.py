import importlib.metadata as md

TYPE_KEY = "t_"


class __TypeRegistry:
    __instance = None

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls, *args, **kwargs)
            cls.__instance.__types = {}

        return cls.__instance

    def __getitem__(self, item) -> type:
        typ = self.__types.get(item)
        if not typ:
            entry_point = md.entry_points(group="object-store", name=item)
            if entry_point is None:
                raise RuntimeError(f"{item} not registered")

            typ = self.__types[item] = entry_point[0].load()

        return typ

    def register_temporary_type(self, typ: type):
        type_name = getattr(typ, TYPE_KEY)
        if type_name is None:
            raise RuntimeError(f"{typ} is missing attribute {TYPE_KEY}")

        self.__types[type_name] = typ


def get_type(type_name: str) -> type:
    return __TypeRegistry()[type_name]


def register_temporary_type(typ: type):
    __TypeRegistry().register_temporary_type(typ)
