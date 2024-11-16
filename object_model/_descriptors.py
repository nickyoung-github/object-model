
class Id:
    def __init__(self, *args, typ=None):
        self.__fields = args
        self.__type = typ

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self.__type, self.__fields

        return self.__type, tuple(getattr(obj, f) for f in self.__fields)

    def __set_name__(self, owner, name):
        if not self.__type:
            self.__type = owner
