from abc import abstractmethod
from inspect import currentframe, getframeinfo


class _CallStack:
    __instance = None

    def __new__(cls, *args, **kwargs):
        # Make this a singleton
        if cls.__instance is None:
            cls.__instance = super().__new__(cls, *args, **kwargs)
            cls.__instance.__stack = []

        return cls.__instance

    def __last_entry_matches(self, value, location) -> bool:
        if self.__stack:
            return self.__stack[-1][2] is value and self.__stack[-1][3] == location

        return False

    def push(self, parent, attr, child, location):
        if not self.__last_entry_matches(parent, location):
            self.__stack.clear()

        self.__stack.append((parent, attr, child, location))

    def copy(self, target, location, ret, copy_root):
        if copy_root and self.__last_entry_matches(target, location):
            while self.__stack:
                parent, attr, _, _ = self.__stack.pop()
                ret = parent._replace(**{attr: ret})

        self.__stack.clear()

        return ret


class ReplaceMixin:
    def __getattribute__(self, item):
        # TODO: This is probably a bad idea and may be retired

        ret = super().__getattribute__(item)

        if isinstance(ret, ReplaceMixin):
            caller = getframeinfo(currentframe().f_back)
            _CallStack().push(self, item, ret, (caller.positions.end_lineno, caller.filename))

        return ret

    def replace(self, /, copy_root: bool = True, **changes):
        caller = getframeinfo(currentframe().f_back)
        return _CallStack().copy(self,
                                 (caller.positions.end_lineno, caller.filename),
                                 self._replace(**changes),
                                 copy_root)

    @abstractmethod
    def _replace(self, /, **changes):
        ...
