from abc import abstractmethod
from sys import getrefcount
from weakref import ref


class _CallStack:
    __instance = None

    def __new__(cls, *args, **kwargs):
        # Make this a singleton
        if cls.__instance is None:
            cls.__instance = super().__new__(cls, *args, **kwargs)
            cls.__instance.__stack = []

        return cls.__instance

    def __last_entry_matches(self, value, refcount) -> bool:
        if self.__stack:
            return self.__stack[-1][2]() is value and self.__stack[-1][3] == refcount

        return False

    def push(self, parent, attr, child, parent_refcount, child_refcount):
        if not self.__last_entry_matches(parent, parent_refcount):
            self.__stack.clear()

        value = (ref(parent), attr, ref(child), child_refcount)
        if (not self.__stack) or self.__stack[-1] != value:
            # The debugger can cause some crazy stuff to happen
            self.__stack.append(value)

    def copy_root(self, changed_object, refcount, **changes):
        ret = changed_object._replace(**changes)

        if self.__last_entry_matches(changed_object, refcount):
            while self.__stack:
                parent, attr, _, _ = self.__stack.pop()
                ret = parent()._replace(**{attr: ret})

        return ret


class ReplaceMixin:
    def _post_getattribute(self, attr, value, parent_refcount, child_refcount):
        _CallStack().push(self, attr, value, parent_refcount, child_refcount)

    def replace(self, /, **changes):
        return _CallStack().copy_root(self, getrefcount(self) - 2, **changes)

    @abstractmethod
    def _replace(self, /, **changes):
        ...
