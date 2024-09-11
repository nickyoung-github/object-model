from abc import abstractmethod


class _CallStack:
    __instance = None

    def __new__(cls, *args, **kwargs):
        # Make this a singleton
        if cls.__instance is None:
            cls.__instance = super().__new__(cls, *args, **kwargs)
            cls.__instance.__stack = []

        return cls.__instance

    def __bool__(self):
        return bool(self.__stack)

    def push(self, parent, attr, child):
        value = (parent, attr, child)
        stack = self.__stack
        if len(stack) == 0 or stack[-1] != value:
            self.__stack.append(value)

        arse = True

    def pop(self) -> tuple:
        return self.__stack.pop()

    def peek(self):
        return self.__stack[-1][2] if self.__stack else None

    def clear(self):
        self.__stack = []


class ReplaceMixin:
    def _post_getattribute(self, item, value):
        call_stack = _CallStack()

        if call_stack and call_stack.peek() is not self:
            call_stack.clear()

        call_stack.push(self, item, value)

    def replace(self, /, **changes):
        ret = self._replace(**changes)
        call_stack = _CallStack()

        if call_stack and call_stack.peek() is self:
            while call_stack:
                parent, attr, _ = call_stack.pop()
                ret = parent._replace(**{attr: ret})

        return ret

    @abstractmethod
    def _replace(self, /, **changes):
        ...
