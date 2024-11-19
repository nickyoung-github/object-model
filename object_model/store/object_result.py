from asyncio import Future

from object_model.store.persistable import PersistableMixin


class ObjectResult:
    def __init__(self):
        self.__future = Future()

    @property
    def value(self):
        return self.__future.result()

    @property
    async def value_a(self):
        return self.__future

    @property
    def done(self) -> bool:
        return self.__future.done()

    def add_done_callback(self, fn):
        self.__future.add_done_callback(fn)

    def set_result(self, result: PersistableMixin):
        self.__future.set_result(result)

    def set_exception(self, exception: Exception):
        self.__future.set_exception(exception)
