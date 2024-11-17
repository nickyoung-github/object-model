from abc import ABC, abstractmethod
from asyncio import Future
import datetime as dt
from os import geteuid
from platform import uname
from pwd import getpwuid

from .exception import DBNotFoundError
from .persistable import ImmutableMixin, ObjectRecord, PersistableMixin
from .._type_registry import is_temporary_type


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


class ObjectStore(ABC):
    def __init__(self, allow_temporary_types: bool):
        self.__allow_temporary_types = allow_temporary_types
        self.__entered = False
        self.__username = getpwuid(geteuid()).pw_name
        self.__hostname = uname().node
        self.__comment = ""
        self.__pending_reads: tuple[ObjectRecord, ...] = ()
        self.__pending_writes: tuple[ObjectRecord, ...] = ()
        self.__read_results: dict[tuple[str, bytes], ObjectResult] = {}
        self.__written_objects: dict[tuple[str, bytes], PersistableMixin] = {}
        self.__write_future: Future[bool] = Future()

    def __enter__(self, comment: str = ""):
        if self.__entered:
            raise RuntimeError("Fatal error: context re-entered")

        self.__comment = comment
        self.__entered = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__entered = False
        self.__execute()

    @abstractmethod
    def _execute_reads(self, reads: tuple[ObjectRecord, ...]) -> tuple[ObjectRecord, ...]:
        ...

    @abstractmethod
    def _execute_writes(self,
                        writes: tuple[ObjectRecord, ...],
                        username: str,
                        hostname: str,
                        comment: str) -> tuple[ObjectRecord, ...]:
        ...

    def read(self,
             typ: type[PersistableMixin],
             *args,
             effective_time: dt.datetime = dt.datetime.max,
             entry_time: dt.datetime = dt.datetime.max,
             **kwargs) -> ObjectResult:
        object_id_type, object_id = typ.make_id(*args, **kwargs)

        record = ObjectRecord(object_id_type=object_id_type,
                              object_id=object_id,
                              effective_time=effective_time,
                              entry_time=entry_time)

        self.__pending_reads += (record,)
        result = self.__read_results[(object_id_type, object_id)] = ObjectResult()

        if not self.__entered:
            self.__execute()

        return result

    def write(self, obj: PersistableMixin, as_of_effective_time: bool = False) -> Future[bool]:
        if not self.__allow_temporary_types and is_temporary_type(type(obj)):
            raise RuntimeError(f"Cannot persist temporary type {type(obj)}")

        if (as_of_effective_time or obj.entry_version > 1) and isinstance(obj, ImmutableMixin):
            raise RuntimeError(f"Cannot update immutable objects")

        record = ObjectRecord(object_type=obj.object_type,
                              object_id_type=obj.object_id_type,
                              object_id=obj.object_id,
                              object_contents=obj.object_contents,
                              effective_version=obj.effective_version if as_of_effective_time else obj.effective_version + 1,
                              entry_version=obj.entry_version + 1 if as_of_effective_time else 1,
                              effective_time=obj.effective_time if as_of_effective_time else dt.datetime.max,
                              entry_time=dt.datetime.max)

        self.__pending_writes += (record,)
        self.__written_objects[(obj.object_id_type, obj.object_id)] = obj

        ret = self.__write_future
        if not self.__entered:
            self.__execute()

        return ret

    def __execute(self):
        try:
            if self.__pending_writes:
                records = self._execute_writes(self.__pending_writes, self.__username, self.__hostname, self.__comment)

                for record in records:
                    obj = self.__written_objects.pop((record.object_id_type, record.object_id))
                    obj.set_db_info(record)

                if self.__written_objects:
                    raise RuntimeError("Failed to receive replies for all written objects")

            self.__write_future.set_result(True)
        except Exception as e:
            self.__write_future.set_exception(e)
        else:
            try:
                if self.__pending_reads:
                    records = self._execute_reads(self.__pending_reads)

                    for record in records:
                        result = self.__read_results.pop((record.object_id_type, record.object_id))
                        result.set_result(PersistableMixin.from_db_record(record))

                    while self.__read_results:
                        _, result = self.__read_results.popitem()
                        result.set_exception(DBNotFoundError())
            except Exception as e:
                while self.__read_results:
                    _, result = self.__read_results.popitem()
                    result.set_exception(e)
        finally:
            self.__write_future = Future()
            self.__pending_reads = ()
            self.__pending_writes = ()
            self.__written_objects.clear()
            self.__read_results.clear()
            self.__comment = ""
