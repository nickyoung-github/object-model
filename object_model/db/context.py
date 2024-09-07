from abc import ABC, abstractmethod
from concurrent.futures import Future
import datetime as dt
from functools import cached_property
from os import geteuid
from platform import uname
from pwd import getpwuid

from .persistable import DBRecord, ImmutableMixin, PersistableMixin
from ..json import dumps


class DBResult:
    def __init__(self, typ: type[PersistableMixin], future: Future[DBRecord]):
        self.__type = typ
        self.__future = future

    @cached_property
    def value(self):
        return self.__type.from_db_record(self.__future.result())

    @property
    def done(self) -> bool:
        return self.__future.done()

    def add_done_callback(self, fn):
        self.__future.add_done_callback(fn)


class DBContext(ABC):
    def __init__(self):
        self.__entered = False
        self.__username = getpwuid(geteuid()).pw_name
        self.__hostname = uname().node
        self.__comment = None

    def __enter__(self):
        if self.__entered:
            raise RuntimeError("Fatal error: context re-entered")

        self.__entered = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__entered = False
        self._execute(self.__username, self.__hostname, self.__comment)

    @abstractmethod
    def read(self,
             typ: type[PersistableMixin],
             *args,
             effective_time: dt.datetime = dt.datetime.max,
             entry_time: dt.datetime = dt.datetime.max,
             **kwargs) -> DBResult:
        ...

    def write(self, obj: PersistableMixin, as_of_effective_time: bool = False):
        if (as_of_effective_time or obj.entry_version > 1) and isinstance(obj, ImmutableMixin):
            raise RuntimeError(f"Cannot update immutable objects")

        record = DBRecord(obj.object_type,
                          obj.object_id,
                          dumps(obj),
                          obj.effective_version if as_of_effective_time else obj.effective_version + 1,
                          obj.entry_version + 1 if as_of_effective_time else 1,
                          obj.effective_time if as_of_effective_time else dt.datetime.max,
                          dt.datetime.max)
        return self._write(record)

    @abstractmethod
    def _write(self, record: DBRecord) -> Future[bool]:
        ...

    def _execute_if_ready(self):
        if not self.__entered:
            return self._execute(self.__username, self.__hostname, self.__comment)

    @abstractmethod
    def _execute(self, username: str, hostname: str, comment: str):
        ...
