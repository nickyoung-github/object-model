from abc import ABC, abstractmethod
from asyncio import Future
import datetime as dt
from jsonschema import validate
from orjson import loads
from platform import system, uname
from pydantic import BaseModel
from pwd import getpwuid
from typing import Iterable

from . import ObjectResult
from .exception import NotFoundError
from .persistable import ImmutableMixin, ObjectRecord, PersistableMixin
from .._json import schema
from .._type_registry import CLASS_TYPE_KEY, is_temporary_type


def _get_user_name():
    if system() == "Windows":
        import win32api
        return win32api.GetUserName()
    else:
        from os import geteuid
        return getpwuid(geteuid()).pw_name


class ReadRequest(BaseModel):
    reads: tuple[ObjectRecord, ...]


class WriteRequest(BaseModel):
    writes: tuple[ObjectRecord, ...]
    username: str
    hostname: str
    comment: str


class RegisterSchemaRequest(BaseModel):
    name: str
    json_schema: dict


class ObjectStore(ABC):
    def __init__(self, check_schema: bool, allow_temporary_types: bool):
        self.__allow_temporary_types = allow_temporary_types
        self.__check_schema: bool = check_schema
        self.__json_schema = {}
        self.__entered = False
        self.__username = _get_user_name()
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
    def _execute_reads(self, reads: ReadRequest) -> Iterable[ObjectRecord]:
        ...

    @abstractmethod
    def _execute_writes(self, writes: WriteRequest) -> Iterable[ObjectRecord]:
        ...

    @property
    def allow_temporary_types(self) -> bool:
        return self.__allow_temporary_types

    @property
    def check_schema(self) -> bool:
        return self.__check_schema

    def _execute_writes_with_check(self, writes: WriteRequest) -> Iterable[ObjectRecord]:
        if self.__check_schema:
            writes_by_type = {}

            for write in writes.writes:
                writes_by_type.setdefault(write.object_id_type, []).append(loads(write.object_contents))

            for typ, instances in writes_by_type.items():
                if not self.__allow_temporary_types and is_temporary_type(typ):
                    raise RuntimeError(f"Cannot persist temporary type {typ}")

                if typ not in self.__json_schema["$defs"]:
                    raise RuntimeError(f"Attempt to write unkknown type {typ}")

                for instance in instances:
                    validate(schema=self.__json_schema["$defs"][typ], instance=instance)

        return self._execute_writes(writes)

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
        if (as_of_effective_time or obj.entry_version > 1) and isinstance(obj, ImmutableMixin):
            raise RuntimeError(f"Cannot update immutable objects")

        record = ObjectRecord(object_type=obj.object_type,
                              object_id_type=obj.object_id_type,
                              object_id=obj.object_id,
                              object_contents=obj.object_contents,
                              effective_version=obj.effective_version + (0 if as_of_effective_time else 1),
                              entry_version=obj.entry_version + 1 if as_of_effective_time else 1,
                              effective_time=obj.effective_time if as_of_effective_time else dt.datetime.max,
                              object_store_id=obj.object_store_id)

        self.__pending_writes += (record,)
        self.__written_objects[(obj.object_id_type, obj.object_id)] = obj

        ret = self.__write_future
        if not self.__entered:
            self.__execute()

        return ret

    def register_type(self, typ: type[PersistableMixin]):
        self.register_schema(RegisterSchemaRequest(name=getattr(typ, CLASS_TYPE_KEY), json_schema=schema(typ)))

    def register_schema(self, request: RegisterSchemaRequest):
        defs = request.json_schema.pop("$defs", {})
        defs[request.name] = request.json_schema
        self.__json_schema.update(defs)

    def __execute(self):
        try:
            if self.__pending_writes:
                records = self._execute_writes(WriteRequest(writes=self.__pending_writes,
                                                            username=self.__username,
                                                            hostname=self.__hostname,
                                                            comment=self.__comment))

                for record in records:
                    obj = self.__written_objects.pop((record.object_id_type, record.object_id))
                    obj.init_from_record(record)

                if self.__written_objects:
                    raise RuntimeError("Failed to receive replies for all written objects")

            self.__write_future.set_result(True)
        except Exception as e:
            self.__write_future.set_exception(e)
        else:
            try:
                if self.__pending_reads:
                    records = self._execute_reads(ReadRequest(reads=self.__pending_reads))

                    for record in records:
                        result = self.__read_results.pop((record.object_id_type, record.object_id))
                        result.set_result(PersistableMixin.from_object_record(record))

                    while self.__read_results:
                        _, result = self.__read_results.popitem()
                        result.set_exception(NotFoundError())
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
