from asyncio import Future
from datetime import datetime
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import BLOB, Column, Field, Index, JSON, PrimaryKeyConstraint, SQLModel

from object_model.store.persistable import PersistableMixin

# ToDo: This uses SQL-specific bits, which will be ignored for non-SQL implementations.
#       It's either that or duplication ...

JSONVariant = JSON().with_variant(JSONB, "postgresql").with_variant(BLOB, "sqlite")


class ObjectRecord(SQLModel, table=True):
    object_id: bytes = Field(sa_column=Column(JSONVariant))
    object_contents: bytes = Field(sa_column=Column(JSONVariant), default=bytes())
    transaction_id: int = -1
    object_id_type: str
    object_type: str = ""
    effective_time: datetime = datetime.max
    entry_time: datetime = datetime.max
    effective_version: int = -1
    entry_version: int = -1

    __tablename__ = "objects"

    __table_args__ = (
        PrimaryKeyConstraint(
            "object_id_type", "object_id", "effective_version", "entry_version",
        ),
        Index(
            "idx_objects_by_time",
            "effective_time", "entry_time", "object_type", "object_id", "effective_version", "entry_version"
        ),
        {
            "postgresql_partition_by": "LIST(object_id_type)"
        }
    )


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
