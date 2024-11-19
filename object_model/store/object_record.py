from datetime import datetime
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import BLOB, Column, Field, Index, JSON, PrimaryKeyConstraint, SQLModel


# ToDo: This uses SQL-specific bits, which will be ignored for non-SQL implementations.
#       It's either that or duplication ...

HEAD_VERSION = -1
JSONVariant = JSON().with_variant(JSONB, "postgresql").with_variant(BLOB, "sqlite")


class ObjectRecord(SQLModel, table=True):
    object_id: bytes = Field(sa_column=Column(JSONVariant))
    object_contents: bytes = Field(sa_column=Column(JSONVariant), default=bytes())
    transaction_id: int
    object_id_type: str
    object_type: str = ""
    effective_time: datetime
    entry_time: datetime
    effective_version: int = HEAD_VERSION
    entry_version: int = HEAD_VERSION

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
