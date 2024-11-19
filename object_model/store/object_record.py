from datetime import datetime
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import BLOB, Column, Field, Index, JSON, PrimaryKeyConstraint, SQLModel

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
