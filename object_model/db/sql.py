from datetime import datetime
from functools import cached_property

from sqlalchemy import Index, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Column, Field, JSON, SQLModel, Session, create_engine, select

from uuid import UUID, uuid4

from object_model.db import DBContext
from object_model.db.context import HEAD_VERSION
from object_model.db.persistable import ObjectRecord


class TransactionRecord(SQLModel, table=True):
    transaction_id: int = Field(default=None, primary_key=True, index=True)
    entry_time: datetime = Field(sa_column_kwargs={"server_default": text("CURRENT_TIMESTAMP")})
    username: str
    hostname: str
    comment: str


class SqlDBContext(DBContext):
    def __init__(self, connection_string: str):
        super().__init__()
        self.__existing_types: set[str] = set()
        self.__min_entry_time: datetime = datetime.min
        self.__engine = create_engine(connection_string)

    def _execute_reads(self, reads: tuple[ObjectRecord, ...]) -> tuple[ObjectRecord, ...]:
        records = ()
        grouped_reads = {}
        record_type = self.__object_record

        for r in reads:
            grouped_reads.setdefault((r.object_id_type, r.effective_time, r.entry_time), []).append(r.object_id)

        with Session(self.__engine) as s:
            while grouped_reads:
                (object_id_type, effective_time, entry_time), object_ids = grouped_reads.popitem()

                valid_records = select(record_type).where(record_type.object_id_type == object_id_type).where(
                    record_type.effective_time <= effective_time).where(
                        record_type.entry_time <= entry_time).where(
                            record_type.object_id.in_(object_ids)).subquery("inner")

                query = s.query(
                    record_type,
                    func.rank().over(
                        partition_by=(record_type.object_id_type, record_type.object_id),
                        order_by=((record_type.effective_time.desc()), record_type.entry_time.desc())
                    ).label("rank")
                ).select_from(valid_records).subquery()

                query = s.query(record_type).select_from(query).filter(query.inner.rank == 1)

                for record in query:
                    records += (record,)

        return records

    def _execute_writes(self,
                        writes: tuple[ObjectRecord, ...],
                        username: str,
                        hostname: str,
                        comment: str) -> tuple[ObjectRecord, ...]:
        records = ()

        with Session(self.__engine) as s:
            transaction = TransactionRecord(username=username, hostname=hostname, comment=comment)
            s.add(transaction)

            s.commit()

            for w in writes:
                record = self.__object_record(**w.model_dump(),
                                              transaction_id=transaction.transaction_id,
                                              entry_time=transaction.entry_time,
                                              effective_time=min(transaction.entry_time, w.effective_time))
                s.add(record)
                records += (record,)

            s.commit()

        return records

    @cached_property
    def __object_record(self) -> type[ObjectRecord]:
        class DBObjectRecord(SQLModel, ObjectRecord, table=True):
            uuid: UUID = Field(default_factory=uuid4, primary_key=True)
            object_id: str = Field(sa_column=Column(JSON))
            object_contents: str = Field(sa_column=Column(JSON))
            transaction_id: int

            __table_args__ = (
                Index(
                    "idx_objects",
                    "object_type", "object_id", "effective_version", "entry_version",
                    unique=True
                ),
            )

        return DBObjectRecord

    def __create_schema(self):
        SQLModel.metadata.create_all(self.__engine)

        with Session(self.__engine) as s:
            statement = select(TransactionRecord).where(TransactionRecord.transaction_id == 0)
            for transaction in s.exec(statement):
                self.__min_entry_time = transaction.entry_time
