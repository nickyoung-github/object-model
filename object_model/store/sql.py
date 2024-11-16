from datetime import datetime

from sqlalchemy import Index, and_, create_engine, func, select, text
from sqlmodel import Field, SQLModel, Session
from tempfile import NamedTemporaryFile

from .store import ObjectStore
from .object_record import ObjectRecord


class Transactions(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True, index=True)
    entry_time: datetime = Field(sa_column_kwargs={"server_default": text("CURRENT_TIMESTAMP")})
    username: str
    hostname: str
    comment: str

    __table_args__ = (
        Index(
            "idx_transactions",
            "entry_time", "id",
            unique=True
        ),
    )


class SqlStore(ObjectStore):
    def __init__(self, connection_string: str, allow_temporary_types: bool, debug: bool = False):
        super().__init__(allow_temporary_types)
        self.__existing_types: set[str] = set()
        self.__min_entry_time: datetime = datetime.min
        self.__engine = create_engine(connection_string, echo=debug)
        self.__create_schema()

    def _execute_reads(self, reads: tuple[ObjectRecord, ...]) -> tuple[ObjectRecord, ...]:
        records = ()
        grouped_reads = {}

        for r in reads:
            grouped_reads.setdefault((r.object_id_type, r.effective_time, r.entry_time), []).append(r.object_id)

        with Session(self.__engine) as s:
            while grouped_reads:
                (object_id_type, effective_time, entry_time), object_ids = grouped_reads.popitem()

                subquery = s.query(ObjectRecord.object_id_type,
                                   ObjectRecord.object_id,
                                   func.max(ObjectRecord.effective_version).label("effective_version"),
                                   func.max(ObjectRecord.entry_version).label("entry_version")).filter(
                    ObjectRecord.effective_time <= effective_time,
                    ObjectRecord.entry_time <= entry_time,
                    ObjectRecord.object_id_type == object_id_type,
                    ObjectRecord.object_id.in_(object_ids)).group_by(
                        ObjectRecord.object_id_type, ObjectRecord.object_id).subquery()

                query = s.query(ObjectRecord).join(subquery, and_(
                    ObjectRecord.object_id_type == subquery.c.object_id_type,
                    ObjectRecord.object_id == subquery.c.object_id,
                    ObjectRecord.effective_version == subquery.c.effective_version,
                    ObjectRecord.entry_version == subquery.c.entry_version
                ))

                for record in query.all():
                    records += (record,)

        return records

    def _execute_writes(self,
                        writes: tuple[ObjectRecord, ...],
                        username: str,
                        hostname: str,
                        comment: str) -> tuple[ObjectRecord, ...]:
        records = ()

        with Session(self.__engine) as s:
            transaction = Transactions(username=username, hostname=hostname, comment=comment)
            s.add(transaction)
            s.commit()

            for record in writes:
                record.transaction_id = transaction.id
                record.entry_time = transaction.entry_time
                record.effective_time = min(transaction.entry_time, record.effective_time)

                s.add(record)
                records += (record,)

            s.commit()

            for record in records:
                s.refresh(record)

        return records

    def __create_schema(self):
        SQLModel.metadata.create_all(self.__engine)

        with Session(self.__engine) as s:
            statement = select(func.min(Transactions.entry_time))
            for entry_time in s.exec(statement):
                self.__min_entry_time = entry_time


class TempStore(SqlStore):

    def __init__(self):
        self.__file = NamedTemporaryFile()
        super().__init__(f"sqlite:///{self.__file.name}", True)


class MemoryStore(SqlStore):
    def __init__(self):
        super().__init__("sqlite+pysqlite:///:memory:", True)
