from datetime import datetime

from psycopg.types.json import set_json_dumps, set_json_loads
from sqlalchemy import String, and_, create_engine, func, literal, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy_utc import utcnow
from sqlmodel import Field, SQLModel, Session
from tempfile import NamedTemporaryFile
from typing import Iterable
from uuid import UUID, uuid4

from .exception import FailedUpdateError, WrongStoreError
from .object_store import ObjectStore, ReadRequest, WriteRequest
from .object_record import ObjectRecord


class Id(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)


class Transactions(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    entry_time: datetime = Field(default=None, index=True, sa_column_kwargs={"server_default": utcnow()})
    username: str
    hostname: str
    comment: str


class SqlStore(ObjectStore):
    def __init__(self, connection_string: str, check_schema: bool, allow_temporary_types: bool, debug: bool = False):
        super().__init__(check_schema, allow_temporary_types)
        set_json_dumps(lambda x: x.decode("utf-8") if isinstance(x, bytes) else x)
        set_json_loads(lambda x: x)

        self.__existing_types: set[str] = set()
        self.__min_entry_time: datetime = datetime.min
        self.__id = None
        self.__engine = create_engine(connection_string, echo=debug)
        self.__is_partitioned = self.__engine.dialect.name == "postgresql"
        self.__create_schema()

    def _execute_reads(self, reads: ReadRequest) -> Iterable[ObjectRecord]:
        grouped_reads = {}

        for r in reads.reads:
            grouped_reads.setdefault((r.object_id_type, r.effective_time, r.entry_time), []).append(r.object_id)

        with Session(self.__engine) as s:
            while grouped_reads:
                (object_id_type, effective_time, entry_time), object_ids = grouped_reads.popitem()

                # Find the max version at or before the given times

                subquery = s.query(ObjectRecord.object_id_type,
                                   ObjectRecord.object_id,
                                   func.max(ObjectRecord.effective_version).label("effective_version"),
                                   func.max(ObjectRecord.entry_version).label("entry_version")).filter(
                    ObjectRecord.effective_time <= effective_time,
                    ObjectRecord.entry_time <= entry_time,
                    ObjectRecord.object_id_type == object_id_type,
                    ObjectRecord.object_id.cast(String).in_([literal(o.decode("utf-8")) for o in object_ids])).group_by(
                        ObjectRecord.object_id_type, ObjectRecord.object_id).subquery()

                # Load the contents corresponding to the max version

                query = s.query(ObjectRecord).join(subquery, and_(
                    ObjectRecord.object_id_type == subquery.c.object_id_type,
                    ObjectRecord.object_id == subquery.c.object_id,
                    ObjectRecord.effective_version == subquery.c.effective_version,
                    ObjectRecord.entry_version == subquery.c.entry_version
                ))

                return query.all()

    def _execute_writes(self, writes: WriteRequest) -> Iterable[ObjectRecord]:
        records = ()

        with Session(self.__engine) as s:
            transaction = Transactions(username=writes.username, hostname=writes.hostname, comment=writes.comment)
            s.add(transaction)
            s.commit()

            for record in writes.writes:
                if record.object_store_id and record.object_store_id != self.__id:
                    raise WrongStoreError(record.object_type, record.object_id)

                self.__add_type(record.object_id_type, s)

                record.transaction_id = transaction.id
                record.entry_time = transaction.entry_time
                record.effective_time = min(transaction.entry_time, record.effective_time)
                record.object_store_id = self.__id

                s.add(record)
                records += (record,)

            try:
                s.commit()
            except IntegrityError:
                raise FailedUpdateError()

            for record in records:
                s.refresh(record)

        return records

    def __add_type(self, object_id_type: str, session: Session):
        if self.__is_partitioned:
            if object_id_type not in SQLModel.metadata.tables:
                partition_stmt = text(fr"""
                    CREATE TABLE IF NOT EXISTS "{object_id_type}"
                    PARTITION OF objects
                    FOR VALUES IN ('{object_id_type}')
                """)

                session.execute(partition_stmt)
                session.commit()

                SQLModel.metadata.reflect(self.__engine, only=(object_id_type,))

    def __create_schema(self):
        SQLModel.metadata.create_all(self.__engine)

        with Session(self.__engine) as s:
            self.__min_entry_time = next(iter(s.exec(select(func.min(Transactions.entry_time))).first())) or\
                                    datetime.max

            if self.__min_entry_time == datetime.max:
                # It's a new DB, set the ID
                object_store_id = Id()
                s.add(object_store_id)
                s.commit()
                s.refresh(object_store_id)
                self.__id = object_store_id.id
            else:
                self.__id = next(iter(s.exec(select(Id.id)).first()))


class TempStore(SqlStore):
    def __init__(self, debug: bool = False):
        self.__file = NamedTemporaryFile()
        super().__init__(f"sqlite:///{self.__file.name}", False, True, debug=debug)


class MemoryStore(SqlStore):
    def __init__(self, debug: bool = False):
        super().__init__("sqlite+pysqlite:///:memory:", False, True, debug=debug)
