from abc import abstractmethod
from datetime import datetime

from . import DBContext, DBError
from .context import HEAD_VERSION
from .persistable import DBRecord


class SqlDBContext(DBContext):
    _EXCEPTION_TYPE = Exception
    _FIELDS = ("transaction_id", "effective_time", "entry_time", "effective_version", "entry_version",
               "object_type", "object_id", "contents")

    def __init__(self):
        super().__init__()
        self.__existing_types: set[str] = set()
        self.__min_entry_time: datetime = datetime.min

    @property
    @abstractmethod
    def _connection(self):
        ...

    @abstractmethod
    def _transaction(self):
        ...

    @abstractmethod
    def _normalise_exception(self, exception: Exception) -> DBError:
        ...

    @classmethod
    def _utc_timestamp_sql(cls) -> str:
        return f" (current_timestamp AT TIME ZONE 'UTC') NOT NULL"

    @classmethod
    def transaction_id_sql(cls) -> str:
        return "BIGSERIAL PRIMARY KEY"

    @classmethod
    def _next_transaction_sql(cls, username: str, hostname: str, comment: str | None) -> str:
        comment_str = "NULL" if comment is None else f"'{comment}'"
        return fr"""INSERT INTO transactions (username, hostname, comment) VALUES
                    ('{username}', '{hostname}', {comment_str})
                    RETURNING id, entry_time"""

    @classmethod
    def _transactions_sql(cls) -> str:
        return fr"""
            CREATE TABLE IF NOT EXISTS transactions (
                id          {cls.transaction_id_sql()},
                entry_time  timestamp DEFAULT{cls._utc_timestamp_sql()},
                username    varchar(16) NOT NULL,
                hostname    varchar(64) NOT NULL,
                comment     varchar(1024) NULL
            )
        """

    @classmethod
    def _add_type_sql(cls, typ: str) -> str | None:
        return None

    @classmethod
    def _get_types_sql(cls) -> str:
        return """SELECT DISTINCT object_type from objects"""

    @classmethod
    def _min_entry_time_sql(cls) -> str:
        return """SELECT MIN(entry_time) from transactions"""

    @classmethod
    def _objects_sql(cls) -> str:
        return fr"""
            CREATE TABLE IF NOT EXISTS objects (
                entry_time          timestamp  NOT NULL, 
                effective_time      timestamp  NOT NULL, 
                effective_version   bigint     NOT NULL,
                entry_version       bigint     NOT NULL,
                transaction_id      bigint     NOT NULL, 
                object_type         varchar    NOT NULL,
                object_id           jsonb      NOT NULL,
                contents            jsonb      NOT NULL,

                UNIQUE (object_type, object_id, effective_version, entry_version)
            )
    """

    @classmethod
    def _index_sql(cls) -> tuple[str, ...]:
        return (
            r"""CREATE UNIQUE INDEX IF NOT EXISTS id_time_idx
                ON objects (object_type, object_id, effective_time DESC, entry_time DESC)""",
        )

    def _execute_reads(self, reads: tuple[DBRecord, ...]) -> tuple[DBRecord, ...]:
        records = ()
        grouped_reads = {}

        for r in reads:
            grouped_reads.setdefault((r.object_type, r.effective_time, r.entry_time), []).append(r.object_id)

        try:
            cursor = self._connection.cursor()

            while grouped_reads:
                (object_type, effective_time, entry_time), ids = grouped_reads.popitem()
                statement = self.__select_statement(object_type, effective_time, entry_time, ids)
                db_records = cursor.execute(statement).fetchall()
                records += tuple(DBRecord(**dict(zip(self._FIELDS[1:], dbr))) for dbr in db_records)
        except self._EXCEPTION_TYPE as e:
            raise self._normalise_exception(e)

        return records

    def _execute_writes(self,
                        writes: tuple[DBRecord, ...],
                        username: str,
                        hostname: str,
                        comment: str) -> tuple[DBRecord, ...]:
        records = ()
        added_types = []
        grouped_writes = {}

        for w in writes:
            grouped_writes.setdefault((w.object_type, w.effective_time), []).append(w)

        try:
            cursor = self._connection.cursor()
            with self._transaction():
                statement = self._next_transaction_sql(username, hostname, comment)
                transaction_id, entry_time = cursor.execute(statement).fetchone()

                while grouped_writes:
                    (object_type, effective_time), write_records = grouped_writes.popitem()
                    if object_type not in self.__existing_types:
                        # cursor.execute(self._add_type_sql(object_type))
                        added_types.append(object_type)

                    effective_time = entry_time if effective_time == datetime.max else effective_time
                    statement = self.__insert_statement(transaction_id, entry_time, effective_time, write_records)
                    db_records = cursor.execute(statement).fetchall()
                    records += tuple(DBRecord(**dict(zip(self._FIELDS[1:], dbr + (bytes(),)))) for dbr in db_records)
        except self._EXCEPTION_TYPE as e:
            raise self._normalise_exception(e)

        self.__existing_types.update(added_types)

        return records

    def __select_statement(self,
                           object_type: str,
                           effective_time: datetime,
                           entry_time: datetime,
                           ids: list[bytes]) -> str:
        return fr"""
            SELECT {", ".join(self._FIELDS[1:])} FROM (
                SELECT *, rank() OVER (
                    PARTITION BY object_type, object_id
                    ORDER BY effective_time DESC, entry_time DESC
                ) AS rank
                FROM objects
                WHERE object_type = '{object_type}'
                AND object_id IN ('{"', '".join(i.decode('UTF-8') for i in ids)}')
                AND effective_time <= '{effective_time}'
                AND entry_time <= '{entry_time}'
            )
            WHERE rank = 1
        """

    def __insert_statement(self,
                           transaction_id: int,
                           entry_time: datetime,
                           effective_time: datetime,
                           records: list[DBRecord]) -> str:
        statement = f"""
            INSERT INTO objects ({", ".join(self._FIELDS)})
            VALUES
        """

        values_clause = []
        for record in records:
            if record.effective_version == HEAD_VERSION:
                effective_sql = f"(SELECT max(effective_version) + 1 FROM objects " +\
                                f"WHERE object_type = {record.object_type} AND object_id = {record.object_id})"
            else:
                effective_sql = f"{record.effective_version}"

            values = [str(transaction_id), f"'{effective_time}'", f"'{entry_time}'", effective_sql]
            for field in self._FIELDS[4:]:
                value = getattr(record, field)
                db_value = "NULL" if value is None else \
                    f"'{value}'" if isinstance(value, str) else \
                    f"'{value.decode("UTF-8")}'" if isinstance(value, bytes) else \
                    f"'{value.isoformat()}'" if isinstance(value, datetime) else\
                    str(value)

                values.append(db_value)

            values_clause.append(f"""({", ".join(values)})""")

        statement += ",\n".join(values_clause)
        statement += f"\n RETURNING {', '.join(self._FIELDS[1:7])}"

        return statement

    def _create_schema(self):
        cursor = self._connection.cursor()
        with self._transaction():
            cursor.execute(self._transactions_sql())
            cursor.execute(self._objects_sql())

            for statement in self._index_sql():
                cursor.execute(statement)

            type_results = cursor.execute(self._get_types_sql()).fetchall()
            self.__existing_types.update(r[0] for r in type_results)

            entry_time = cursor.execute(self._min_entry_time_sql()).fetchone()[0]
            if isinstance(entry_time, str):
                entry_time = datetime.fromisoformat(entry_time)

                self.__min_entry_time = entry_time
