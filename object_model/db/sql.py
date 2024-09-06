from abc import abstractmethod
from concurrent.futures import Future
import datetime as dt

from . import DBContext, DBError, DBNotFoundError, DBResult
from .persistable import DBRecord, PersistableMixin


class SqlDBContext(DBContext):
    _EXCEPTION_TYPE = Exception
    _DEFAULT = "'DEFAULT'"
    _FIELDS = ("transaction_id", "effective_time", "entry_time", "object_type", "object_id", "effective_version",
               "entry_version", "contents")

    def __init__(self):
        super().__init__()
        self.__pending_writes: dict[tuple[str, dt.datetime | None], list[DBRecord]] = {}
        self.__pending_reads: dict[tuple[str, dt.datetime, dt.datetime], dict[bytes, Future]] = {}
        self.__pending_types: set[str] = set()
        self.__existing_types: set[str] = set()
        self.__min_entry_time: dt.datetime = dt.datetime.min
        self.__write_future: Future = Future()

    def __enter__(self):
        super().__enter__()
        if self.__pending_writes or self.__pending_reads:
            raise RuntimeError("Uncommitted records exist from previous transaction")

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

    def _execute(self, username: str, hostname: str, comment: str | None):
        cursor = self._connection.cursor()
        with self._transaction():
            if self.__pending_writes:
                try:
                    while self.__pending_types:
                        typ = self.__pending_types.pop()
                        self.__existing_types.add(typ)
                        type_statement = self._add_type_sql(typ)

                        if type_statement:
                            cursor.execute(type_statement)

                    trans_id, entry_time =\
                        cursor.execute(self._next_transaction_sql(username, hostname, comment)).fetchone()
                    for statement in self.__insert_statements(trans_id, entry_time):
                        cursor.execute(statement)
                except self._EXCEPTION_TYPE as e:
                    self.__write_future.set_exception(self._normalise_exception(e))
                else:
                    self.__write_future.set_result(True)

                self.__write_future = Future()
            elif self.__pending_reads:
                for statement, group, ids_futures in self.__select_statements():
                    try:
                        records = cursor.execute(statement).fetchall()
                        for db_record in records:
                            dict_record = dict(zip(self._FIELDS, db_record))
                            dict_record["effective_time"] = dt.datetime.fromisoformat(dict_record["effective_time"])
                            dict_record["entry_time"] = dt.datetime.fromisoformat(dict_record["entry_time"])
                            dict_record["object_id"] = dict_record["object_id"].encode("UTF-8")
                            dict_record["contents"] = dict_record["contents"].encode("UTF-8")
                            dict_record.pop("transaction_id")

                            record = DBRecord(**dict_record)

                            if self.__min_entry_time is None:
                                self.__min_entry_time = record.entry_time

                            future = ids_futures.pop(record.object_id)
                            future.set_result(record)
                    except self._EXCEPTION_TYPE as e:
                        while ids_futures:
                            _, future = ids_futures.popitem()
                            future.set_exception(e)

                    if not ids_futures:
                        self.__pending_reads.pop(group)

                while self.__pending_reads:
                    _, ids_futures = self.__pending_reads.popitem()
                    for id_, future in ids_futures.items():
                        future.set_exception(DBNotFoundError())

    def __insert_statements(self, transaction_id: int, entry_time: str) -> list[str]:
        ret = []
        while self.__pending_writes:
            (typ, effective_time), records = self.__pending_writes.popitem()
            effective_time = entry_time if effective_time == dt.datetime.max else\
                effective_time.strftime("%Y-%m-%d %H:%M:%S.%f")

            statement = f"""
                INSERT INTO objects ({", ".join(self._FIELDS)})
                VALUES
            """

            values_clause = []
            for record in records:
                values = [str(transaction_id), f"'{effective_time}'", f"'{entry_time}'"]
                for field in self._FIELDS[3:]:
                    value = getattr(record, field)
                    db_value = "NULL" if value is None else \
                        f"'{value.decode("UTF-8")}'" if isinstance(value, bytes) else\
                        f"'{value}'" if isinstance(value, str) else\
                        f"'{str(value)}'" if isinstance(value, dt.datetime) else\
                        str(value)

                    values.append(db_value)

                values_clause.append(f"""({", ".join(values)})""")

            statement += ",\n".join(values_clause)
            ret.append(statement)

        return ret

    def __select_statements(self) ->\
            list[tuple[str, tuple[str, dt.datetime, dt.datetime], dict[bytes, Future[DBRecord]]]]:
        ret = []
        for group, ids_futures in self.__pending_reads.items():
            object_type, effective_time, entry_time = group
            statement = fr"""
                SELECT {", ".join(self._FIELDS)} FROM (
                    SELECT *, rank() OVER (
                        PARTITION BY object_type, object_id
                        ORDER BY effective_time DESC, entry_time DESC
                    ) AS rank
                    FROM objects
                    WHERE object_type = '{object_type}'
                    AND object_id IN ('{"', '".join(i.decode('UTF-8') for i in ids_futures.keys())}')
                    AND effective_time <= '{effective_time}'
                    AND entry_time <= '{entry_time}'
                )
                WHERE rank = 1
            """
            ret.append((statement, group, ids_futures))

        return ret

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
                entry_time = dt.datetime.fromisoformat(entry_time)

                self.__min_entry_time = entry_time

    def read(self,
             typ: type[PersistableMixin],
             *args,
             effective_time: dt.datetime = dt.datetime.max,
             entry_time: dt.datetime = dt.datetime.max,
             **kwargs) -> DBResult:
        if self.__pending_writes:
            raise RuntimeError("Cannot read and write in the same transaction")

        object_type, object_id = typ.make_id(*args, **kwargs)
        future = Future[DBRecord]()

        if object_type not in self.__existing_types:
            future.set_exception(DBNotFoundError())
        else:
            self.__pending_reads.setdefault((object_type, effective_time, entry_time), {})[object_id] = future
            self._execute_if_ready()

        return DBResult(typ, future)

    def _write(self, record: DBRecord) -> Future[bool]:
        if self.__pending_reads:
            raise RuntimeError("Cannot read and write in the same transaction")

        if record.object_type not in self.__existing_types:
            self.__pending_types.add(record.object_type)

        self.__pending_writes.setdefault((record.object_type, record.effective_time), []).append(record)

        ret = self.__write_future
        self._execute_if_ready()

        return ret
