from datetime import date, datetime
from sqlite3 import Error as SqliteError, IntegrityError, PARSE_DECLTYPES, connect, register_converter
from tempfile import NamedTemporaryFile

from object_model.db import DBError, DBDuplicateWriteError, DBFailedUpdateError, DBUnknownError
from object_model.db.sql.sql_context import SqlDBContext


class SqliteContext(SqlDBContext):
    _EXCEPTION_TYPE = SqliteError

    def __init__(self, filename: str | None = None):
        super().__init__()
        if filename is None:
            self.__file = NamedTemporaryFile()
            filename = self.__file.name

        register_converter("date", lambda x: date.fromisoformat(x.decode("UTF-8")))
        register_converter("datetime", lambda x: datetime.fromisoformat(x.decode("UTF-8")))
        register_converter("timestamp", lambda x: datetime.fromisoformat(x.decode("UTF-8")))
        register_converter("jsonb", lambda x: x.decode("UTF-8"))

        self.__connection = connect(filename, detect_types=PARSE_DECLTYPES)
        self._create_schema()

    @classmethod
    def _utc_timestamp_sql(cls) -> str:
        return "(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')) NOT NULL"

    @classmethod
    def transaction_id_sql(cls) -> str:
        return "INTEGER PRIMARY KEY AUTOINCREMENT"

    @property
    def _connection(self):
        return self.__connection

    def _transaction(self):
        return self.__connection

    def _normalise_exception(self, exception: Exception) -> DBError:
        if isinstance(exception, IntegrityError):
            if exception.sqlite_errorname == "SQLITE_CONSTRAINT_UNIQUE":
                if "transaction_id" in exception.args[0]:
                    return DBDuplicateWriteError()
                elif "version" in exception.args[0] or "time" in exception.args[0]:
                    return DBFailedUpdateError()

        return DBUnknownError(exception)


