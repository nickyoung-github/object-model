from sqlite3 import Error as SqliteError, IntegrityError, connect
from tempfile import NamedTemporaryFile

from . import DBError, DBDuplicateWriteError, DBFailedUpdateError, DBUnknownError
from .sql import SqlDBContext


class SqliteContext(SqlDBContext):
    _EXCEPTION_TYPE = SqliteError
    _DEFAULT = "NULL"

    def __init__(self, filename: str | None = None):
        super().__init__()
        if filename is None:
            self.__file = NamedTemporaryFile()
            filename = self.__file.name

        self.__connection = connect(filename)
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


