from psycopg import connect
from psycopg.types.json import set_json_loads

from . import DBError, DBUnknownError
from .sql import SqlDBContext


class PostgresContext(SqlDBContext):
    def __init__(self, params: str):
        super().__init__()
        set_json_loads(lambda j: j)
        self.__connection = connect(params)
        self._create_schema()

    @classmethod
    def _add_type_sql(cls, typ: str) -> str | None:
        return fr"""
            """

    def _normalise_exception(self, exception: Exception) -> DBError:
        return DBUnknownError(exception)

    @property
    def _connection(self):
        return self.__connection

    def _transaction(self):
        return self.__connection.transaction()
