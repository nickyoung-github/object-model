from psycopg import connect
from psycopg.adapt import Loader
from psycopg.types.json import set_json_loads

from . import DBError, DBUnknownError
from .sql import SqlDBContext


class PassthroughLoader(Loader):
    def load(self, obj):
        return obj.tobytes().decode("UTF-8")


class PostgresContext(SqlDBContext):

    def __init__(self, params: str):
        super().__init__()
        set_json_loads(lambda j: j.decode("UTF-8"))
        self.__connection = connect(params)
        self.__connection.adapters.register_loader("time", PassthroughLoader)
        self.__connection.adapters.register_loader("timestamp", PassthroughLoader)
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
