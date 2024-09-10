from psycopg import connect
from psycopg.types.json import set_json_loads

from object_model.db import DBError, DBUnknownError
from object_model.db.sql.sql_context import SqlDBContext


class PostgresContext(SqlDBContext):
    def __init__(self, params: str):
        super().__init__()
        set_json_loads(lambda j: j.decode("UTF-8"))
        self.__connection = connect(params)
        self._create_schema()

    @classmethod
    def _add_type_sql(cls, typ: str) -> str | None:
        return fr"""CREATE TABLE IF NOT EXISTS "{typ}"
                    PARTITION OF objects
                    FOR VALUES IN ('{typ}')"""

    @classmethod
    def _get_types_sql(cls) -> str:
        return r"""SELECT c.relname
                   FROM   pg_catalog.pg_inherits i, pg_class c
                   WHERE  c.oid = inhrelid::regclass
                   AND    i.inhparent = 'objects'::regclass"""

    def _normalise_exception(self, exception: Exception) -> DBError:
        return DBUnknownError(exception)

    @classmethod
    def _objects_sql(cls) -> str:
        return super()._objects_sql() + " PARTITION BY LIST (object_type)"

    @property
    def _connection(self):
        return self.__connection

    def _transaction(self):
        return self.__connection.transaction()
