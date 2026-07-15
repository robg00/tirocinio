import psycopg2
from psycopg2.extras import execute_values
from pyflink.datastream.functions import MapFunction


PG_CONN = {
    "host": "postgres",
    "port": 5432,
    "dbname": "streammark",
    "user": "streammark",
    "password": "streammark",
}


class PostgresWriter(MapFunction):
    def __init__(self, sql, row_func, conn_info=None):
        self._sql = sql
        self._row_func = row_func
        self._conn_info = conn_info or PG_CONN
        self._conn = None

    def open(self, runtime_context):
        self._conn = psycopg2.connect(**self._conn_info)

    def map(self, value):
        row = self._row_func(value)
        with self._conn.cursor() as cur:
            execute_values(cur, self._sql, [row])
        self._conn.commit()
        return value

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
