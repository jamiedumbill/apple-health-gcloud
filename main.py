from os import getenv

from psycopg2 import OperationalError
from psycopg2.pool import SimpleConnectionPool

from apple_health import *

# TODO(developer): specify SQL connection details
CONNECTION_NAME = getenv('INSTANCE_CONNECTION_NAME')
DB_USER = getenv('POSTGRES_USER')
DB_PASSWORD = getenv('POSTGRES_PASSWORD')
DB_NAME = getenv('POSTGRES_DATABASE')

pg_config = {
  'user': DB_USER,
  'password': DB_PASSWORD,
  'dbname': DB_NAME
}

# Connection pools reuse connections between invocations,
# and handle dropped or expired connections automatically.
pg_pool = None


def __connect(host):
    """
    Helper function to connect to Postgres
    """
    global pg_pool
    pg_config['host'] = host
    pg_pool = SimpleConnectionPool(1, 1, **pg_config)


def execute_sql(sql):
    global pg_pool

    # Initialize the pool lazily, in case SQL access isn't needed for this
    # GCF instance. Doing so minimizes the number of active SQL connections,
    # which helps keep your GCF instances under SQL connection limits.
    if not pg_pool:
        try:
            print("connecting...")
            __connect(f'/cloudsql/{CONNECTION_NAME}')
        except OperationalError:
            # If production settings fail, use local development ones
            print("failed trying localhost")
            __connect('localhost')

    # Remember to close SQL resources declared while running this function.
    # Keep any declared in global scope (e.g. pg_pool) for later reuse.
    with pg_pool.getconn() as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchone()
        pg_pool.putconn(conn)
        return str(results[0])

def postgres_demo(request):
  check_table_sql = check_table_exists_sql('APPLE_HEALTH_DATA')
  return execute_sql(check_table_sql)
