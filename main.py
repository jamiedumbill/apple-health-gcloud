from os import getenv
import logging

from psycopg2 import OperationalError
from psycopg2.pool import SimpleConnectionPool

from apple_health import *

logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)

# TODO(developer): specify SQL connection details
CONNECTION_NAME = getenv('INSTANCE_CONNECTION_NAME', 'localhost')
DB_USER = getenv('POSTGRES_USER', 'postgres')
DB_PASSWORD = getenv('POSTGRES_PASSWORD', '')
DB_NAME = getenv('POSTGRES_DATABASE', 'applehealth')

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
        try:
          cursor.execute(sql)
          if cursor.rowcount > 0:
            results = cursor.fetchall()
          else:
            results = "No records"
        except Exception as e:
          LOGGER.error("Problem with query %s", e)
          results = "Error"
        finally:
          conn.commit()
          pg_pool.putconn(conn)
        return results

def postgres_demo(request):
  LOGGER.info("Checking apple_health_data table exists in the database")
  table_exists_results = execute_sql(check_table_exists_sql('apple_health_data'))
  table_exists = table_exists_results[0][0]
  if not table_exists:
    LOGGER.info("apple_health_data does not exist creating now")
    execute_sql(create_table_sql())
  else:
    LOGGER.info("table_exists is %s apple_health_data exists", table_exists)
  return str(table_exists)

if __name__ == "__main__":
  postgres_demo('')