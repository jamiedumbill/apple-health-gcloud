from os import getenv
import logging

from psycopg2 import OperationalError
from psycopg2.pool import SimpleConnectionPool

from apple_health import *

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s  %(name)s  %(levelname)s: %(message)s')
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
            LOGGER.info("connecting to postgres...")
            __connect(f'/cloudsql/{CONNECTION_NAME}')
        except OperationalError:
            # If production settings fail, use local development ones
            LOGGER.warning("failed to connect to gcloud trying localhost")
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
  LOGGER.info("checking apple_health_data table exists in the database")
  table_exists = execute_sql(check_table_exists_sql('apple_health_data'))[0][0]

  if not table_exists:
    LOGGER.info("apple_health_data does not exist creating now")
    execute_sql(create_table_sql())
  else:
    LOGGER.info("table_exists is %s apple_health_data exists", table_exists)

  LOGGER.info("truncating apple_health_data")
  execute_sql(truncate_table_sql('apple_health_data'))

  row_count = execute_sql(row_count_sql('apple_health_data'))[0][0]
  LOGGER.info("row count after truncating apple_health_data is %s", row_count)

  LOGGER.info("inserting test row")
  record = AppleHealthRecord('HKQuantityTypeIdentifierBodyMassIndex','count','2015-07-13T07:22:59-04:00',25.6421)
  execute_sql(insert_apple_health_record_sql(record))

  row_count = execute_sql(row_count_sql('apple_health_data'))[0][0]
  LOGGER.info("row count after inserting test row is %s", row_count)

  LOGGER.info("truncating apple_health_data")
  execute_sql(truncate_table_sql('apple_health_data'))

  return str(table_exists)

if __name__ == "__main__":
  postgres_demo('')