from psycopg2.extensions import adapt, AsIs

class AppleHealthRecord(object):
    'Definition for an Apple Health Record'

    def __init__(self, record_type, unit, time_created, record_value):
        self.record_type = record_type
        self.unit = unit
        self.time_created = time_created
        self.record_value = record_value

    def to_insert_sql(self):
        return "('%s', '%s', '%s', %s)" % (self.record_type, self.unit, self.time_created, self.record_value)

    def __str__(self):
        return self.to_insert_sql()

def check_table_exists_sql(table):
    return  ''.join(['SELECT EXISTS ( ',
                    'SELECT 1 ',
                    'FROM   information_schema.tables ',
                    'WHERE  table_name = \'', table, '\'',
                    ');'])

def create_table_sql():
    return  ' '.join(['CREATE TABLE public.apple_health_data (',
                        'record_type TEXT NOT NULL,',
                        'unit TEXT NOT NULL,',
                        'time_created TIMESTAMPTZ NOT NULL,',
                        'record_value DECIMAL NOT NULL',
                        ') TABLESPACE pg_default;'])

def drop_table_sql(table):
        return f'DROP TABLE IF EXISTS {table}'

def truncate_table_sql(table):
        return f'TRUNCATE TABLE {table}'

def row_count_sql(table):
        return f'SELECT COUNT(1) FROM {table}'

def insert_apple_health_record_sql(ahr):
        return f'INSERT INTO apple_health_data (record_type, unit, time_created, record_value) VALUES {ahr.to_insert_sql()}'