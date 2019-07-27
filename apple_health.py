
def AppleHealthRecord(Object):
    'Definition for an Apple Health Record'

    def __init__(self, record_type, unit, time_ceated, value):
        self.record_type = record_type
        self.unit = unit
        self.time_ceated = time_ceated
        self.value = value

def check_table_exists_sql(table):
    return  ''.join(['SELECT EXISTS ( ',
                    'SELECT 1 ',
                    'FROM   information_schema.tables ',
                    'WHERE  table_name = \'' + table + '\'',
                    ');'])

def create_table_sql():
    return  ' '.join(['CREATE TABLE apple_health_data (',
                        'record_type TEXT NOT NULL,',
                        'unit TEXT NOT NULL,',
                        'time_ceated TIMESTAMPTZ NOT NULL,',
                        'value DECIMAL NOT NULL',
                        ')'])