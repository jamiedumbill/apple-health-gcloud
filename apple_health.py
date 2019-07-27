
def AppleHealthRecord(Object):
    'Definition for an Apple Health Record'

    def __init__(self, data_type, unit, time_ceated, value):
        self.type = data_type
        self.unit = unit
        self.time_ceated = time_ceated
        self.value = value

def check_table_exists_sql(table):
    return  ''.join(['SELECT EXISTS ( ',
                    'SELECT 1 ',
                    'FROM   information_schema.tables ',
                    'WHERE  table_name = \'' + table + '\'',
                    ');'])