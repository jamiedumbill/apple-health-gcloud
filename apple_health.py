
def AppleHealthRecord(Object):
    'Definition for an Apple Health Record'

    def __init__(self, data_type, unit, time_ceated, value):
        self.type = data_type
        self.unit = unit
        self.time_ceated = time_ceated
        self.value = value

def check_table_exists_sql(table):
    sql =  'SELECT EXISTS ( '
    sql =+ 'SELECT 1 '
    sql =+ 'FROM   information_schema.tables '
    sql =+ 'WHERE  table_name = \'' + table + '\''
    sql =+ ');'