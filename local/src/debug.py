from lib.excel import write2excel
from utils.db import DbCon

oa = DbCon('oa')
pg = DbCon('dmpg')


def get_table_structure():
    sql = ''' select * from dws_oa_table_structure '''
    df = pg.read_sql(sql)
    print(df.head(10))
    # write2excel(df)


get_table_structure()
