from sqlalchemy import create_engine
from clickhouse_driver import connect
from utils.setting import env_info
import pandas as pd

con_dict = {}


def get_con(db_code='ss'):
    db_str = env_info.get(db_code)
    conn = create_engine(db_str, pool_size=30, pool_timeout=30, pool_recycle=3600, max_overflow=20)
    # con = conn.connect()
    return conn


def ck_client():
    conn = connect(host=env_info.get('ck_host'), port=int(env_info.get('ck_port')),
                   database=env_info.get('ck_db'), user=env_info.get('ck_user'), password=env_info.get('ck_password'))
    # cursor = conn.cursor()
    return conn


def update_con_dict():
    # 固化连接
    con_dict['ss'] = get_con()
    con_dict['pg'] = get_con('pg')
    con_dict['pgbk'] = get_con('pgbk')
    con_dict['zentao'] = get_con('zentao')
    con_dict['ck'] = ck_client()


update_con_dict()


def get_trans_table_list():
    # 数据库直接读取有权限的表来同步
    server_con = con_dict.get('ss').connect()
    sql = ''' SELECT a.name name FROM sysobjects AS a INNER JOIN sysindexes AS b ON a.id = b.id WHERE (a.type = 'u') AND (b.indid IN (0, 1)) and b.rows!=0  '''
    df = pd.read_sql(sql, server_con)
    server_con.close()
    table_list = list(df['name'])

    def true_table(table):
        # 主要的日常oa表
        if table.find('form') == 0:
            return True
        # 主要的系统表
        elif table in ['CTP_ENUM_ITEM', 'FORM_DEFINITION', 'ORG_MEMBER', 'ORG_POST', 'ORG_UNIT']:
            return True
        # 其他表
        elif table in ['FRControl']:
            return True
        else:
            return False

    table_list = list(filter(true_table, table_list))
    table_list.sort(reverse=True)

    return table_list


def get_ck_table_list():
    ck_con = con_dict.get('ck').cursor()
    sql = ''' SELECT distinct table  FROM system.parts where database = '{}' '''.format(env_info.get('ck_db'))
    ck_con.execute(sql)
    data = ck_con.fetchall()
    ck_con.close()
    return [d[0] for d in data]
