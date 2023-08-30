from utils.public_object import con_dict
from utils.read_sql_ck import get_table_rename_dict
import pandas as pd

server_con = con_dict.get('ss')


def read_ss_df(sql):
    server_con = con_dict.get('ss').connect()
    df = pd.read_sql(sql, server_con, coerce_float=False)

    return df


def get_table_data(table, columns=None, row_num=None, where='', return_type='df', data_type=''):
    table_rename_dict = get_table_rename_dict(table)
    # 直接读取
    if row_num:
        top_str = ' top {}'.format(row_num)
    else:
        top_str = ''
    if columns:
        field_sql = ' , '.join([{v: k for k, v in table_rename_dict.items()}.get(field, field) for field in columns])
    else:
        field_sql = ''' * '''
    sql = '''select {} {} 
            from {}  {} '''.format(top_str, field_sql, table, where)

    df = read_ss_df(sql)
    df = df.rename(columns=table_rename_dict)
    if data_type == 'str':
        df = df.astype(str).fillna('').replace({'None': '', 'NaT': '', 'nan': ''})
    return df


def get_enum_df(data_type='str'):
    sql = 'select id,showvalue from CTP_ENUM_ITEM'
    df = read_ss_df(sql)
    if data_type == 'str':
        return df.astype(str).fillna('').replace({'None': '', 'NaT': '', 'nan': ''})


def get_member_df(data_type='str'):
    sql = 'select id,name as showvalue,code as employee_id from org_member '
    df = read_ss_df(sql)
    if data_type == 'str':
        return df.astype(str).fillna('').replace({'None': '', 'NaT': '', 'nan': ''})


def get_unit_df(data_type='str'):
    sql = 'select id,name as showvalue from org_unit'
    df = read_ss_df(sql)
    if data_type == 'str':
        return df.astype(str).fillna('').replace({'None': '', 'NaT': '', 'nan': ''})


def change_enum_value(df, field, enum_df):
    # join上要替换枚举的真实值
    df = df.merge(enum_df, how='left', left_on=field, right_on='id')
    # 没有join上的枚举值用原值来替换
    df.loc[df['showvalue'].isnull(), 'showvalue'] = df.loc[df['showvalue'].isnull(), field]
    # 重命名和删除原来的列
    df = df.drop(['id', field], axis=1)
    df = df.rename(columns={'showvalue': field})

    return df


ss_public_data = {'enum_df': get_enum_df(), 'member_df': get_member_df(), 'unit_df': get_unit_df}
