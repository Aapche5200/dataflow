from utils.public_object import con_dict
import pandas as pd


def read_ck_df(sql):
    # 分别读取表头和数据，合并。
    ck_con = con_dict.get('ck').cursor()

    ck_con.execute(sql)
    columns = ck_con.columns_with_types
    columns = [column[0] for column in columns]
    data = ck_con.fetchall()
    df = pd.DataFrame(data, columns=columns)

    return df


def execute_ck_sql(sql):
    ck_con = con_dict.get('ck').cursor()
    ck_con.execute(sql)


def truncate_table(table_name):
    # 清空CK表
    ck_con = con_dict.get('ck')
    sql = '''TRUNCATE TABLE IF EXISTS {}'''.format(table_name)
    ck_con.execute(sql)


def df_to_insert_sql(table_df, table_name):
    # 中间简单采用转义替换
    # 构建clickhouse的插入sql语句。
    table_df = table_df.fillna('').astype(str)
    table_values = table_df.values
    table_values = ",\n".join(
        [" ('" + "','".join([a.replace("\\", "\\\\").replace(r"'", r"\'") for a in i]) + "') " for i in
         table_values])

    insert_sql = '''
    insert into {} ({}) values {}
    '''.format(table_name, ','.join(table_df.columns), table_values)
    return insert_sql


def get_table_rename_dict(table_name):
    # 获取表的字段字典映射
    sql = '''select field_name, field_display from dws_oa_table_structure where table_name = '{}'
    '''.format(table_name)
    df = read_ck_df(sql)
    rename_dict = df.set_index('field_name').to_dict(orient='dict').get('field_display')
    return rename_dict


def df_to_sql(df, table):
    sql = 'INSERT INTO ' + table + ' (' + str(', '.join(df.columns)) + ') VALUES ' + ', '.join(
        [str(tuple(row.values)) for index, row in df.iterrows()])
    sql = sql.replace('None', 'Null')
    return sql
