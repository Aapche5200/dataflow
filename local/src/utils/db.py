# -*- coding: utf-8 -*-
import time
import base64
from sqlalchemy.types import Float, BIGINT, Date, DateTime, NVARCHAR
from sqlalchemy import create_engine
from clickhouse_driver import connect
import pandas as pd
from lib.mylog import log
from utils.setting import db_state, IS_ONLINE

dbcons = {}
# 基本命名说明：dbs：数据库；dbc：数据库集群地址，# db_name：数据库名字,type:数据库类型，pg-postgres，ms：mysql，ss：sqlserver，ck：clickhouse
dbs = {
    # 数据管理部的报表数据库
    'dmpg': {'type': 'pg',
             'db_str': 'cG9zdGdyZXNxbCtwc3ljb3BnMjovL3BnYWRtaW46RUYyQlI0S3lrSG81N0ZAMTAuMjAuMTIwLjE4MjozMjAwMC9yZXBvcnQ='},
    # OA正式数据库
    'oa': {
        'db_str': 'bXNzcWwrcHltc3NxbDovL3NqZng6NVZ2WFRXUyVuZmJQUmV4MVNiQDEwLjIwLjEyMC4xMTQvQUJWNQ==',
# 'db_str': 'bXNzcWwrcHltc3NxbDovL3NqZng6NVZ2WFRXUyVuZmJQUmV4MVNiQDEwLjIwLjEyMC4xMTQvQUJWNT9jaGFyc2V0PXV0Zjg=',
           'type': 'ss', },

}
# 本地专用的临时数据库
temp_dbs = {

}
if not IS_ONLINE:
    dbs.update(temp_dbs)


def get_encryption_code(word):
    return base64.b64encode(word.encode()).decode()


def get_encryption_word(code):
    return base64.b64decode(code.encode()).decode()


# word = 'mssql+pymssql://sjfx:5VvXTWS%nfbPRex1Sb@10.20.120.114/ABV5?charset=GBK'
# print(get_encryption_code(word))
# print(get_encryption_word(dbs.get('oa').get('db_str')))
# exit()


class DbCon:
    def __init__(self, db_code='dmpg'):
        # is_test：主要用于特定场景中正式库和测试库之间的切换，最好少用
        if db_code not in dbs.keys():
            raise SystemExit('please input right db_code')
        db_str = get_encryption_word(dbs.get(db_code).get('db_str'))

        # db_name：主要用于辨识数据库
        self.db_name = db_str[db_str.rfind('/') + 1:]
        # db_type：主要用于处理不同数据库之间的语句差异，比如limit 语句
        self.db_type = dbs.get(db_code).get('type')
        if self.db_type == 'ck':
            self.engine = connect(db_str)
            self.connect = self.engine.cursor()
        else:
            self.engine = create_engine(db_str, pool_size=30, pool_timeout=30, pool_recycle=-1, max_overflow=20,
                                        )
            self.connect = self.engine.connect()

    def read_sql(self, sql, is_example=False):
        # 直接携带读取SQL的方法，更加方便
        if is_example:
            if self.db_type in ['pg', 'ck']:
                sql = f'select * from ({sql}) temp_table limit 1000'
            elif self.db_type in ['ss']:
                sql = f'select  top 1000 * from ({sql}) temp_table'

        if self.db_type == 'ck':
            curser = self.connect
            curser.execute(sql)
            columns = curser.columns_with_types
            columns = [column[0] for column in columns]
            data = curser.fetchall()
            df = pd.DataFrame(data, columns=columns)
            return df
        else:
            try:
                df = pd.read_sql(sql, self.connect, coerce_float=False)
                return df
            except:
                # 如果连接失效了，重新生成一个。
                self.connect = self.engine.connect()
                df = pd.read_sql(sql, self.connect, coerce_float=False)
                return df

    def execute_sql(self, sql):
        self.connect.execute(sql)

    def to_sql(self, df, table, if_exists='replace', fillna=True, dtype=None, index=False, transfer_time=None,
               **kwargs):

        if df.empty:
            log.warn(f'{table} is empty')

        # 如果要填充的话，就按照数据格式填充
        if fillna:
            field_dict = df.dtypes.astype(str).to_dict()
            str_fields = [k for k, v in field_dict.items() if v in ['object']]
            int_fields = [k for k, v in field_dict.items() if v in ['int64', 'float64', 'int32', 'float32']]
            float_fields = [k for k, v in field_dict.items() if v in ['float64', 'float32']]
            if int_fields:
                df[int_fields] = df[int_fields].fillna(0).astype('int64')
            if float_fields:
                df[float_fields] = df[float_fields].fillna(0.0).astype('float64')
            if str_fields:
                df[str_fields] = df[str_fields].fillna('').astype('str')

        # 追加写入时间
        if not transfer_time:
            transfer_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        transfer_date = transfer_time[:10]
        df['transfer_time'] = transfer_time
        df['transfer_date'] = transfer_date
        if not dtype:
            dtype = {'transfer_time': DateTime(), 'transfer_date': Date()}
        else:
            dtype.update({'transfer_time': DateTime(), 'transfer_date': Date()})
        if self.db_type not in ['ck']:
            df.to_sql(table, self.connect, if_exists=if_exists, dtype=dtype, index=index, **kwargs)

    def col_group_count(self, table, col):
        sql = f''' select {col},count(*) as cnt from {table} group by {col} order by cnt desc '''
        df = self.read_sql(sql)
        df['percent'] = df['cnt'] / df['cnt'].sum()
        return df


def update_cons():
    # 初始化所有数据库连接对象，
    for k, v in dbs.items():
        try:
            dbcons[k] = DbCon(k)
        except:
            pass


update_cons()
