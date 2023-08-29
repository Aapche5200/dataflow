# -*- coding: utf-8 -*-

import time
import base64
import pandas as pd
import sqlalchemy
from sqlalchemy.types import Float, BIGINT, Date, DateTime, NVARCHAR
from collections import defaultdict
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import create_engine, text



dbcons = {}
dbs_xbb = {
    # 统一报表数据库
    'gp': {'type': 'pg',
           'db_str': 'bXlzcWwrcHlteXNxbDovL3Jvb3Q6a2xpR0puVERJVkd1UzYzdkAxMC4yMC4xMTguMTE6MzAwMDYvZGJfeGJiX3ByaXZhdGU='},
}

def get_encryption_code(word):
    return base64.b64encode(word.encode()).decode()

def get_encryption_word(code):
    return base64.b64decode(code.encode()).decode()

class Dbcon_xbb:
    def __init__(self, db_code='dmpg'):
        # is_test：主要用于特定场景中正式库和测试库之间的切换，最好少用
        if db_code not in dbs_xbb.keys():
            raise SystemExit('please input right db_code')
        db_str = get_encryption_word(dbs_xbb.get(db_code).get('db_str'))

        # db_name：主要用于辨识数据库
        self.db_name = db_str[db_str.rfind('/') + 1:]
        # db_type：主要用于处理不同数据库之间的语句差异，比如limit 语句
        self.db_type = dbs_xbb.get(db_code).get('type')
        if self.db_type == 'ck':
            self.engine = connect(db_str)
            self.connect = self.engine.cursor()
        else:
            self.engine = create_engine(db_str, pool_size=30, pool_timeout=30, pool_recycle=-1, max_overflow=20)
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


