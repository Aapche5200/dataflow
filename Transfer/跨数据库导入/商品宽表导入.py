import pymssql
import pandas as pd  # 数据处理例如：读入，插入需要用的包
import numpy as np  # 平均值中位数需要用的包
import os  # 设置路径需要用的包
import psycopg2
from datetime import datetime, timedelta  # 设置当前时间及时间间隔计算需要用的包
import prestodb
from sqlalchemy.engine import create_engine
from pyhive import hive
from impala.dbapi import connect
from sqlalchemy import create_engine

con_hive = hive.Connection(host="ec2-34-222-53-168.us-west-2.compute.amazonaws.com", port=10000, username="hadoop")

sql_hive = ('''										
select * from jiayundw_dim.product_basic_info_df
where active=1
''')

df_hive = pd.read_sql(sql_hive, con_hive)

engine_ms = create_engine("mssql+pymssql://sa:yssshushan2008@172.16.92.2:1433/CFgoodsday?charset=utf8")
df_hive.to_sql('product_basic_info', con=engine_ms, if_exists='replace', index=False)

sql_ms = ('''
select top 10 * from CFgoodsday.dbo.product_basic_info
''')

df_ms = pd.read_sql(sql_ms, engine_ms)
print(df_ms)
