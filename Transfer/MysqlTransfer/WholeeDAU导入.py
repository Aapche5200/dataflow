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


start = '2020-08-29'
con_hive = prestodb.dbapi.connect(
    host='ec2-54-68-88-224.us-west-2.compute.amazonaws.com',
    port=80,
    user='hadoop',
    catalog='hive',
    schema='default',
)

sql_hive = ('''
select date(server_time + interval '8' hour) as event_date,
       count(distinct cid) as dau
from ods_kafka.user_trace_prod
where date(date_id) between date('{0}')- interval '1' day and date('{0}')
and date(server_time + interval '8' hour) = cast('{0}' as date)
and event_type in ('pageview','impression')
and mid like '8.%'
group by 1
order by 1
''').format(start)

cursor = con_hive.cursor()
cursor.execute(sql_hive)
data = cursor.fetchall()
column_descriptions = cursor.description
if data:
    df_hive = pd.DataFrame(data)
    df_hive.columns = [c[0] for c in column_descriptions]
else:
    df_hive = pd.DataFrame()
print(df_hive)

engine_ms = create_engine(
    "mysql+pymysql://root:yssshushan2008@127.0.0.1:3306/CFflows?charset=utf8")
df_hive.to_sql('WholeeDau', con=engine_ms, if_exists='append', index=False)

sql_ms = ('''
select * from CFflows.WholeeDau
order by event_date ASC
''')

df_ms = pd.read_sql(sql_ms, engine_ms)
print(df_ms)
