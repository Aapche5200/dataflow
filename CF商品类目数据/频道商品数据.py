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


start = '2020-05-31'
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
from jiayundw_dwd.flow_user_trace_da
where date_id between '{0}' and '{0}'
and date(server_time + interval '8' hour) = cast('{0}' as date)
and event_type in ('product','impression','click','deep_link','pay_success','play','stay','push_link')
and mid like '1.%'
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

engine_ms = create_engine("mssql+pymssql://sa:yssshushan2008@172.16.92.2:1433/CFflows?charset=utf8")
df_hive.to_sql('CfDau', con=engine_ms, if_exists='append', index=False)

sql_ms = ('''
select top 10 event_date from CFflows.dbo.CfDau
order by event_date desc
''')

df_ms = pd.read_sql(sql_ms, engine_ms)
print(df_ms)
