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

con_hive = prestodb.dbapi.connect(
    host='ec2-54-68-88-224.us-west-2.compute.amazonaws.com',
    port=80,
    user='hadoop',
    catalog='hive',
    schema='default',
)

sql_hive = ('''										
select item_no,											
        pid,											
        seller_id,											
        seller_name,											
        seller_type,											
        front_cate_one,											
        front_cate_two,
        front_cate_three,
        create_at,											
        product_level,											
        rating,											
        "7d_rating",											
        gmv,											
        qty,											
        refund_total,											
        return_refund_total,											
        cancel_refund_total,											
        miss_refund_total,											
        reject_refund_total,											
        other_refund_total,																						
        refund_qty,											
        return_refund_qty,											
        cancel_refund_qty,											
        miss_refund_qty,											
        reject_refund_qty,											
        other_refund_qty											
from analysts.yss_item_net_four
where pt ='2020-07-14'
''')

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

engine_ms = create_engine("mssql+pymssql://sa:yssshushan2008@172.16.92.2:1433/CFcategory?charset=utf8")
df_hive.to_sql('NetGmvData', con=engine_ms, if_exists='append', index=False)

sql_ms = ('''
select top 10 create_at from CFcategory.dbo.NetGmvData
order by create_at desc
''')

df_ms = pd.read_sql(sql_ms, engine_ms)
print(df_ms)
