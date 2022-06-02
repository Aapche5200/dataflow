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
start = '2020-07-10'
con_hive = prestodb.dbapi.connect(
    host='ec2-54-68-88-224.us-west-2.compute.amazonaws.com',
    port=80,
    user='hadoop',
    catalog='hive',
    schema='default',
)

sql_hive = ('''										
SELECT log_date,product_level,front_cate_one,item_num from
((select      	log_date,
                case when a.product_level=5 then '头部商品' 
                when a.product_level=4 then '腰部商品'
                when a.product_level=3 then '长尾商品'
                when a.product_level in (1,2,6) then '测试商品'
                else '其他' end product_level,
           front_cate_one,
           count(distinct case when product_active=1 then product_no else null end) as item_num
from  jiayundw_dws.product_info_history_df as a 
join jiayundw_dim.product_basic_info_df as b on b.item_no=a.product_no
where log_date BETWEEN '{0}' and  '{0}'
group by 1,2,3
order by 1,2,3)
union all 
(select        log_date,
                case when a.product_level=5 then '头部商品' 
                when a.product_level=4 then '腰部商品'
                when a.product_level=3 then '长尾商品'
                when a.product_level in (1,2,6) then '测试商品'
                else '其他' end product_level,
								'全类目' as front_cate_one,
           count(distinct case when product_active=1 then product_no else null end) as item_num
from  jiayundw_dws.product_info_history_df as a 
join jiayundw_dim.product_basic_info_df as b on b.item_no=a.product_no
where log_date BETWEEN '{0}' and  '{0}'
group by 1,2,3
order by 1,2,3))
order by 1,2,3
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

engine_ms = create_engine("mssql+pymssql://sa:yssshushan2008@172.16.92.2:1433/CFcategory?charset=utf8")
df_hive.to_sql('商品等级数据_前台', con=engine_ms, if_exists='append', index=False)

sql_ms = ('''
select top 10 log_date from CFcategory.dbo.商品等级数据_前台
order by log_date desc
''')

df_ms = pd.read_sql(sql_ms, engine_ms)
print(df_ms)
