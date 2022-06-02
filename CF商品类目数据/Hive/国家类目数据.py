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

con_hive = prestodb.dbapi.connect(
    host='ec2-54-68-88-224.us-west-2.compute.amazonaws.com',
    port=80,
    user='hadoop',
    catalog='hive',
    schema='default',
)

con_ms = pymssql.connect("172.16.92.2", "sa", "yssshushan2008", "CFflows", charset="utf8")


sql_hive = ('''
select 
       case when so.channel <>'cod' then 'ppd' else 'cod' end pay_style,
       AVG(sol.price_real),
       count(distinct so.order_name)  as pay_order_num,
       count(distinct so.user_id)  as pay_user_num,
       sum(sol.origin_qty)  as origin_qty,
       sum(sol.origin_qty*sol.price_unit) as origin_amount
from jiayundw_dm.sale_order_info_df as so 
join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
join jiayundw_dim.product_basic_info_df as a on sol.item_no=a.item_no
where sol.is_delivery=0 and date(so.create_at+interval '8' hour) between date('2020-05-28') and date('2020-05-31')
group by 
         case when so.channel <>'cod' then 'ppd' else 'cod' end 

''')

cursor = con_hive.cursor()
cursor.execute(sql_hive)
data = cursor.fetchall()
column_descriptions = cursor.description
if data:
    data_hive = pd.DataFrame(data)
    data_hive.columns = [c[0] for c in column_descriptions]
else:
    data_hive = pd.DataFrame()

print(data_hive)

writer = pd.ExcelWriter('郭美红' + '.xlsx')
data_hive.to_excel(writer, sheet_name='商品数据', index=False)
os.chdir(r'/Users/apache/Downloads/A-python')
writer.save()
