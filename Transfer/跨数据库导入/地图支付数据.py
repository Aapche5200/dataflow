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

con_hive = hive.Connection(host="ec2-34-222-53-168.us-west-2.compute.amazonaws.com", port=10000, username="hadoop")

sql_hive = ('''
select date(so.create_at+interval '8' hour) as log_date,shipping_zip,shipping_country,shipping_state,shipping_city,
			 sum(sol.origin_qty)  as origin_qty,
			 sum(sol.origin_qty*sol.price_unit) as origin_amount
from jiayundw_dm.sale_order_info_df as so 
join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
where sol.is_delivery=0 and date(so.create_at+interval '8' hour) between '2020-04-09' and '2020-04-13' 
group by date(so.create_at+interval '8' hour) ,shipping_zip,shipping_country,shipping_state,shipping_city
''')

data_hive = pd.read_sql(sql_hive, con_hive)

engine_ms = create_engine("mssql+pymssql://sa:yssshushan2008@172.16.92.2:1433/CFcategory?charset=utf8")
data_hive.to_sql('MapPay', con=engine_ms, if_exists='append', index=False)
