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

con_ms = pymssql.connect("172.16.92.2", "sa", "yssshushan2008", "CFflows", charset="utf8")


sql_hive = ('''
select sol.item_no,shipping_country,a.front_cate_one,a.front_cate_two,a.front_cate_three,
       cate_one_cn,cate_two_cn,cate_three_cn,
       count(distinct so.order_name)  as pay_order_num,
       count(distinct so.user_id)  as pay_user_num,
       sum(sol.origin_qty)  as origin_qty,
       sum(sol.origin_qty*sol.price_unit) as origin_amount
from jiayundw_dm.sale_order_info_df as so 
join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
join jiayundw_dim.product_basic_info_df as a on sol.item_no=a.item_no
where sol.is_delivery=0 and date(so.create_at+interval '8' hour) between date('2020-04-01') and date('2020-04-13')
and shipping_country in ('United States', 'Mexico') 
group by sol.item_no,shipping_country,a.front_cate_one,a.front_cate_two,a.front_cate_three
''')

data_hive = pd.read_sql(sql_hive, con_hive)
print(data_hive)

writer = pd.ExcelWriter('国家类目数据-货号' + '.xlsx')
data_hive.to_excel(writer, sheet_name='商品数据', index=False)
os.chdir(r'/Users/apache/Downloads/A-python')
writer.save()
