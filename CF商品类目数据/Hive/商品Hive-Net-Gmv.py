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
        host='ec2-34-220-123-60.us-west-2.compute.amazonaws.com',
        port=80,
        user='hadoop',
        catalog='hive',
        schema='default',
)

sql_hive_goods_netgmv = '''
select
b.front_cate_one,
,sum(a.total) total
,sum(a.refund_total) refund_total
,sum(a.qty) qty
,sum(a.refund_qty) refund_qty
,(sum(a.total)-sum(a.refund_total)) net_gmv
from (
select a.create_date
,a.seller_type
,case when a.channel='cod' then 'cod'else'ppd'end channel
,a.warehouse
,a.item_no
,a.total
,a.refund_total
,case when a.is_delivery=0 then a.origin_qty else 0 end qty
,case when a.is_delivery=0 then a.refund_qty else 0 end refund_qty
from analysts.tbl_order_detail a
 where a.create_date between date('2019-12-25') and date('2019-01-25')
 and a.pt = '2020-01-25'
 ) a
 left jion jiayundw_dim.product_basic_info_df as b
 on a.item_no=b.item_no
group b.front_cate_one
'''

cursor_one = con_hive.cursor()
# hive 一级类目net-gmv数据处理
cursor_one.execute(sql_hive_goods_netgmv)
data_hive_cateone_netgmv = cursor_one.fetchall()
column_descriptions = cursor_one.description
if data_hive_cateone_netgmv:
    data_hive_cateone_netgmv_df = pd.DataFrame(data_hive_cateone_netgmv)
    data_hive_cateone_netgmv_df.columns = [c[0] for c in column_descriptions]
else:
    data_hive_cateone_netgmv_df = pd.DataFrame()

print(data_hive_cateone_netgmv_df.head(10))
