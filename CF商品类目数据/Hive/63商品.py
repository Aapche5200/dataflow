import pymssql
import pandas as pd  # 数据处理例如：读入，插入需要用的包
import numpy as np  # 平均值中位数需要用的包
import os  # 设置路径需要用的包
import psycopg2
import prestodb
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


sql_hive = ('''
SELECT DISTINCT a.item_no,front_cate_one,front_cate_two,front_cate_three,illegal_tags,name,active,write_uid,seller_id,seller_name,
rating,price,origin_qty,pay_user_num,origin_amount 
from jiayundw_dim.product_basic_info_df a
left join
(
select item_no,
       count(distinct so.order_name)  as pay_order_num,
			 count(distinct so.user_id)  as pay_user_num,
			 sum(sol.origin_qty)  as origin_qty,
			 sum(sol.origin_qty*sol.price_unit) as origin_amount
from jiayundw_dm.sale_order_info_history_df as so 
join jiayundw_dm.sale_order_line_history_df as sol on sol.order_name=so.order_name
where sol.is_delivery=0 and date(so.create_at+interval '8' hour) between date('2020-05-24') and date('2020-05-24') 
and sol.date_id ='2020-05-24'  and so.date_id='2020-05-24'
group by item_no
) as c on a.item_no=c.item_no
WHERE illegal_tags like '%73%'  and active=1
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

data_hive.rename(columns={'item_no': '货号', 'cate_one_cn': '后台一级', 'cate_two_cn': '后台二级',
                          'cate_three_cn': '后台三级',
                          'illegal_tags': '标签', 'name': '标题', 'write_uid': '上货来源',
                          'seller_id': '商家ID', 'seller_name': '商家名称',
                          'uv': '商详页UV', 'click_num': '商品点击', 'impression_num': '商品曝光',
                          'price': '售价', 'origin_qty': '销量', 'pay_user_num': '支付买家数',
                          'origin_amount': '支付金额'},
                 inplace=True)

writer = pd.ExcelWriter('73商品-18--24号' + '.xlsx')
data_hive.to_excel(writer, sheet_name='商品数据', index=False)
os.chdir(r'/Users/apache/Downloads/A-python')
writer.save()
