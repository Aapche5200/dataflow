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
select create_date, product_no,illegal_tags,write_uid,cate_one_cn,cate_two_cn,cate_three_cn,
AVG(price_real) as avg_price,sum(product_qty) as qty,sum(gmv) as gmv from analystsdev.shushan_temp_01
where  illegal_tags like '%63%'
group by create_date, product_no,illegal_tags,write_uid,cate_one_cn,cate_two_cn,cate_three_cn
''')

data_hive = pd.read_sql(sql_hive, con_hive)


writer = pd.ExcelWriter('杨俊-63商品-new' + '.xlsx')
data_hive.to_excel(writer, sheet_name='商品数据', index=False)
os.chdir(r'/Users/apache/Downloads/A-python')
writer.save()
