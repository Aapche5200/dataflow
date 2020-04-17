import os
from datetime import datetime, timedelta  # 设置当前时间及时间间隔计算需要用的包
import pandas as pd
from sqlalchemy.engine import create_engine
from pyhive import hive
from impala.dbapi import connect

con_hive = hive.Connection(host="ec2-34-222-53-168.us-west-2.compute.amazonaws.com", port=10000, username="hadoop")


sql_goods = '''
    select item_no,front_cate_one,product_level,sum(gmv),sum(refund_total),
    sum(gmv) - sum(refund_total) as netgmv
    from analysts.djy_temp_01_ss
    where product_level in (4,5)
    group by item_no,front_cate_one,product_level
'''

data_goods = pd.read_sql(sql_goods, con_hive)
print(data_goods.head(1))

writer = pd.ExcelWriter('新等级' + '.xlsx')
data_goods.to_excel(writer, sheet_name='商品', index=False)
os.chdir(r'/Users/apache/Downloads/A-python')
writer.save()
