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

con_redshift = psycopg2.connect(user='operation', password='Operation123',
                                host='jiayundatapro.cls0csjdlwvj.us-west-2.redshift.amazonaws.com',
                                port=5439, database='jiayundata')

con_hive = hive.Connection(host="ec2-34-222-53-168.us-west-2.compute.amazonaws.com", port=10000, username="hadoop")

con_ms = pymssql.connect("172.16.92.2", "sa", "yssshushan2008", "CFflows", charset="utf8")


sql_hive = ('''
%hive
select a.*,b.pay_uv,b.qty,b.amount
from 
(
    select ua.log_date,item_no,
    count(distinct case when  event_type='product' and mid like '%.9.1' then cid else null end) as imp_uv,
    count(distinct case when  event_type='click' and mid like '%.9.1' then cid else null end) as click_uv,
count(distinct case when event_type in ('product','impression') and mid='1.5.1.1' then cid else null end) as detail_uv
from jiayundata.flow_pid_action_di as ua
join jiayundw_dim.product_basic_info_df as pro on pro.pid=ua.pid
where ua.log_date between '2020-03-30' and '2020-04-05' and item_no in 
(
'PBC013360655N'

)
group by ua.log_date,item_no
) as a 
left join 
(
    select date(so.create_at+interval '8' hour) as log_date,item_no,
    count(distinct so.user_id)  as pay_uv,
    sum(sol.origin_qty)  as qty,
    sum(sol.origin_qty*sol.price_unit) as amount   
from jiayundw_dm.sale_order_info_df as so 
join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
where  sol.is_delivery=0  and date(so.create_at+interval '8' hour) between '2020-03-30' and '2020-04-05'
group by date(so.create_at+interval '8' hour),item_no
) as b on b.item_no=a.item_no and b.log_date=a.log_date
order by a.item_no,a.log_date
''')

data_hive = pd.read_sql(sql_hive, con_hive)


writer = pd.ExcelWriter('63商品' + '.xlsx')
data_hive.to_excel(writer, sheet_name='商品数据', index=False)
os.chdir(r'/Users/apache/Downloads/A-python')
writer.save()
