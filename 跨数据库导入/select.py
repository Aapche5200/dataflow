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
SELECT DISTINCT a.item_no,cate_one_cn,cate_two_cn,cate_three_cn,
front_cate_one,front_cate_two,front_cate_three,illegal_tags,write_uid,
uv ,click_num ,impression_num ,origin_qty,pay_user_num,origin_amount 
from jiayundw_dim.product_basic_info_df a
left join
(
 select item_no,
               count(distinct case when event_type in ('product','impression') and mid='1.5.1.1' then cid else null end) as uv,
               count(case when  event_type='product' and mid like '%.9.1' then cid else null end) as impression_num,
               count(case when  event_type='click' and mid like '%.9.1' then cid else null end) as click_num
        from jiayundw_dws.flow_pid_action_di as ua 
        join jiayundw_dim.product_basic_info_df as pro on pro.pid=ua.pid
where ua.log_date between '2020-04-08' and '2020-04-14' 
group by item_no
) as b on a.item_no=b.item_no
left join
(
select item_no,
       count(distinct so.order_name)  as pay_order_num,
			 count(distinct so.user_id)  as pay_user_num,
			 sum(sol.origin_qty)  as origin_qty,
			 sum(sol.origin_qty*sol.price_unit) as origin_amount
from jiayundw_dm.sale_order_info_df as so 
join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
where sol.is_delivery=0 and date(so.create_at+interval '8' hour) between '2020-04-08' and '2020-04-14' 
group by item_no 
) as c on a.item_no=c.item_no
WHERE illegal_tags like '%63%' and active=1
''')

data_hive = pd.read_sql(sql_hive, con_hive)
print(data_hive)

writer = pd.ExcelWriter('商品数据' + '.xlsx')
data_hive.to_excel(writer, sheet_name='商品数据', index=False)
os.chdir(r'/Users/apache/Downloads/A-python')
writer.save()
