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
SELECT DISTINCT a.item_no,cate_one_cn,cate_two_cn,cate_three_cn,b.log_date,illegal_tags,write_uid,
uv ,click_num ,impression_num ,origin_qty,pay_user_num,origin_amount,state 
from jiayundw_dim.product_basic_info_df a
left join
(
 select item_no,ua.log_date,
               count(distinct case when event_type in ('product','impression') and mid='1.5.1.1' then cid else null end) as uv,
               count(case when  event_type='product' and mid like '%.9.1' then cid else null end) as impression_num,
               count(case when  event_type='click' and mid like '%.9.1' then cid else null end) as click_num
        from jiayundw_dws.flow_pid_action_di as ua 
        join jiayundw_dim.product_basic_info_df as pro on pro.pid=ua.pid
where ua.log_date between '2020-04-10' and '2020-04-10' 
group by item_no,ua.log_date
) as b on a.item_no=b.item_no
left join
(
select item_no,
       case when so.channel = 'cod' then 'cod' else 'feicod'  end state,
       date(so.create_at+interval '8' hour) as log_date,
       count(distinct so.order_name)  as pay_order_num,
       count(distinct so.user_id)  as pay_user_num,
       sum(sol.origin_qty)  as origin_qty,
       sum(sol.origin_qty*sol.price_unit) as origin_amount
from jiayundw_dm.sale_order_info_df as so 
join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
where sol.is_delivery=0 and date(so.create_at+interval '8' hour) between '2020-04-10' and '2020-04-10' 
group by item_no,
       case when so.channel = 'cod' then 'cod' else 'feicod'  end,
       date(so.create_at+interval '8' hour)   
) as c on b.item_no=c.item_no and b.log_date=c.log_date
WHERE a.item_no in (
'HPC005054083N',
'HPC003952216N',
'HPC003304614N',
'HPC012142979N',
'HPC013200225N',
'HHA009305158N',
'HPC009420630N',
'HPC009052278N',
'HPC009977339N',
'HSA011102078N',
'HHA010707263N',
'HPC003967624N',
'HPC003967688N',
'HPC010665664N',
'HPC009471360N',
'HPC009915249N',
'HPC012453223N',
'HPC009092083N',
'HKA003970074N',
'HPC003304516N',
'HPC003429642N',
'ESD012606864N',
'HPC012331706N',
'HPC010687806N',
'HPC004087093N',
'HPC004983921N',
'HPC009091859N',
'HPC010681269N',
'HPC004009999N',
'HPC008018147N',
'HSS006994604N',
'HPC003780894N',
'HPC008625767N',
'HPC009088751N',
'HPC008775419N',
'HPC011468215N',
'HPC009922607N',
'SMS009266703N',
'SMS009266705N',
'SMS009266706N',
'SMS009267174N',
'SMS009267176N',
'SMS009267479N',
'SMS009267481N',
'SWS006045683N',
'SWS006045684N',
'SWS006045685N',
'SWS006045686N',
'SWS006045687N',
'SWS006045688N',
'SWS006045689N',
'SWS006045690N',
'SWS006045691N',
'SWS006045692N',
'SWS006045695N',
'SWS006045696N',
'SWS006045697N',
'SWS006045698N',
'SWS006045699N',
'SWS006045700N',
'SWS006045701N',
'SWS006045702N',
'SWS006045703N',
'SWS006045704N',
'SWS006045705N',
'SWS006045706N',
'SWS006045707N',
'SWS006045708N',
'SWS006045710N',
'SWS006045713N',
'SWS006045714N',
'SWS006045879N',
'SWS006045881N',
'SWS006110859N',
'SWS006110861N',
'SWS006110863N',
'SWS006110864N',
'SWS006308168N',
'SWS006308172N',
'SWS006308179N',
'SWS006308182N',
'SWS006309762N',
'MTT010971706N',
'MTT004242051N',
'MTT005642896N',
'MTT003952944N',
'MTT012495731N',
'MTT004035466N',
'MTT003446878N',
'MTT004215695N',
'MTT004069856N',
'MTT003446884N'

)
''')

data_hive = pd.read_sql(sql_hive, con_hive)


writer = pd.ExcelWriter('cod及非cod验证-打标后' + '.xlsx')
data_hive.to_excel(writer, sheet_name='商品数据', index=False)
os.chdir(r'/Users/apache/Downloads/A-python')
writer.save()
