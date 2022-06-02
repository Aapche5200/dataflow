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

start = '2020-05-01'
end = '2020-05-30'

sql_hive = ('''
select 
a.front_cate_one as "前台一级类目",
a.yezi_num as "叶子类目数",
a.item_num as "在架商品数",
"4_num_l" as "质量分>=4商品数",
"2_num_l" as "质量分<=2商品数",
"4_num_7d" as "近7天质量分>=4商品数",
"2_num_7d" as "近7天质量分<=2商品数",
cast("2_num_7d" as double)/(cast("2_num_7d" as double)+cast("4_num_7d" as double)) as "近7天质量分<=2占比",
cast("2_num_l" as double)/(cast("2_num_l" as double)+cast("4_num_l" as double)) as "质量分<=2占比",
pay_item_num as "近30天支付商品数",
origin_qty "近30天销量",
"7d_qty" as "近7天销量",
(cast(pay_item_num as double)/cast(item_num as double)) as "近30天动销率",
(cast("7d_qty" as double)/cast(impression_num as double)) as "近7天曝光转化",
(cast("7d_pay_user" as double)/cast(uv as double)) as "近7天支付转化"
from
    (
        select front_cate_one,
        count(distinct case when front_cate_three='' then front_cate_two  else null end)+
        count(distinct front_cate_three) as yezi_num,
        count(distinct item_no) as item_num,
        count(distinct(case when quality>=4 then item_no else null end)) as "4_num_l",
        count(distinct(case when quality<=2 then item_no else null end)) as "2_num_l",
        count(distinct(case when "7d_quality">=4 then item_no else null end)) as "4_num_7d",
        count(distinct(case when "7d_quality"<=2 then item_no else null end)) as "2_num_7d"
        from jiayundw_dim.product_basic_info_df a
        where active=1
        group by 1
    ) as a
left join
    (
        select
        a.front_cate_one,
        count(distinct sol.item_no)  as pay_item_num,
        sum(sol.origin_qty)  as origin_qty,
        sum(case when date(so.create_at+interval '8' hour) between date(current_date- interval '7' day) and date(current_date- interval '1' day)  
            then sol.origin_qty else null end) as "7d_qty",
        count(distinct(case when date(so.create_at+interval '8' hour) between date(current_date- interval '7' day) and date(current_date- interval '1' day)  
            then so.user_id else null end)) as "7d_pay_user"
        from jiayundw_dm.sale_order_info_history_df as so 
            join jiayundw_dm.sale_order_line_history_df as sol on sol.order_name=so.order_name
            join jiayundw_dim.product_basic_info_df as a on sol.item_no=a.item_no
            where sol.is_delivery=0 and 
                date(so.create_at+interval '8' hour) 
                    between date(current_date- interval '30' day) and date(current_date- interval '1' day) 
                and date(sol.date_id) =date(current_date- interval '1' day)  
                and date(so.date_id)=date(current_date- interval '1' day) 
            group by a.front_cate_one
    ) as b  on a.front_cate_one=b.front_cate_one
left join
    (
        select 
        front_cate_one,
        count(distinct case when event_type in ('product') and mid='1.5.1.1' then cid else null end) as uv,
        count(case when  event_type='product' and (mid like '%.9.1' or mid like '%.9.2') then cid else null end) as impression_num
        from jiayundw_dws.flow_pid_action_di as ua 
            join jiayundw_dim.product_basic_info_df as pro on cast(pro.pid as varchar) =ua.pid
            where date(ua.log_date) between date(current_date- interval '7' day) and date(current_date- interval '1' day)  
            group by front_cate_one
    ) as c on a.front_cate_one=c.front_cate_one

''').format(start, end)


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

writer = pd.ExcelWriter('类目体检' + '.xlsx')
data_hive.to_excel(writer, sheet_name='一级类目', index=False)
os.chdir(r'/Users/apache/Downloads/A-python')
writer.save()
