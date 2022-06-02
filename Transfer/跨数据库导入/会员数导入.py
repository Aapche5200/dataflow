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
from sqlalchemy import create_engine


start = '2020-08-12'
con_hive = prestodb.dbapi.connect(
    host='ec2-54-68-88-224.us-west-2.compute.amazonaws.com',
    port=80,
    user='hadoop',
    catalog='hive',
    schema='default',
)

sql_hive = ('''										
select pay_ch_datepay_ch_date as "日期",
case when country_code='us' then '美国'
    when country_code='gb' then '英国'
    else null end "国家", 
count(distinct user_id) as "会员数"
from (
select  b.so_pay_ch_date as pay_ch_datepay_ch_date,
            a.country_code,
            a.is_wholee_mix,
            a.so_name as order_name,
            a.user_id,
            c.is_delivery,                              --是否商品单
            a.type as order_type,                       --0普通订单，1拼团订单，3 hiboss，4 b2b订单，5 wholee订单
            a.order_source,                             --订单来源：砍价bargain,spinner等,wholee_member购买会员身份
            c.item_no,
            c.product_id as pid,
            c.sku_id,
            c.real_price as price_real,
            c.origin_qty
    from    (
        select  date_id,so_name,user_id,type,order_source,country_code,is_wholee_mix
        from    dw_dwd.sale_order_order_df 
        where   date_id = '{0}'
            and is_valid = 1                              --是否有效单
            and is_test = 0                               --是否测试单
            and is_cheating = 0                           --是否欺诈单
            and type = '5'                                --wholee单（含会员单）
    ) a
    join    (
        select  date_id,so_name,so_pay_ch_date 
        from    dw_dwd.sale_payment_order_df 
        where   date_id = '{0}'
            and date(so_pay_ch_date) >= date('{0}') --interval '9' day--订单支付时间，中国时区
            and date(so_pay_ch_date) <= date('{0}')
    ) b on a.so_name = b.so_name and a.date_id = b.date_id
    left join (
        select  date_id,so_name,is_delivery,item_no,product_id,sku_id,real_price,origin_qty 
        from    dw_dwd.sale_order_order_line_df 
        where   date_id = '{0}'
    ) c on a.so_name = c.so_name and a.date_id = c.date_id
) as a
where 
is_delivery=6 
--order_source ='wholee_member' or 
--is_wholee_mix=1
group by 1,2

union all

select pay_ch_datepay_ch_date as "日期",
'整站'as "国家", 
count(distinct user_id) as "会员数"
from (
select  b.so_pay_ch_date as pay_ch_datepay_ch_date,
            a.country_code,
            a.is_wholee_mix,
            a.so_name as order_name,
            a.user_id,
            c.is_delivery,                              --是否商品单
            a.type as order_type,                       --0普通订单，1拼团订单，3 hiboss，4 b2b订单，5 wholee订单
            a.order_source,                             --订单来源：砍价bargain,spinner等,wholee_member购买会员身份
            c.item_no,
            c.product_id as pid,
            c.sku_id,
            c.real_price as price_real,
            c.origin_qty
    from    (
        select  date_id,so_name,user_id,type,order_source,country_code,is_wholee_mix
        from    dw_dwd.sale_order_order_df 
        where   date_id = '{0}'
            and is_valid = 1                              --是否有效单
            and is_test = 0                               --是否测试单
            and is_cheating = 0                           --是否欺诈单
            and type = '5'                                --wholee单（含会员单）
    ) a
    join    (
        select  date_id,so_name,so_pay_ch_date 
        from    dw_dwd.sale_payment_order_df 
        where   date_id = '{0}'
            and date(so_pay_ch_date) >= date('{0}') --interval '9' day--订单支付时间，中国时区
            and date(so_pay_ch_date) <= date('{0}')
    ) b on a.so_name = b.so_name and a.date_id = b.date_id
    left join (
        select  date_id,so_name,is_delivery,item_no,product_id,sku_id,real_price,origin_qty 
        from    dw_dwd.sale_order_order_line_df 
        where   date_id = '{0}'
    ) c on a.so_name = c.so_name and a.date_id = c.date_id
) as a
where 
is_delivery=6 
--order_source ='wholee_member' or 
--is_wholee_mix=1
group by 1
''').format(start)

cursor = con_hive.cursor()
cursor.execute(sql_hive)
data = cursor.fetchall()
column_descriptions = cursor.description
if data:
    df_hive = pd.DataFrame(data)
    df_hive.columns = [c[0] for c in column_descriptions]
else:
    df_hive = pd.DataFrame()

engine_ms = create_engine("mssql+pymssql://sa:yssshushan2008@172.16.92.2:1433/CFflows?charset=utf8")
df_hive.to_sql('UserPrime', con=engine_ms, if_exists='append', index=False)

print(df_hive)

sql_ms = ('''
select top 10 日期 from CFflows.dbo.UserPrime
order by 日期 desc
''')

df_ms = pd.read_sql(sql_ms, engine_ms)
print(df_ms)
