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
select a.*,
"首页支付订单量","category页支付订单量","购物车支付订单量","个人中心支付订单量","商详页支付订单量","搜索结果页支付订单量",
"首页买家数","category页买家数","购物车买家数","个人中心买家数","商详页买家数","搜索结果页买家数",
"首页销量","category页销量","购物车销量","个人中心销量","商详页销量","搜索结果页销量",
"首页销售","category页销售","购物车销售","个人中心销售","商详页销售","搜索结果页销售",
case when "首页曝光uv"=0 then 0 else cast("首页买家数" as double)/cast("首页曝光uv" as double) end "首页cvr",
case when "category页曝光uv"=0 then 0 else cast("category页买家数" as double)/cast("category页曝光uv" as double) end "category页cvr",
case when "购物车曝光uv"=0 then 0 else cast("购物车买家数" as double)/cast("购物车曝光uv" as double) end "购物车cvr",
case when "个人中心曝光uv"=0 then 0 else cast("个人中心买家数" as double)/cast("个人中心曝光uv" as double) end "个人中心cvr",
case when "商详页曝光uv"=0 then 0 else cast("商详页买家数" as double)/cast("商详页曝光uv" as double) end "商详页cvr",
case when "搜索结果页曝光uv"=0 then 0 else cast("搜索结果页买家数" as double)/cast("搜索结果页曝光uv" as double) end "搜索结果页cvr"
from
(
select
    date(server_time + interval '8'hour ) as event_date,
    front_cate_one_en as front_cate_one,
    count(distinct case when mid='8.1.9.1' and event_type ='product' and pid is not null then cid else null end) "首页曝光uv",
    count(distinct case when mid='8.8.9.1' and event_type ='product' and pid is not null then cid else null end) "category页曝光uv",
    count(distinct case when mid='8.3.9.1' and event_type ='product' and pid is not null then cid else null end) "购物车曝光uv",
    count(distinct case when mid='8.4.9.1' and event_type ='product' and pid is not null then cid else null end) "个人中心曝光uv",
    count(distinct case when mid='8.5.9.1' and event_type ='product' and pid is not null then cid else null end) "商详页曝光uv",
    count(distinct case when mid='8.15.9.1' and event_type ='product' and pid is not null then cid else null end) "搜索结果页曝光uv",

    count(distinct case when mid='8.1.9.1' and event_type ='click' and pid is not null then cid else null end) "首页点击uv",
    count(distinct case when mid='8.8.9.1' and event_type ='click' and pid is not null then cid else null end) "category页点击uv",
    count(distinct case when mid='8.3.9.1' and event_type ='click' and pid is not null then cid else null end) "购物车点击uv",
    count(distinct case when mid='8.4.9.1' and event_type ='click' and pid is not null then cid else null end) "个人中心点击uv",
    count(distinct case when mid='8.5.9.1' and event_type ='click' and pid is not null then cid else null end) "商详页点击uv",
    count(distinct case when mid='8.15.9.1' and event_type ='click' and pid is not null then cid else null end) "搜索结果页点击uv"
from ods_kafka.user_trace_prod as ua
join dw_dim.product_basic_info_df as pro on ua.pid=pro.product_id
where date(ua.date_id) between date('{0}')- interval '1' day and date('{0}')
and date(server_time + interval '8' hour) between date('{0}') and date('{0}')
and pro.date_id = '{0}'
and pro.illegal_tags like '%78%'
and event_type in ('click','product')
and ua.pid is not null
and ua.mid like '8.%.9.1'
group by 1,2
) as a
left join
(
select pay_ch_datepay_ch_date as event_date,
a.front_cate_one_en as front_cate_one,
    count(distinct case when mid='8.1.9.1' then order_name else null end) "首页支付订单量",
    count(distinct case when mid='8.8.9.1' then order_name else null end) "category页支付订单量",
    count(distinct case when mid='8.3.9.1' then order_name else null end) "购物车支付订单量",
    count(distinct case when mid='8.4.9.1' then order_name else null end) "个人中心支付订单量",
    count(distinct case when mid='8.5.9.1' then order_name else null end) "商详页支付订单量",
    count(distinct case when mid='8.15.9.1' then order_name else null end) "搜索结果页支付订单量",

    count(distinct case when mid='8.1.9.1' then user_id else null end) "首页买家数",
    count(distinct case when mid='8.8.9.1' then user_id else null end) "category页买家数",
    count(distinct case when mid='8.3.9.1' then user_id else null end) "购物车买家数",
    count(distinct case when mid='8.4.9.1' then user_id else null end) "个人中心买家数",
    count(distinct case when mid='8.5.9.1' then user_id else null end) "商详页买家数",
    count(distinct case when mid='8.15.9.1' then user_id else null end) "搜索结果页买家数",

    sum(case when mid='8.1.9.1' then origin_qty else null end) "首页销量",
    sum(case when mid='8.8.9.1' then origin_qty else null end) "category页销量",
    sum(case when mid='8.3.9.1' then origin_qty else null end) "购物车销量",
    sum(case when mid='8.4.9.1' then origin_qty else null end) "个人中心销量",
    sum(case when mid='8.5.9.1' then origin_qty else null end) "商详页销量",
    sum(case when mid='8.15.9.1' then origin_qty else null end) "搜索结果页销量",

    sum(case when mid='8.1.9.1'  then origin_qty*price_real else null end) "首页销售",
    sum(case when mid='8.8.9.1' then origin_qty*price_real else null end) "category页销售",
    sum(case when mid='8.3.9.1'  then origin_qty*price_real else null end) "购物车销售",
    sum(case when mid='8.4.9.1'  then origin_qty*price_real else null end) "个人中心销售",
    sum(case when mid='8.5.9.1'  then origin_qty*price_real else null end) "商详页销售",
    sum(case when mid='8.15.9.1'  then origin_qty*price_real else null end) "搜索结果页销售"
from
(
select a.*,b.*
from
(
select  b.so_pay_ch_date as pay_ch_datepay_ch_date,
            a.create_ch_time,
            b.so_pay_ch_time,
            a.so_name as order_name,
            a.user_id,
            c.is_delivery,                              --是否商品单
            a.type as order_type,                       --0普通订单，1拼团订单，3 hiboss，4 b2b订单，5 wholee订单
            a.order_source,                             --订单来源：砍价bargain,spinner等,wholee_member购买会员身份
            c.item_no,
            d.front_cate_one_en,
            d.front_cate_two_en,
            d.front_cate_three_en,
            d.cate_one_en,
            d.cate_two_en,
            d.cate_three_en,
            c.product_id as pid,
            c.sku_id,
            c.real_price as price_real,
            c.origin_qty
    from    (
        select  date_id,so_name,user_id,type,order_source,create_ch_date,MAX(create_ch_time) as create_ch_time
        from    dw_dwd.sale_order_order_df
        where   date_id = '{0}'
            and date(create_ch_date)=date('{0}')
            and is_valid = 1                              --是否有效单
            and is_test = 0                               --是否测试单
            and is_cheating = 0                           --是否欺诈单
            and type = '5'                                --wholee单（含会员单）
        group by date_id,so_name,user_id,type,order_source,create_ch_date
    ) a
    join    (
        select  date_id,so_name,so_pay_ch_date,so_pay_ch_time
        from    dw_dwd.sale_payment_order_df
        where   date_id = '{0}'
            and date(so_pay_ch_date) >= date('{0}') --interval '7' day--订单支付时间，中国时区
            and date(so_pay_ch_date) <= date('{0}')
    ) b on a.so_name = b.so_name and a.date_id = b.date_id
    left join (
        select  date_id,so_name,is_delivery,item_no,product_id,sku_id,real_price,origin_qty
        from    dw_dwd.sale_order_order_line_df
        where   date_id = '{0}'
    ) c on a.so_name = c.so_name and a.date_id = c.date_id
    left join (
        select  *
        from    dw_dim.product_basic_info_df
        where   date_id = '{0}'
    ) d on c.item_no = d.item_no --商品宽表
) as a
join
(
        select
        min(server_time + interval '8' hour) as server_time,
        mid,
        b.uid,
        pid,
        event_type
        from ods_kafka.user_trace_prod as a
        left join analysts.tb_kxw_every_day_cid_uid as b on a.cid=b.cid
        where date(a.date_id) between date('{0}')- interval '1' day and date('{0}')
            and date(a.server_time + interval '8' hour) between date('{0}') and date('{0}')
            and b.pt='{0}'
            and a.pid is not null
            and mid  like '8.%.9.1'
            and event_type='click'
        group by mid,b.uid,pid,event_type
) as b on a.pid=b.pid  and a.user_id=b.uid
where a.create_ch_time>=b.server_time
    and a.item_no is not null
    and is_delivery=0
    and order_source is null
) as a
group by 1,2
) as b on a.event_date=b.event_date and a.front_cate_one=b.front_cate_one
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

print(df_hive)

engine_ms = create_engine(
    "mssql+pymssql://sa:yssshushan2008@172.16.92.2:1433/CFflows?charset=utf8")
df_hive.to_sql('CategoryPages', con=engine_ms, if_exists='append', index=False)

sql_cate_pages = ('''
select top 10 * from CFflows.dbo.CategoryPages
order by event_date DESC
''')

data_cate_pages = pd.read_sql(sql_cate_pages, engine_ms)
print(data_cate_pages)
