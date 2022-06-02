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

start = '2020-08-29'
con_hive = prestodb.dbapi.connect(
    host='ec2-54-68-88-224.us-west-2.compute.amazonaws.com',
    port=80,
    user='hadoop',
    catalog='hive',
    schema='default',
)

sql_hive = """
select
c.event_date,
a.seller_type,
c.front_cate_one,
null as front_cate_two,
null as front_cate_three,
a.item_num,
c.uv,
c.impression_num,
c.click_num,
b.pay_order_num,
b.pay_user_num,
b.origin_qty,
b.origin_amount
from
    (
select   date(server_time + interval '8' hour) as event_date,
         front_cate_one_en as front_cate_one,
         count(distinct case when event_type='product' and mid='8.5.1.1' then cid else null end) as uv,
         sum(case when  event_type='product'  and (mid like '%.9.1' or mid like '%.9.2')  then 1 else 0 end) as impression_num,
         sum(case when  event_type='click' and (mid like '%.9.1' or mid like '%.9.2')  then 1 else 0 end) as click_num,
         sum(case when  event_type='click' and mid='8.5.4.4' then 1 else 0 end) as add_num,
         0 as wishlist_num
from ods_kafka.user_trace_prod ua
join dw_dim.product_basic_info_df as pro on pro.product_id=ua.pid
where  date(ua.date_id)>=date('{0}')- interval '1' day
and date(ua.date_id)<=date('{0}')
and date(pro.date_id)=date('{0}')
and date(server_time + interval '8' hour)=date('{0}')
and pid is not null
and mid like '8.%'
and illegal_tags like '%78%'
group by date(server_time + interval '8' hour),front_cate_one_en
    ) as c
left join
    (
        select
        'total' as seller_type,
        front_cate_one_en as front_cate_one,
        count(distinct item_no) as item_num
        from
            dw_dim.product_basic_info_df
            where active_status=1 and date_id='{0}' and illegal_tags like '%78%'
            group by front_cate_one_en
    ) as a on a.front_cate_one=c.front_cate_one
left join
    (
select pay_ch_datepay_ch_date as event_date,front_cate_one_en as front_cate_one,count(distinct order_name) as pay_order_num, count(distinct user_id) as pay_user_num,sum(origin_qty) as origin_qty,sum(origin_qty*price_real) as origin_amount
from (
select  b.so_pay_ch_date as pay_ch_datepay_ch_date,
            a.so_name as order_name,
            a.user_id,
            d.is_delivery,                              --是否商品单
            a.type as order_type,                       --0普通订单，1拼团订单，3 hiboss，4 b2b订单，5 wholee订单
            a.order_source,                             --订单来源：砍价bargain,spinner等,wholee_member购买会员身份
            d.item_no,
            e.front_cate_one_en,
            e.front_cate_two_en,
            e.front_cate_three_en,
            e.cate_one_en,
            e.cate_two_en,
            e.cate_three_en,
            d.product_id as pid,
            d.sku_id,
            d.real_price as price_real,
            d.origin_qty,
            c.cod_fee as order_cod_fee,
            c.shipping_fee as order_shipping_fee,
            c.tax_fee as order_tax_fee,
            a.amount as order_amount --订单支付金额 单位美元
    from    (
        select  date_id,so_name,user_id,type,order_source,amount
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
            and date(so_pay_ch_date) >= date('{0}')-- interval '7' day--订单支付时间，中国时区
            and date(so_pay_ch_date) <= date('{0}')
    ) b on a.so_name = b.so_name and a.date_id = b.date_id
    left join (
        select  date_id,so_name,cod_fee,shipping_fee,tax_fee
        from    dw_dwd.sale_order_order_fee_df
        where   date_id = '{0}'
    ) c on a.so_name = c.so_name and a.date_id = c.date_id
    left join (
        select  date_id,so_name,is_delivery,item_no,product_id,sku_id,real_price,origin_qty
        from    dw_dwd.sale_order_order_line_df
        where   date_id = '{0}'
    ) d on a.so_name = d.so_name and a.date_id = d.date_id
    left join (
        select  *
        from    dw_dim.product_basic_info_df
        where   date_id = '{0}'
    ) e on d.item_no = e.item_no --商品宽表
) as a
where is_delivery=0 and order_source is null
group by 1,2
    ) as b on c.front_cate_one=b.front_cate_one


union all

select
c.event_date,
a.seller_type,
c.front_cate_one,
null as front_cate_two,
null as front_cate_three,
a.item_num,
c.uv,
c.impression_num,
c.click_num,
b.pay_order_num,
b.pay_user_num,
b.origin_qty,
b.origin_amount
from
    (
select   date(server_time + interval '8' hour) as event_date,
         '全类目' as front_cate_one,
         count(distinct case when event_type='product' and mid='8.5.1.1' then cid else null end) as uv,
         sum(case when  event_type='product'  and (mid like '%.9.1' or mid like '%.9.2')  then 1 else 0 end) as impression_num,
         sum(case when  event_type='click' and (mid like '%.9.1' or mid like '%.9.2')  then 1 else 0 end) as click_num,
         sum(case when  event_type='click' and mid='8.5.4.4' then 1 else 0 end) as add_num,
         0 as wishlist_num
from ods_kafka.user_trace_prod ua
join dw_dim.product_basic_info_df as pro on pro.product_id=ua.pid
where  date(ua.date_id)>=date('{0}')- interval '1' day
and date(ua.date_id)<=date('{0}')
and date(pro.date_id)=date('{0}')
and date(server_time + interval '8' hour)=date('{0}')
and pid is not null
and mid like '8.%'
and illegal_tags like '%78%'
group by date(server_time + interval '8' hour)
    ) as c
left join
    (
        select
        'total' as seller_type,
        '全类目' as front_cate_one,
        count(distinct item_no) as item_num
        from
            dw_dim.product_basic_info_df as dd
            where active_status=1 and date_id='{0}' and illegal_tags like '%78%'
    ) as a on a.front_cate_one=c.front_cate_one
left join
    (
select pay_ch_datepay_ch_date as event_date,'全类目' as front_cate_one,count(distinct order_name) as pay_order_num, count(distinct user_id) as pay_user_num,sum(origin_qty) as origin_qty,sum(origin_qty*price_real) as origin_amount
from (
select  b.so_pay_ch_date as pay_ch_datepay_ch_date,
            a.so_name as order_name,
            a.user_id,
            d.is_delivery,                              --是否商品单
            a.type as order_type,                       --0普通订单，1拼团订单，3 hiboss，4 b2b订单，5 wholee订单
            a.order_source,                             --订单来源：砍价bargain,spinner等,wholee_member购买会员身份
            d.item_no,
            e.front_cate_one_en,
            e.front_cate_two_en,
            e.front_cate_three_en,
            e.cate_one_en,
            e.cate_two_en,
            e.cate_three_en,
            d.product_id as pid,
            d.sku_id,
            d.real_price as price_real,
            d.origin_qty,
            c.cod_fee as order_cod_fee,
            c.shipping_fee as order_shipping_fee,
            c.tax_fee as order_tax_fee,
            a.amount as order_amount --订单支付金额 单位美元
    from    (
        select  date_id,so_name,user_id,type,order_source,amount
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
            and date(so_pay_ch_date) >= date('{0}')-- interval '7' day--订单支付时间，中国时区
            and date(so_pay_ch_date) <= date('{0}')
    ) b on a.so_name = b.so_name and a.date_id = b.date_id
    left join (
        select  date_id,so_name,cod_fee,shipping_fee,tax_fee
        from    dw_dwd.sale_order_order_fee_df
        where   date_id = '{0}'
    ) c on a.so_name = c.so_name and a.date_id = c.date_id
    left join (
        select  date_id,so_name,is_delivery,item_no,product_id,sku_id,real_price,origin_qty
        from    dw_dwd.sale_order_order_line_df
        where   date_id = '{0}'
    ) d on a.so_name = d.so_name and a.date_id = d.date_id
    left join (
        select  *
        from    dw_dim.product_basic_info_df
        where   date_id = '{0}'
    ) e on d.item_no = e.item_no --商品宽表
) as a
where is_delivery=0 and order_source is null
group by 1
    ) as b on c.front_cate_one=b.front_cate_one

""".format(start)

cursor = con_hive.cursor()
cursor.execute(sql_hive)
data = cursor.fetchall()
column_descriptions = cursor.description
if data:
    data_hive = pd.DataFrame(data)
    data_hive.columns = [c[0] for c in column_descriptions]
else:
    data_hive = pd.DataFrame()

data_hive['支付转化率'] = data_hive['pay_user_num'] / data_hive['uv']
data_hive['曝光转化率'] = data_hive['origin_qty'] / data_hive['impression_num']
data_hive['zhou'] = ''
data_hive['front_one'] = ''
data_hive['曝光商品数'] = ''
data_hive['点击数'] = ''
data_hive['男买家数'] = ''
data_hive['女买家数'] = ''
data_hive['动销率'] = ''
data_hive['支付商品数'] = ''
data_hive['印度在售商品数'] = ''
data_hive['新增商品数'] = ''
data_hive['月累积支付商品数'] = ''
data_hive['下单量'] = ''
data_hive['下单件数'] = ''
data_hive['下单买家数'] = ''
data_hive['退货数量'] = ''
data_hive['退货金额'] = ''
data_hive['下单转化率'] = ''
data_hive['新买家数'] = ''
data_hive['女访客'] = ''
data_hive['男访客'] = ''
data_hive['平均曝光数'] = ''
data_hive['缺货率'] = ''
data_hive['客单价'] = ''
data_hive['KA缺货率'] = ''
data_hive['盲采缺货率'] = ''

data_hive.rename(
    columns={'zhou': '周',
             'event_date': '日期',
             'seller_type': '业务类型',
             'front_cate_one': '一级类目',
             'front_cate_two': '二级类目',
             'front_cate_three': '三级类目',
             'front_one': '前台一级类目',
             'impression_num': '曝光数量',
             '曝光商品数': '曝光商品数',
             'click_num': '点击数量',
             'uv': '独立访客',
             'pay_order_num': '支付订单量',
             'pay_user_num': '买家数',
             '男买家数': '男买家数',
             '女买家数': '女买家数',
             '支付转化率': '支付转化率',
             '曝光转化率': '曝光转化率',
             '动销率': '动销率',
             'origin_amount': '支付金额',
             'origin_qty': '支付商品件数',
             '支付商品数': '支付商品数',
             'item_num': '在售商品数',
             '印度在售商品数': '印度在售商品数',
             '新增商品数': '新增商品数',
             '月累积支付商品数': '月累积支付商品数',
             '下单量': '下单量',
             '下单件数': '下单件数',
             '下单买家数': '下单买家数',
             '退货数量': '退货数量',
             '退货金额': '退货金额',
             '下单转化率': '下单转化率',
             '新买家数': '新买家数',
             '女访客': '女访客',
             '男访客': '男访客',
             '平均曝光数': '平均曝光数',
             '缺货率': '缺货率',
             '客单价': '客单价',
             'KA缺货率': 'KA缺货率',
             '盲采缺货率': '盲采缺货率',
             },
    inplace=True)

order = [
    '周',
    '日期',
    '业务类型',
    '一级类目',
    '二级类目',
    '三级类目',
    '前台一级类目',
    '曝光数量',
    '曝光商品数',
    '点击数量',
    '独立访客',
    '支付订单量',
    '买家数',
    '男买家数',
    '女买家数',
    '支付转化率',
    '曝光转化率',
    '动销率',
    '支付金额',
    '支付商品件数',
    '支付商品数',
    '在售商品数',
    '印度在售商品数',
    '新增商品数',
    '月累积支付商品数',
    '下单量',
    '下单件数',
    '下单买家数',
    '退货数量',
    '退货金额',
    '下单转化率',
    '新买家数',
    '女访客',
    '男访客',
    '平均曝光数',
    '缺货率',
    '客单价',
    'KA缺货率',
    '盲采缺货率',
]
data_hive = data_hive[order]
data_hive['周'] = 0
data_hive['曝光商品数'] = 0
data_hive['男买家数'] = 0
data_hive['女买家数'] = 0
data_hive['动销率'] = 0
data_hive['支付商品数'] = 0
data_hive['印度在售商品数'] = 0
data_hive['新增商品数'] = 0
data_hive['月累积支付商品数'] = 0
data_hive['下单量'] = 0
data_hive['下单件数'] = 0
data_hive['下单买家数'] = 0
data_hive['退货数量'] = 0
data_hive['退货金额'] = 0
data_hive['下单转化率'] = 0
data_hive['新买家数'] = 0
data_hive['女访客'] = 0
data_hive['男访客'] = 0
data_hive['平均曝光数'] = 0
data_hive['缺货率'] = 0
data_hive['客单价'] = 0
data_hive['KA缺货率'] = 0
data_hive['盲采缺货率'] = 0
print(data_hive)

engine_ms = create_engine(
    "mysql+pymysql://root:yssshushan2008@127.0.0.1:3306/CFcategory?charset=utf8")
data_hive.to_sql(
    'category_123_day_wholee',
    con=engine_ms,
    if_exists='append',
    index=False)

sql_ms = ('''
select * from CFcategory.category_123_day_wholee
order by 日期 desc
limit 10
''')

df_ms = pd.read_sql(sql_ms, engine_ms)
print(df_ms)
