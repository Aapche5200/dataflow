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

start = '2020-06-17'
con_hive = prestodb.dbapi.connect(
    host='ec2-54-68-88-224.us-west-2.compute.amazonaws.com',
    port=80,
    user='hadoop',
    catalog='hive',
    schema='default',
)

sql_hive = """
select 
a.event_date,
c.seller_type, 
c.front_cate_one,
c.front_cate_two,
c.front_cate_three,
c.item_num,
a.uv,
a.impression_num,
a.click_num,
b.pay_order_num,
b.pay_user_num,
b.origin_qty,
b.origin_amount 
from
    (
        select  
        'total' as seller_type, 
        front_cate_one,
        front_cate_two,
        front_cate_three,
        count(distinct item_no) as item_num
        from
            jiayundw_dim.product_basic_info_df
            where active=1
            group by 2,3,4
    ) as c 
left join
    (
        select 
        ua.log_date event_date,
        front_cate_one,
        front_cate_two,
        front_cate_three,
        count(distinct case when event_type in ('product') and mid='1.5.1.1' then cid else null end) as uv,
        count(case when  event_type='product' and (mid like '%.9.1' or mid like '%.9.2') then cid else null end) as impression_num,
        count(case when  event_type='click' and (mid like '%.9.1' or mid like '%.9.2') then cid else null end) as click_num
        from jiayundw_dws.flow_pid_action_di as ua 
            join jiayundw_dim.product_basic_info_df as pro on cast(pro.pid as varchar) =ua.pid
            where ua.log_date between ('{0}') and ('{0}')
            group by ua.log_date,front_cate_one,front_cate_two,front_cate_three
    ) as a on a.front_cate_one=c.front_cate_one and 
                a.front_cate_two=c.front_cate_two and 
                a.front_cate_three=c.front_cate_three
left join 
    (
        select
        date(so.create_at+interval '8' hour) event_date, 
        a.front_cate_one,
        a.front_cate_two,
        a.front_cate_three,
        count(distinct so.order_name)  as pay_order_num,
        count(distinct so.user_id)  as pay_user_num,
        sum(sol.origin_qty)  as origin_qty,
        sum(sol.origin_qty*sol.price_unit) as origin_amount
        from jiayundw_dm.sale_order_info_history_df as so 
            join jiayundw_dm.sale_order_line_history_df as sol on sol.order_name=so.order_name
            join jiayundw_dim.product_basic_info_df as a on sol.item_no=a.item_no
            where sol.is_delivery=0 and 
                date(so.create_at+interval '8' hour) between date('{0}') and date('{0}') 
                and sol.date_id ='{0}'  and so.date_id='{0}'
            group by date(so.create_at+interval '8' hour),a.front_cate_one,
                    a.front_cate_two,
                    a.front_cate_three
    ) as b on c.front_cate_one=b.front_cate_one and
            c.front_cate_two=b.front_cate_two and
            c.front_cate_three=b.front_cate_three
where c.front_cate_three !=''

union all

select 
a.event_date,
c.seller_type, 
c.front_cate_one,
c.front_cate_two,
c.front_cate_three,
c.item_num,
a.uv,
a.impression_num,
a.click_num,
b.pay_order_num,
b.pay_user_num,
b.origin_qty,
b.origin_amount 
from
    (
        select  
        'seller' as seller_type, 
        front_cate_one,
        front_cate_two,
        front_cate_three,
        count(distinct item_no) as item_num
        from
            jiayundw_dim.product_basic_info_df
            where active=1 and write_uid=5
            group by 2,3,4
    ) as c 
left join
    (
        select 
        ua.log_date event_date,
        'seller' as seller_type, 
        front_cate_one,
        front_cate_two,
        front_cate_three,
        count(distinct case when event_type in ('product','impression') and mid='1.5.1.1' then cid else null end) as uv,
        count(case when  event_type='product' and mid like '%.9.1' then cid else null end) as impression_num,
        count(case when  event_type='click' and mid like '%.9.1' then cid else null end) as click_num
        from 
            jiayundw_dws.flow_pid_action_di as ua 
            join jiayundw_dim.product_basic_info_df as pro on cast(pro.pid as varchar) =ua.pid
            where ua.log_date between ('{0}') and ('{0}') and write_uid=5
            group by ua.log_date,front_cate_one,front_cate_two,front_cate_three
    ) as a on a.front_cate_one=c.front_cate_one and a.seller_type=c.seller_type and a.front_cate_two=c.front_cate_two
            and a.front_cate_three=c.front_cate_three
left join 
    (
        select 
        date(so.create_at+interval '8' hour) event_date,
        'seller' as seller_type, 
        a.front_cate_one,
        a.front_cate_two,
        a.front_cate_three,
        count(distinct so.order_name)  as pay_order_num,
        count(distinct so.user_id)  as pay_user_num,
        sum(sol.origin_qty)  as origin_qty,
        sum(sol.origin_qty*sol.price_unit) as origin_amount
        from 
            jiayundw_dm.sale_order_info_history_df as so 
            join jiayundw_dm.sale_order_line_history_df as sol on sol.order_name=so.order_name
            join jiayundw_dim.product_basic_info_df as a on sol.item_no=a.item_no
            where sol.is_delivery=0 and 
                date(so.create_at+interval '8' hour) between date('{0}') and date('{0}')  and write_uid=5 
                and sol.date_id ='{0}'  and so.date_id='{0}'
            group by date(so.create_at+interval '8' hour),a.front_cate_one,a.front_cate_two,a.front_cate_three
    ) as b on c.front_cate_one=b.front_cate_one and c.seller_type=b.seller_type and c.front_cate_two=b.front_cate_two
            and c.front_cate_three=b.front_cate_three
where c.front_cate_three !=''

union all

select 
a.event_date,
c.seller_type, 
c.front_cate_one,
c.front_cate_two,
c.front_cate_three,
c.item_num,
a.uv,
a.impression_num,
a.click_num,
b.pay_order_num,
b.pay_user_num,
b.origin_qty,
b.origin_amount 
from
    (
        select  
        'cf' as seller_type, 
        front_cate_one,
        front_cate_two,
        front_cate_three,
        count(distinct item_no) as item_num
        from
            jiayundw_dim.product_basic_info_df
            where active=1 and write_uid!=5
            group by 2,3,4
    ) as c 
left join
    (
        select 
        ua.log_date event_date,
        'cf' as seller_type, 
        front_cate_one,
        front_cate_two,
        front_cate_three,
        count(distinct case when event_type in ('product','impression') and mid='1.5.1.1' then cid else null end) as uv,
        count(case when  event_type='product' and mid like '%.9.1' then cid else null end) as impression_num,
        count(case when  event_type='click' and mid like '%.9.1' then cid else null end) as click_num
        from 
            jiayundw_dws.flow_pid_action_di as ua 
            join jiayundw_dim.product_basic_info_df as pro on cast(pro.pid as varchar) =ua.pid
            where ua.log_date between ('{0}') and ('{0}') and write_uid!=5
            group by ua.log_date,front_cate_one,front_cate_two,front_cate_three
    ) as a on a.front_cate_one=c.front_cate_one and a.seller_type=c.seller_type and a.front_cate_two=c.front_cate_two
            and a.front_cate_three=c.front_cate_three
left join 
    (
        select 
        date(so.create_at+interval '8' hour) event_date,
        'cf' as seller_type, 
        a.front_cate_one,
        a.front_cate_two,
        a.front_cate_three,
        count(distinct so.order_name)  as pay_order_num,
        count(distinct so.user_id)  as pay_user_num,
        sum(sol.origin_qty)  as origin_qty,
        sum(sol.origin_qty*sol.price_unit) as origin_amount
        from 
            jiayundw_dm.sale_order_info_history_df as so 
            join jiayundw_dm.sale_order_line_history_df as sol on sol.order_name=so.order_name
            join jiayundw_dim.product_basic_info_df as a on sol.item_no=a.item_no
            where sol.is_delivery=0 and 
                date(so.create_at+interval '8' hour) between date('{0}') and date('{0}')  and write_uid!=5 
                and sol.date_id ='{0}'  and so.date_id='{0}'
            group by date(so.create_at+interval '8' hour) ,a.front_cate_one,a.front_cate_two,a.front_cate_three
    ) as b on c.front_cate_one=b.front_cate_one and c.seller_type=b.seller_type and c.front_cate_two=b.front_cate_two 
            and c.front_cate_three=b.front_cate_three
where c.front_cate_three !=''
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

order = ['周', '日期', '业务类型', '一级类目', '二级类目', '三级类目', '前台一级类目', '曝光数量', '曝光商品数',
         '点击数量', '独立访客', '支付订单量', '买家数', '男买家数', '女买家数', '支付转化率', '曝光转化率', '动销率',
         '支付金额', '支付商品件数', '支付商品数', '在售商品数', '印度在售商品数', '新增商品数', '月累积支付商品数',
         '下单量', '下单件数', '下单买家数', '退货数量', '退货金额', '下单转化率', '新买家数', '女访客', '男访客',
         '平均曝光数', '缺货率', '客单价', 'KA缺货率', '盲采缺货率', ]
data_hive = data_hive[order]

# data_hive['三级类目'].fillna(0, inplace=True)
print(data_hive)

engine_ms = create_engine("mssql+pymssql://sa:yssshushan2008@172.16.92.2:1433/CFcategory?charset=utf8")
data_hive.to_sql('category_123_day', con=engine_ms, if_exists='append', index=False)

sql_ms = ('''
select top 10 日期 from CFcategory.dbo.category_123_day
order by 日期 desc
''')

df_ms = pd.read_sql(sql_ms, engine_ms)
print(df_ms)
