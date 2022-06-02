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

start = '2020-07-14'
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
        select 
        ua.log_date event_date,
        '全类目' as front_cate_one,
        count(distinct case when event_type in ('product') and mid='1.5.1.1' then cid else null end) as uv,
        count(case when  event_type='product' and (mid like '%.9.1' or mid like '%.9.2') then cid else null end) as impression_num,
        count(case when  event_type='click' and (mid like '%.9.1' or mid like '%.9.2') then cid else null end) as click_num
        from jiayundw_dws.flow_pid_action_di as ua 
            join jiayundw_dim.product_basic_info_df as pro on cast(pro.pid as varchar)=ua.pid
            where ua.log_date between ('{0}') and ('{0}') and  ua.log_date = ('{0}')  and ua.pid is not null and event_type in ('product','click') 
            and (mid like '%.9.1'  or mid = '1.5.1.1' or mid like '%.9.2')
            group by ua.log_date
    ) as c 
left join
    (
        select  
        'total' as seller_type, 
        '全类目' as front_cate_one,
        count(distinct item_no) as item_num
        from
            jiayundw_dim.product_basic_info_df as dd
            where active=1 
    ) as a on a.front_cate_one=c.front_cate_one
left join 
    (
        select
        date(so.create_at+interval '8' hour) event_date, 
        '全类目' as front_cate_one,
        count(distinct so.order_name)  as pay_order_num,
        count(distinct so.user_id)  as pay_user_num,
        sum(sol.origin_qty)  as origin_qty,
        sum(sol.origin_qty*sol.price_real) as origin_amount
        from jiayundw_dm.sale_order_info_history_df as so 
            left join jiayundw_dm.sale_order_line_history_df as sol on sol.order_name=so.order_name
            left join jiayundw_dim.product_basic_info_df as a on sol.item_no=a.item_no
            where so.is_delivery=0 and 
                date(so.create_at+interval '8' hour) between date('{0}') and date('{0}') 
                and sol.date_id ='{0}'  and so.date_id='{0}'
		and date(sol.create_at+interval '8' hour) = date('{0}')
                    and so.is_consolidation <> 2
                    and cast(so.is_test as integer) = 0
                    and cast(so.is_cheating as integer) = 0
            group by date(so.create_at+interval '8' hour)
    ) as b on c.front_cate_one=b.front_cate_one

UNION ALL

select 
c.event_date,
c.seller_type, 
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
        select 
        ua.log_date event_date,
        case when write_uid=5 then 'seller' else 'cf' end seller_type, 
        '全类目' as front_cate_one,
        count(distinct case when event_type in ('product') and mid='1.5.1.1' then cid else null end) as uv,
        count(case when  event_type='product' and (mid like '%.9.1' or mid like '%.9.2') then cid else null end) as impression_num,
        count(case when  event_type='click' and (mid like '%.9.1' or mid like '%.9.2') then cid else null end) as click_num
        from 
            jiayundw_dws.flow_pid_action_di as ua 
            join jiayundw_dim.product_basic_info_df as pro on cast(pro.pid as varchar) =ua.pid
            where ua.log_date between ('{0}') and ('{0}') and  ua.log_date = ('{0}')  and ua.pid is not null and event_type in ('product','click') 
            and (mid like '%.9.1'  or mid = '1.5.1.1' or mid like '%.9.2')
            group by ua.log_date,case when write_uid=5 then 'seller' else 'cf' end
    ) as c 
left join

    (
        select  
        case when write_uid=5 then 'seller' else 'cf' end seller_type, 
        '全类目' as front_cate_one,
        count(distinct item_no) as item_num
        from
            jiayundw_dim.product_basic_info_df
            where active=1
        group by case when write_uid=5 then 'seller' else 'cf' end
    ) as a on a.front_cate_one=c.front_cate_one and a.seller_type=c.seller_type
left join 
    (
        select 
        date(so.create_at+interval '8' hour) event_date,
        case when write_uid=5 then 'seller' else 'cf' end seller_type, 
        '全类目' as front_cate_one,
        count(distinct so.order_name)  as pay_order_num,
        count(distinct so.user_id)  as pay_user_num,
        sum(sol.origin_qty)  as origin_qty,
        sum(sol.origin_qty*sol.price_real) as origin_amount
        from 
            jiayundw_dm.sale_order_info_history_df as so 
            left join jiayundw_dm.sale_order_line_history_df as sol on sol.order_name=so.order_name
            left join jiayundw_dim.product_basic_info_df as a on sol.item_no=a.item_no
            where so.is_delivery=0 and
                date(so.create_at+interval '8' hour) between date('{0}') and date('{0}') 
                and sol.date_id ='{0}'  and so.date_id='{0}'
		and date(sol.create_at+interval '8' hour) = date('{0}')
                    and so.is_consolidation <> 2
                    and cast(so.is_test as integer) = 0
                    and cast(so.is_cheating as integer) = 0
            group by date(so.create_at+interval '8' hour) ,case when write_uid=5 then 'seller' else 'cf' end
    ) as b on c.front_cate_one=b.front_cate_one and c.seller_type=b.seller_type

UNION ALL

select
b.event_date,
a.seller_type, 
a.front_cate_one,
null as front_cate_two,
null as front_cate_three,
item_num,
uv,
impression_num,
click_num,
pay_order_num,
pay_user_num,
origin_qty,
origin_amount
from
(
select  
        case when write_uid=5 then 'seller' else 'cf' end seller_type, 
        front_cate_one,
        count(distinct item_no) as item_num
        from
            jiayundw_dim.product_basic_info_df
            where active=1
            group by case when write_uid=5 then 'seller' else 'cf' end,
                front_cate_one
union all
select  
        'total' as seller_type, 
        front_cate_one,
        count(distinct item_no) as item_num
        from
            jiayundw_dim.product_basic_info_df
            where active=1 
            group by front_cate_one
) as a 
left join
(
select 
stat_date as event_date,
case when seller_type='all' then 'total' else seller_type end seller_type,
cate_one_en as front_cate_one,
ipv_uv as uv,
imp_pv as impression_num,
click_pv as click_num
from analysts.tb_djy_cate_log_daily
where cate_level=1 and pt='{0}' and stat_date='{0}'
) as b on a.front_cate_one=b.front_cate_one and a.seller_type=b.seller_type
left join
(
select
stat_date as event_date,
case when seller_type='all' then 'total' else seller_type end seller_type,
cate_one_en as front_cate_one,
paid_gmv origin_amount,	
paid_qty origin_qty,	
paid_order pay_order_num,
paid_user pay_user_num
from analysts.tb_djy_cate_trd_daily
where cate_level=1 and pt='{0}' and stat_date='{0}'
) as c on a.front_cate_one=c.front_cate_one and a.seller_type=c.seller_type

UNION ALL

select
b.event_date,
a.seller_type, 
a.front_cate_one,
a.front_cate_two,
null as front_cate_three,
item_num,
uv,
impression_num,
click_num,
pay_order_num,
pay_user_num,
origin_qty,
origin_amount
from
(
select  
        case when write_uid=5 then 'seller' else 'cf' end seller_type, 
        front_cate_one,
        front_cate_two,
        count(distinct item_no) as item_num
        from
            jiayundw_dim.product_basic_info_df
            where active=1
            group by case when write_uid=5 then 'seller' else 'cf' end,
                front_cate_one,front_cate_two
union all
select  
        'total' as seller_type, 
        front_cate_one,front_cate_two,
        count(distinct item_no) as item_num
        from
            jiayundw_dim.product_basic_info_df
            where active=1 
            group by front_cate_one,front_cate_two
) as a 
left join
(
select 
stat_date as event_date,
case when seller_type='all' then 'total' else seller_type end seller_type,
cate_one_en as front_cate_one,
cate_two_en as front_cate_two,
ipv_uv as uv,
imp_pv as impression_num,
click_pv as click_num
from analysts.tb_djy_cate_log_daily
where cate_level=2 and pt='{0}' and stat_date='{0}'
) as b on a.front_cate_one=b.front_cate_one and a.front_cate_two=b.front_cate_two and a.seller_type=b.seller_type
left join
(
select
stat_date as event_date,
case when seller_type='all' then 'total' else seller_type end seller_type,
cate_one_en as front_cate_one,
cate_two_en as front_cate_two,
paid_gmv origin_amount,	
paid_qty origin_qty,	
paid_order pay_order_num,
paid_user pay_user_num
from analysts.tb_djy_cate_trd_daily
where cate_level=2 and pt='{0}' and stat_date='{0}'
) as c on a.front_cate_one=c.front_cate_one and a.front_cate_two=c.front_cate_two and a.seller_type=c.seller_type

UNION ALL

select
b.event_date,
a.seller_type, 
a.front_cate_one,
a.front_cate_two,
a.front_cate_three,
item_num,
uv,
impression_num,
click_num,
pay_order_num,
pay_user_num,
origin_qty,
origin_amount
from
(
select  
        case when write_uid=5 then 'seller' else 'cf' end seller_type, 
        front_cate_one,
        front_cate_two,
        front_cate_three,
        count(distinct item_no) as item_num
        from
            jiayundw_dim.product_basic_info_df
            where active=1
            group by case when write_uid=5 then 'seller' else 'cf' end,
                front_cate_one,front_cate_two,front_cate_three
union all
select  
        'total' as seller_type, 
        front_cate_one,front_cate_two,
        front_cate_three,
        count(distinct item_no) as item_num
        from
            jiayundw_dim.product_basic_info_df
            where active=1 
            group by front_cate_one,front_cate_two,front_cate_three
) as a 
left join
(
select 
stat_date as event_date,
case when seller_type='all' then 'total' else seller_type end seller_type,
cate_one_en as front_cate_one,
cate_two_en as front_cate_two,
cate_three_en as front_cate_three,
ipv_uv as uv,
imp_pv as impression_num,
click_pv as click_num
from analysts.tb_djy_cate_log_daily
where cate_level=3 and pt='{0}' and stat_date='{0}'
) as b on a.front_cate_one=b.front_cate_one and a.front_cate_two=b.front_cate_two 
    and a.front_cate_three=b.front_cate_three and a.seller_type=b.seller_type
left join
(
select
stat_date as event_date,
case when seller_type='all' then 'total' else seller_type end seller_type,
cate_one_en as front_cate_one,
cate_two_en as front_cate_two,
cate_three_en as front_cate_three,
paid_gmv origin_amount,	
paid_qty origin_qty,	
paid_order pay_order_num,
paid_user pay_user_num
from analysts.tb_djy_cate_trd_daily
where cate_level=3 and pt='{0}' and stat_date='{0}'
) as c on a.front_cate_one=c.front_cate_one and a.front_cate_two=c.front_cate_two 
    and a.front_cate_three=c.front_cate_three and a.seller_type=c.seller_type
""".format(start)

# select
# event_date,
# seller_type,
# front_cate_one,
# front_cate_two,
# front_cate_three,
# item_num,
# uv,
# impression_num,
# click_num,
# pay_order_num,
# pay_user_num,
# origin_qty,
# origin_amount
# from analysts.yss_category_front_cate_daily_report
# where pt='{0}'

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

print(data_hive)

engine_ms = create_engine("mssql+pymssql://sa:yssshushan2008@172.16.92.2:1433/CFcategory?charset=utf8")
data_hive.to_sql('category_123_day', con=engine_ms, if_exists='append', index=False)

sql_ms = ('''
select top 10 日期 from CFcategory.dbo.category_123_day
order by 日期 desc
''')

df_ms = pd.read_sql(sql_ms, engine_ms)
print(df_ms)
