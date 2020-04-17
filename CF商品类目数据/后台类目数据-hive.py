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

con_redshift = psycopg2.connect(user='operation', password='Operation123',
                                host='jiayundatapro.cls0csjdlwvj.us-west-2.redshift.amazonaws.com',
                                port=5439, database='jiayundata')

sql_cagegory_hive_three = """
select cate_one_cn,cate_two_cn,cate_three_cn,month(a.create_date) as date_time,a.seller_type
,sum(a.total) as total,sum(a.refund_total) as refund_total,(sum(a.total)-sum(a.refund_total)) net_gmv
    from ( select a.create_date
        ,a.seller_type
        ,case when a.channel='cod' then 'cod'else'ppd'end channel
        ,a.warehouse
        ,a.item_no
        ,a.total
        ,a.refund_total
        ,case when a.is_delivery=0 then a.origin_qty else 0 end qty
        ,case when a.is_delivery=0 then a.refund_qty else 0 end refund_qty
        from analysts.rpt_middle_outofstock_refund_detail_d a
        where a.create_date between date('2019-10-01') and  date('2020-01-31') 
            and a.pt = '2020-02-10'
        ) a
    left join jiayundw_dim.product_basic_info_df as b on b.item_no=a.item_no
    group by cate_one_cn,cate_two_cn,cate_three_cn,month(a.create_date),a.seller_type
"""

sql_cagegory_hive_two = """
select cate_one_cn,cate_two_cn,month(a.create_date) as date_time,a.seller_type
,sum(a.total) as total,sum(a.refund_total) as refund_total,(sum(a.total)-sum(a.refund_total)) net_gmv
    from ( select a.create_date
        ,a.seller_type
        ,case when a.channel='cod' then 'cod'else'ppd'end channel
        ,a.warehouse
        ,a.item_no
        ,a.total
        ,a.refund_total
        ,case when a.is_delivery=0 then a.origin_qty else 0 end qty
        ,case when a.is_delivery=0 then a.refund_qty else 0 end refund_qty
        from analysts.rpt_middle_outofstock_refund_detail_d a
        where a.create_date between date('2019-10-01') and  date('2020-01-31') 
            and a.pt = '2020-02-10'
        ) a
    left join jiayundw_dim.product_basic_info_df as b on b.item_no=a.item_no
    group by cate_one_cn,cate_two_cn,month(a.create_date),a.seller_type
"""

sql_cagegory_hive_one = """
select cate_one_cn,month(a.create_date) as date_time,a.seller_type
,sum(a.total) as total,sum(a.refund_total) as refund_total,(sum(a.total)-sum(a.refund_total)) net_gmv
    from ( select a.create_date
        ,a.seller_type
        ,case when a.channel='cod' then 'cod'else'ppd'end channel
        ,a.warehouse
        ,a.item_no
        ,a.total
        ,a.refund_total
        ,case when a.is_delivery=0 then a.origin_qty else 0 end qty
        ,case when a.is_delivery=0 then a.refund_qty else 0 end refund_qty
        from analysts.rpt_middle_outofstock_refund_detail_d a
        where a.create_date between date('2019-10-01') and  date('2020-01-31') 
            and a.pt = '2020-02-10'
        ) a
    left join jiayundw_dim.product_basic_info_df as b on b.item_no=a.item_no
    group by cate_one_cn,month(a.create_date),a.seller_type
"""

sql_category_red_three = """
SELECT a.cate_one_cn,a.cate_two_cn,a.cate_three_cn,a.seller_type,a.季度 as date_time,a.uv,a.impression_num,a.click_num,b.gmv, b.销量, b.售出商品数, b.出单均价,c.good_nums from 
(select cate_one_cn,cate_two_cn,cate_three_cn,case when write_uid =5 then 'seller' else 'cf' end seller_type,
datepart(month,ua.log_date)as 季度,
               count(distinct case when event_type in ('product','impression') and mid='1.5.1.1' then cid else null end) as uv,
               count(case when  event_type='product' and mid like '%.9.1' then cid else null end) as impression_num,
               count(case when  event_type='click' and mid like '%.9.1' then cid else null end) as click_num
        from jiayundw_dws_spectrum.flow_pid_action_di as ua 
        join jiayundw_dim.product_basic_info_df as pro on pro.pid=ua.pid
where ua.log_date between '20191001' and '20200131'  
        group by 1,2,3,4,5) as a 
LEFT JOIN 
(SELECT 
b.cate_one_cn,cate_two_cn,cate_three_cn,datepart(month,a.log_date)as 季度,
case when b.write_uid =5 then 'seller' else 'cf' end seller_type,
sum(origin_amount) as gmv , sum(origin_qty) as 销量,count(distinct a.item_no) as 售出商品数,
case when sum(origin_qty)=0 then 0 else sum(origin_amount)::FLOAT/sum(origin_qty)::FLOAT end 出单均价
from (
select 
item_no,
date(so.create_at+interval '8 hour') as log_date,
				count(distinct item_no) as sale_item_num,
       count(distinct so.order_name)  as pay_order_num,
			 count(distinct so.user_id)  as pay_user_num,
			 sum(sol.origin_qty)  as origin_qty,
			 sum(sol.origin_qty*sol.price_unit) as origin_amount
from jiayundw_dm.sale_order_info_df as so 
join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
where sol.is_delivery=0 and date(so.create_at+interval '8 hour') between '20191001' and '20200131' 
group by 
1,
2
) as a
left join jiayundw_dim.product_basic_info_df  as b on a.item_no=b.item_no 
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5) as b on a.cate_one_cn=b.cate_one_cn and a.seller_type=b.seller_type and a.季度=b.季度 and a.cate_two_cn=b.cate_two_cn and a.cate_three_cn=b.cate_three_cn
LEFT JOIN
(
select cate_one_cn,cate_two_cn,cate_three_cn,
case when write_uid =5 then 'seller' else 'cf' end seller_type,
COUNT(DISTINCT item_no) as good_nums from jiayundw_dim.product_basic_info_df
WHERE active = 1
GROUP BY 1,2,3,4) as c on a.cate_one_cn=c.cate_one_cn and a.seller_type=c.seller_type and a.cate_two_cn=c.cate_two_cn and a.cate_three_cn=c.cate_three_cn
"""

sql_category_red_two = """
SELECT a.cate_one_cn,a.cate_two_cn,a.seller_type,a.季度 as date_time,a.uv,a.impression_num,a.click_num,b.gmv, b.销量, b.售出商品数, b.出单均价,c.good_nums from 
(select cate_one_cn,cate_two_cn,case when write_uid =5 then 'seller' else 'cf' end seller_type,
datepart(month,ua.log_date)as 季度,
               count(distinct case when event_type in ('product','impression') and mid='1.5.1.1' then cid else null end) as uv,
               count(case when  event_type='product' and mid like '%.9.1' then cid else null end) as impression_num,
               count(case when  event_type='click' and mid like '%.9.1' then cid else null end) as click_num
        from jiayundw_dws_spectrum.flow_pid_action_di as ua 
        join jiayundw_dim.product_basic_info_df as pro on pro.pid=ua.pid
where ua.log_date between '20191001' and '20200131'  
        group by 1,2,3,4) as a 
LEFT JOIN 
(SELECT 
b.cate_one_cn,cate_two_cn,datepart(month,a.log_date)as 季度,
case when b.write_uid =5 then 'seller' else 'cf' end seller_type,
sum(origin_amount) as gmv , sum(origin_qty) as 销量,count(distinct a.item_no) as 售出商品数,
case when sum(origin_qty)=0 then 0 else sum(origin_amount)::FLOAT/sum(origin_qty)::FLOAT end 出单均价
from (
select 
item_no,
date(so.create_at+interval '8 hour') as log_date,
				count(distinct item_no) as sale_item_num,
       count(distinct so.order_name)  as pay_order_num,
			 count(distinct so.user_id)  as pay_user_num,
			 sum(sol.origin_qty)  as origin_qty,
			 sum(sol.origin_qty*sol.price_unit) as origin_amount
from jiayundw_dm.sale_order_info_df as so 
join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
where sol.is_delivery=0 and date(so.create_at+interval '8 hour') between '20191001' and '20200131' 
group by 
1,
2
) as a
left join jiayundw_dim.product_basic_info_df  as b on a.item_no=b.item_no 
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4) as b on a.cate_one_cn=b.cate_one_cn and a.seller_type=b.seller_type and a.季度=b.季度 and a.cate_two_cn=b.cate_two_cn
LEFT JOIN
(
select cate_one_cn,cate_two_cn,
case when write_uid =5 then 'seller' else 'cf' end seller_type,
COUNT(DISTINCT item_no) as good_nums from jiayundw_dim.product_basic_info_df
WHERE active = 1
GROUP BY 1,2,3) as c on a.cate_one_cn=c.cate_one_cn and a.seller_type=c.seller_type and a.cate_two_cn=c.cate_two_cn
"""

sql_category_red_one = """
SELECT a.cate_one_cn,a.seller_type,a.季度 as date_time,a.uv,a.impression_num,a.click_num,b.gmv, b.销量, b.售出商品数, b.出单均价,c.good_nums from 
(select cate_one_cn,case when write_uid =5 then 'seller' else 'cf' end seller_type,
datepart(month,ua.log_date)as 季度,
               count(distinct case when event_type in ('product','impression') and mid='1.5.1.1' then cid else null end) as uv,
               count(case when  event_type='product' and mid like '%.9.1' then cid else null end) as impression_num,
               count(case when  event_type='click' and mid like '%.9.1' then cid else null end) as click_num
        from jiayundw_dws_spectrum.flow_pid_action_di as ua 
        join jiayundw_dim.product_basic_info_df as pro on pro.pid=ua.pid
where ua.log_date between '20191001' and '20200131'  
        group by 1,2,3) as a 
LEFT JOIN 
(SELECT 
b.cate_one_cn,datepart(month,a.log_date)as 季度,
case when b.write_uid =5 then 'seller' else 'cf' end seller_type,
sum(origin_amount) as gmv , sum(origin_qty) as 销量,count(distinct a.item_no) as 售出商品数,
case when sum(origin_qty)=0 then 0 else sum(origin_amount)::FLOAT/sum(origin_qty)::FLOAT end 出单均价
from (
select 
item_no,
date(so.create_at+interval '8 hour') as log_date,
				count(distinct item_no) as sale_item_num,
       count(distinct so.order_name)  as pay_order_num,
			 count(distinct so.user_id)  as pay_user_num,
			 sum(sol.origin_qty)  as origin_qty,
			 sum(sol.origin_qty*sol.price_unit) as origin_amount
from jiayundw_dm.sale_order_info_df as so 
join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
where sol.is_delivery=0 and date(so.create_at+interval '8 hour') between '20191001' and '20200131' 
group by 
1,
2
) as a
left join jiayundw_dim.product_basic_info_df  as b on a.item_no=b.item_no 
GROUP BY 1,2,3
ORDER BY 1,2,3) as b on a.cate_one_cn=b.cate_one_cn and a.seller_type=b.seller_type and a.季度=b.季度
LEFT JOIN
(
select cate_one_cn,
case when write_uid =5 then 'seller' else 'cf' end seller_type,
COUNT(DISTINCT item_no) as good_nums from jiayundw_dim.product_basic_info_df
WHERE active = 1
GROUP BY 1,2) as c on a.cate_one_cn=c.cate_one_cn and a.seller_type=c.seller_type
"""


data_category_hive_three = pd.read_sql(sql_cagegory_hive_three, con_hive)
data_category_hive_two = pd.read_sql(sql_cagegory_hive_two, con_hive)
data_category_hive_one = pd.read_sql(sql_cagegory_hive_one, con_hive)

data_category_red_three = pd.read_sql(sql_category_red_three, con_redshift)
data_category_red_two = pd.read_sql(sql_category_red_two, con_redshift)
data_category_red_one = pd.read_sql(sql_category_red_one, con_redshift)

data_category_three = pd.merge(data_category_red_three, data_category_hive_three,
                               on=['cate_one_cn', 'cate_two_cn', 'cate_three_cn', 'date_time', 'seller_type'],
                               how='left')
data_category_two = pd.merge(data_category_red_two, data_category_hive_two,
                             on=['cate_one_cn', 'cate_two_cn', 'date_time', 'seller_type'],
                             how='left')
data_category_one = pd.merge(data_category_red_one, data_category_hive_one,
                             on=['cate_one_cn', 'date_time', 'seller_type'],
                             how='left')


writer = pd.ExcelWriter('后台一二三级4个月net数据-查看' + '.xlsx')
data_category_three.to_excel(writer, sheet_name='三级类目', index=False)
data_category_two.to_excel(writer, sheet_name='二级类目', index=False)
data_category_one.to_excel(writer, sheet_name='一级类目', index=False)
os.chdir(r'/Users/apache/Downloads/A-python')
writer.save()






