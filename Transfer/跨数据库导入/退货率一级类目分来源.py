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


start_date = '2020-06-01'
end_date = '2020-06-07'

con_redshift = prestodb.dbapi.connect(
    host='ec2-54-68-88-224.us-west-2.compute.amazonaws.com',
    port=80,
    user='hadoop',
    catalog='hive',
    schema='default',
)


sql_red = ('''
select d.old_cate_one,extract(week from a.send_date) as week,d.yewu_type
      ,sum(b.product_qty)  sp_shipped_qty
      ,sum(case when b.product_qty<b.refund_qty_real_return then b.product_qty else b.refund_qty_real_return end) refund_qty_real_return
      ,case when sum(b.product_qty)=0 then 0 else 
        cast(sum(case when b.product_qty<b.refund_qty_real_return then b.product_qty else b.refund_qty_real_return end) as double)/cast(sum(b.product_qty) as double) end tuihuolv
from
   (select order_name
    from jiayundw_dm.sale_order_info_df 
    where create_at is not null and is_consolidation<>2 and is_delivery=0
    ) a1
join     
   (select fulfillment_order_name
          ,date(send_time+interval '8' hour) send_date
          ,warehouse
          ,seller_type
          ,shipping_country country
          ,order_name
    from jiayundw_dm.sale_order_info_fo_df 
    where send_time>=timestamp '{0} 16:00:00'and send_time<timestamp '{1} 16:00:00'
    ) a on a.order_name=a1.order_name
left join   
   (select fulfillment_order_name
          ,sku_id
          ,item_no
          ,category_three_en
          ,product_qty
          ,sp_shipped_qty
          ,case when is_reject = 0 then refund_qty_real_return else 0 end refund_qty_real_return
    from jiayundw_dm.sale_order_line_fo_df 
    where is_delivery=0 
    ) b on a.fulfillment_order_name=b.fulfillment_order_name
left join   
       (select item_no
              ,front_cate_one old_cate_one
              ,front_cate_two
              ,front_cate_three
              ,case when write_uid=5 then 'seller' else 'cf' end yewu_type							
      from jiayundw_dim.product_basic_info_df) d on b.item_no=d.item_no
WHERE yewu_type is not null
group by  d.old_cate_one,2,3
UNION ALL
(select d.old_cate_one,extract(week from a.send_date) as week,'total' yewu_type
      ,sum(b.product_qty)  sp_shipped_qty
      ,sum(case when b.product_qty<b.refund_qty_real_return then b.product_qty else b.refund_qty_real_return end) refund_qty_real_return
      ,case when sum(b.product_qty)=0 then 0 else 
        cast(sum(case when b.product_qty<b.refund_qty_real_return then b.product_qty else b.refund_qty_real_return end) as double)/cast(sum(b.product_qty) as double) end tuihuolv
from
   (select order_name
    from jiayundw_dm.sale_order_info_df 
    where create_at is not null and is_consolidation<>2 and is_delivery=0
    ) a1
join     
   (select fulfillment_order_name
          ,date(send_time+interval '8' hour) send_date
          ,warehouse
          ,seller_type
          ,shipping_country country
          ,order_name
    from jiayundw_dm.sale_order_info_fo_df 
    where send_time>=timestamp '{0} 16:00:00'and send_time< timestamp '{1} 16:00:00'
    ) a on a.order_name=a1.order_name
left join   
   (select fulfillment_order_name
          ,sku_id
          ,item_no
          ,category_three_en
          ,product_qty
          ,sp_shipped_qty
          ,case when is_reject = 0 then refund_qty_real_return else 0 end refund_qty_real_return
    from jiayundw_dm.sale_order_line_fo_df 
    where is_delivery=0 
    ) b on a.fulfillment_order_name=b.fulfillment_order_name
left join   
       (select item_no
              ,front_cate_one old_cate_one
              ,front_cate_two
              ,front_cate_three
              ,case when write_uid=5 then 'seller' else 'cf' end yewu_type							
      from jiayundw_dim.product_basic_info_df) d on b.item_no=d.item_no
WHERE yewu_type is not null
group by  d.old_cate_one,2
)
UNION ALL
(select distinct '全类目' as old_cate_one,extract(week from a.send_date) as week,d.yewu_type
      ,sum(b.product_qty)  sp_shipped_qty
      ,sum(case when b.product_qty<b.refund_qty_real_return then b.product_qty else b.refund_qty_real_return end) refund_qty_real_return
      ,case when sum(b.product_qty)=0 then 0 else 
        cast(sum(case when b.product_qty<b.refund_qty_real_return then b.product_qty else b.refund_qty_real_return end) as double)/cast(sum(b.product_qty) as double) end tuihuolv
from
   (select order_name
    from jiayundw_dm.sale_order_info_df 
    where create_at is not null and is_consolidation<>2 and is_delivery=0
    ) a1
join     
   (select fulfillment_order_name
          ,date(send_time+interval '8' hour) send_date
          ,warehouse
          ,seller_type
          ,shipping_country country
          ,order_name
    from jiayundw_dm.sale_order_info_fo_df 
    where send_time>= timestamp '{0} 16:00:00'and send_time< timestamp '{1} 16:00:00'
    ) a on a.order_name=a1.order_name
left join   
   (select fulfillment_order_name
          ,sku_id
          ,item_no
          ,category_three_en
          ,product_qty
          ,sp_shipped_qty
          ,case when is_reject = 0 then refund_qty_real_return else 0 end refund_qty_real_return
    from jiayundw_dm.sale_order_line_fo_df 
    where is_delivery=0 
    ) b on a.fulfillment_order_name=b.fulfillment_order_name
left join   
       (select item_no
              ,front_cate_one
              ,front_cate_two
              ,front_cate_three
              ,case when write_uid=5 then 'seller' else 'cf' end yewu_type							
      from jiayundw_dim.product_basic_info_df) d on b.item_no=d.item_no
WHERE yewu_type is not null
group by  2,3
UNION ALL
(select distinct '全类目' as old_cate_one,extract(week from a.send_date) as week,'total' yewu_type
      ,sum(b.product_qty)  sp_shipped_qty
      ,sum(case when b.product_qty<b.refund_qty_real_return then b.product_qty else b.refund_qty_real_return end) refund_qty_real_return
      ,case when sum(b.product_qty)=0 then 0 else 
        cast(sum(case when b.product_qty<b.refund_qty_real_return then b.product_qty else b.refund_qty_real_return end) as double)/cast(sum(b.product_qty) as double) end tuihuolv
from
   (select order_name
    from jiayundw_dm.sale_order_info_df 
    where create_at is not null and is_consolidation<>2 and is_delivery=0
    ) a1
join     
   (select fulfillment_order_name
          ,date(send_time+interval '8' hour) send_date
          ,warehouse
          ,seller_type
          ,shipping_country country
          ,order_name
    from jiayundw_dm.sale_order_info_fo_df 
    where send_time>= timestamp '{0} 16:00:00'and send_time< timestamp '{1} 16:00:00'
    ) a on a.order_name=a1.order_name
left join   
   (select fulfillment_order_name
          ,sku_id
          ,item_no
          ,category_three_en
          ,product_qty
          ,sp_shipped_qty
          ,case when is_reject = 0 then refund_qty_real_return else 0 end refund_qty_real_return
    from jiayundw_dm.sale_order_line_fo_df 
    where is_delivery=0 
    ) b on a.fulfillment_order_name=b.fulfillment_order_name
left join   
       (select item_no
              ,front_cate_one
              ,front_cate_two
              ,front_cate_three
              ,case when write_uid=5 then 'seller' else 'cf' end yewu_type							
      from jiayundw_dim.product_basic_info_df) d on b.item_no=d.item_no
WHERE yewu_type is not null
group by  2
))
ORDER BY 2,1,3
''').format(start_date, end_date)

cursor = con_redshift.cursor()
cursor.execute(sql_red)
data = cursor.fetchall()
column_descriptions = cursor.description
if data:
    data_red = pd.DataFrame(data)
    data_red.columns = [c[0] for c in column_descriptions]
else:
    data_red = pd.DataFrame()
print(data_red)
engine_ms = create_engine("mssql+pymssql://sa:yssshushan2008@172.16.92.2:1433/CFcategory?charset=utf8")
data_red.to_sql('CategoryReturn', con=engine_ms, if_exists='append', index=False)

sql_ms = ('''
select top 10 week from CFcategory.dbo.CategoryReturn
order by week desc
''')

data_ms = pd.read_sql(sql_ms, engine_ms)
print(data_ms)
