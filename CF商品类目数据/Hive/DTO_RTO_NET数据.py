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

con_redshift = psycopg2.connect(user='operation', password='Operation123',
                                host='jiayundatapro.cls0csjdlwvj.us-west-2.redshift.amazonaws.com',
                                port=5439, database='jiayundata')

con_hive = hive.Connection(host="ec2-34-222-53-168.us-west-2.compute.amazonaws.com", port=10000, username="hadoop")

sql_hive_goods = ('''

select t1.item_no,
        t1.seller_id,
        t1.seller_name,
        t1.seller_type,
        t1.front_cate_one,
        t1.front_cate_two,
        t1.active,
        t1.create_date,
        t1.product_level,
        t1.rating,
        t1.7d_rating,
        t1.gmv,
        t1.qty,
        t1.refund_total,
        t1.return_refund_total,
        t1.cancel_refund_total,
        t1.miss_refund_total,
        t1.reject_refund_total,
        t1.other_refund_total,
        
        t1.refund_qty,
        t1.return_refund_qty,
        t1.cancel_refund_qty,
        t1.miss_refund_qty,
        t1.reject_refund_qty,
        t1.other_refund_qty
from
(
                select      item_no,
					        front_cate_one,
					        front_cate_two,
					        active,
					        max(seller_type) as seller_type,
					        max(seller_id) as seller_id,
					        max(seller_name) as seller_name,
					        max(create_date) as create_date,
					        max(product_level) as product_level,
					        max(rating) as rating,
					        max(7d_rating) as 7d_rating,
					        sum(gmv) as gmv,
					        sum(qty) as qty,
							SUM(refund_total) refund_total,
							sum(refund_qty) as refund_qty
							 ,SUM(CASE WHEN t.refund_reason = 'return' THEN t.refund_total ELSE 0 END) return_refund_total
							 ,SUM(CASE WHEN t.refund_reason = 'cancel' THEN t.refund_total ELSE 0 END) cancel_refund_total
							 ,SUM(CASE WHEN t.refund_reason = 'miss_delivery' THEN t.refund_total ELSE 0 END) miss_refund_total
							 ,SUM(CASE WHEN t.refund_reason = 'reject' THEN t.refund_total ELSE 0 END) reject_refund_total
							 ,SUM(CASE WHEN t.refund_reason = 'other' THEN t.refund_total ELSE 0 END)  other_refund_total
							 ,SUM(CASE WHEN t.refund_reason = 'return' THEN t.refund_qty ELSE 0 END) return_refund_qty
							 ,SUM(CASE WHEN t.refund_reason = 'cancel' THEN t.refund_qty ELSE 0 END) cancel_refund_qty
							 ,SUM(CASE WHEN t.refund_reason = 'miss_delivery' THEN t.refund_qty ELSE 0 END) miss_refund_qty
							 ,SUM(CASE WHEN t.refund_reason = 'reject' THEN t.refund_qty ELSE 0 END) reject_refund_qty
							 ,SUM(CASE WHEN t.refund_reason = 'other' THEN t.refund_qty ELSE 0 END)  other_refund_qty
	from (
								select  a.create_date as create_at,
								        case   
											when reason = 'return' then 'return'
											when reason = 'cancel' then 'cancel'
											when reason = 'miss_delivery' then 'miss_delivery'
											when reason = 'reject' then 'reject'
											when cancel_reason is null then 'other'
											else 'cancel'
										end as refund_reason,
										b.item_no,
										b.front_cate_one,
										b.front_cate_two,
										b.active,
										sum(a.total) as gmv,
										sum(origin_qty) as qty,
										sum(refund_total) refund_total,
										sum(refund_qty) refund_qty,
										max(case when b.write_uid = 5 then 'seller' else 'cf' end) as seller_type,
            					        max(b.seller_id) as seller_id,
            					        max(b.seller_name) as seller_name,
            					        max(b.create_date) as create_date,
            					        max(b.product_level) as product_level,
            					        max(b.rating) as rating,
            					        max(b.7d_rating) as 7d_rating
								from  (
								        select * 
								        from analysts.tbl_order_detail 
								        where pt = '2020-01-27'
								            and create_date >= '2019-12-10'
								            and create_date <= '2020-01-10' 
								    ) a
								join  jiayundw_dim.product_basic_info_df b on a.item_no = b.item_no
								where b.active=1 
								group by a.create_date,
								         case   
											when reason = 'return' then 'return'
											when reason = 'cancel' then 'cancel'
											when reason = 'miss_delivery' then 'miss_delivery'
											when reason = 'reject' then 'reject'
											when cancel_reason is null then 'other'
											else 'cancel'
										end,
										b.item_no,
										b.front_cate_one,
										b.front_cate_two,
										b.active
				) t
				group by 
				    item_no,
					front_cate_one,
					front_cate_two,
					active
				having qty>=20
) t1

''')

sql_redshift_goods = ('''
SELECT a.item_no,a."name",a.front_cate_one,a.front_cate_two,a.front_cate_three,a.product_level,a.write_uid,a.price,b.origin_qty,b.origin_amount
from
(SELECT item_no,"name",front_cate_one,front_cate_two,front_cate_three,product_level,write_uid,price
from jiayundw_dim.product_basic_info_df
WHERE front_cate_one in ('Women''s Clothing','Men''s Clothing')
and lower("name") like '%un-stitched%'  or lower("name") like '%unstitched%'
and active=1) as a
LEFT JOIN
(select item_no,
       count(distinct so.order_name)  as pay_order_num,
			 count(distinct so.user_id)  as pay_user_num,
			 sum(sol.origin_qty)  as origin_qty,
			 sum(sol.origin_qty*sol.price_unit) as origin_amount
from jiayundw_dm.sale_order_info_df as so
join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
where sol.is_delivery=0 and date(so.create_at+interval '8 hour') between '2019-12-20' and '2020-01-01'
group by 1) as b on a.item_no = b.item_no
''')

data_hive_goods_df = pd.read_sql(sql_hive_goods, con_hive)

print(data_hive_goods_df.head(10))

# data_redshfit_goods_df = pd.read_sql(sql_redshift_goods, con_redshift)
# print(data_redshfit_goods_df.head(10))
#
# data_total = pd.merge(data_redshfit_goods_df, data_hive_goods_df, on=['item_no'], how='left')
# print(data_total)

# 写入Excel操作
writer = pd.ExcelWriter('钱学姐-数据' + '.xlsx')
data_hive_goods_df.to_excel(writer, sheet_name='钱学姐-货号', index=False)
os.chdir(r'/Users/apache/Downloads/A-python')
writer.save()
