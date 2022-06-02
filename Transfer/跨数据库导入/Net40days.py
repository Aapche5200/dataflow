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

con_hive = prestodb.dbapi.connect(
    host='ec2-54-68-88-224.us-west-2.compute.amazonaws.com',
    port=80,
    user='hadoop',
    catalog='hive',
    schema='default',
)

sql_hive = ('''										
select t1.item_no,											
        t1.pid,											
        t1.seller_id,											
        t1.seller_name,											
        t1.seller_type,											
        t1.front_cate_one,											
        t1.front_cate_two,
        t1.front_cate_three,
        t1.create_at,											
        t1.product_level,											
        t1.rating,											
        t1."7d_rating",											
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
                            pid,						
					        create_at,											
					        front_cate_one,						
					        front_cate_two,
					        front_cate_three,
					        max(seller_type) as seller_type,						
					        max(seller_id) as seller_id,						
					        max(seller_name) as seller_name,						
					        max(product_level) as product_level,						
					        max(rating) as rating,						
					        max("7d_rating") as "7d_rating",						
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
								select  date(a.create_date) as create_at,			
								        reason,			
								        case   			
											when reason = 'return' then 'return'
											when reason = 'cancel' then 'cancel'
											when reason = 'miss_delivery' then 'miss_delivery'
											when reason = 'reject' then 'reject'
											when cancel_reason is null then 'other'
											else 'cancel'
										end as refund_reason,	
										b.item_no,	
										b.pid,	
										b.front_cate_one,	
										b.front_cate_two,
										b.front_cate_three,
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
            					        max(b."7d_rating") as "7d_rating"
                                    from  (			
								        select date(create_date) as create_date,reason,cancel_reason,item_no,total,origin_qty,refund_total,refund_qty			
								        from analysts.rpt_middle_outofstock_refund_detail_d 			
								        where pt = '2020-07-08'		
								            and date(create_date) + interval '40' day = date(pt)
								    ) a			
								join  jiayundw_dim.product_basic_info_df b on a.item_no = b.item_no			
								group by 			
								        a.create_date,			
								        reason,			
								        case   			
											when reason = 'return' then 'return'
											when reason = 'cancel' then 'cancel'
											when reason = 'miss_delivery' then 'miss_delivery'
											when reason = 'reject' then 'reject'
											when cancel_reason is null then 'other'
											else 'cancel'
										end,	
										b.item_no,	
										b.pid,	
										b.front_cate_one,	
										b.front_cate_two,
										b.front_cate_three
				) t							
				group by 							
				    item_no,
				    create_at,							
				    pid,							
					front_cate_one,						
					front_cate_two,
					front_cate_three
) t1
''')

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

engine_ms = create_engine("mssql+pymssql://sa:yssshushan2008@172.16.92.2:1433/CFcategory?charset=utf8")
df_hive.to_sql('NetGmvData', con=engine_ms, if_exists='append', index=False)

sql_ms = ('''
select top 10 create_at from CFcategory.dbo.NetGmvData
order by create_at desc
''')

df_ms = pd.read_sql(sql_ms, engine_ms)
print(df_ms)
