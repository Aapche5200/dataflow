import os
from datetime import datetime, timedelta  # 设置当前时间及时间间隔计算需要用的包
import pandas as pd
from sqlalchemy.engine import create_engine
from pyhive import hive
from impala.dbapi import connect

con_hive = hive.Connection(host="ec2-34-222-53-168.us-west-2.compute.amazonaws.com", port=10000, username="hadoop")

sql_goods = """
select a.item_no,a.sku_id,b.product_level,b.write_uid,sum(qty)
,sum(a.total) as total,sum(a.refund_total) as refund_total,(sum(a.total)-sum(a.refund_total)) net_gmv
    from ( select a.create_date
        ,a.seller_type
        ,case when a.channel='cod' then 'cod'else'ppd'end channel
        ,a.warehouse
        ,a.item_no
        ,a.sku_id
        ,a.total
        ,a.refund_total
        ,case when a.is_delivery=0 then a.origin_qty else 0 end qty
        ,case when a.is_delivery=0 then a.refund_qty else 0 end refund_qty
        from analysts.rpt_middle_outofstock_refund_detail_d a
        where a.create_date between date('2020-02-03') and  date('2020-02-09') 
            and a.pt = '2020-02-10'
        ) a
    left join jiayundw_dim.product_basic_info_df as b on b.item_no=a.item_no
    where b.product_level in (4,5) and b.write_uid=5
    group by a.item_no,a.sku_id,b.product_level,b.write_uid
"""

data_goods = pd.read_sql(sql_goods, con_hive)
print(data_goods.head(5))
writer = pd.ExcelWriter('caijia-商品-SKU' + '.xlsx')
data_goods.to_excel(writer, sheet_name='商品', index=False)
os.chdir(r'/Users/apache/Downloads/A-python')
writer.save()
