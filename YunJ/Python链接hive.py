import pandas as pd
from pyhive import hive

con_hive = hive.Connection(host="175.24.24.12",
                           username="yinss",
                           port=10008,
                           password='700234',
                           auth='CUSTOM',
                           database='dw', )
print(1)
sql_cate = '''
   select a.order_id as orderid,
      concat(item_oms_cid4, '-', item_oms_cname4) cate
   from dw.dw_trd_order_barcode_anlys_d as a
   where date(pay_time) between date('2020-07-01') and date('2020-12-31')
     and stat_day= '20210201'
     and order_status in (2, 3, 4, 9, -1, -2, -3) --计算正常销售 5 6是退款
     and normal_busi_type = 1
     and order_source <> '外部直播'
     and item_oms_cname1 not like '%测试分类勿选%'
     and cid1 != 293
     and item_oms_cname4 <> '未知'
     and pay_time is not null
     and dept_name ='服饰鞋包'
     and item_oms_cid4 is not null
       '''

data = pd.read_sql(sql_cate, con_hive)
