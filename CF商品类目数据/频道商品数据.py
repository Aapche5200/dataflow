#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 14:23:05 2019

@author: apache
"""


import psycopg2
import pymysql
import pandas as pd
import numpy as np
import time
import os
from datetime import datetime, timedelta



start_input='2019-11-18'
end_input='2019-11-24'

def get_redshift_test_conn():
    conn = psycopg2.connect(
        user='operation',
        password='Operation123',
        host='jiayundatapro.cls0csjdlwvj.us-west-2.redshift.amazonaws.com',
        port=5439,
        database='jiayundata')
    return conn


start_timestamp=time.mktime(time.strptime(start_input, '%Y-%m-%d')) # 1482286976.0
end_timestamp=time.mktime(time.strptime(end_input, '%Y-%m-%d'))

start_yesterday = datetime.fromtimestamp(start_timestamp-24*3600).strftime('%Y-%m-%d')
end_tomorrow = datetime.fromtimestamp(end_timestamp+24*3600).strftime('%Y-%m-%d')

print(start_yesterday,start_input,end_input,end_tomorrow)

sql="""
select a.pay_date as 日期, a.country as 国家,a.gender as 性别,a.laiyuan as 页面ID, f.description as 描述, 
e.item_no as 货号, h.front_cate_one as 一级类目, h.front_cate_two as 二级类目, h.front_cate_three as 三级类目,
h.rating as 评分,h.write_uid,h.price,h.product_level,i.supply_chain_type,
b.pv_impression as 曝光PV,b.uv_impression as 曝光UV,c.pv_click as 点击PV,c.uv_click as 点击UV,d.uv_add 加购UV,
a.uv_pay as 支付UV, a.so_num as 订单数,a.gmv as GMV
from 
(select pay_date ,
case when country in ( 'bh','qa','kw','om','in','sa','ae')   then country
else 'others' end country, laiyuan,
case when (a.gender ='U' or a.gender is null) then 'unknow' else a.gender end as gender,pid,
count(DISTINCT cid) uv_pay,
count(DISTINCT order_name) so_num,
sum(coalesce(origin_qty, '0')*origin_real) gmv 
from analysts.user_pay_success_detail_08 a
where a.pay_date between '{1}' and '{2}'
and laiyuan in (

'1.113135',
'1.113136',
'1.113137',
'1.113138',
'1.113139',
'1.113140',
'1.113141',
'1.113142',
'1.113143',
'1.113144',
'1.113148',
'1.113149',
'1.113150',
'1.113151',
'1.113152',
'1.113153',
'1.113302',
'1.113155',
'1.113156',
'1.113157',
'1.113158',
'1.113159',
'1.113160',
'1.113161',
'1.113162',
'1.113164',
'1.113165',
'1.113166',
'1.113167',
'1.113168',
'1.113191',
'1.113192',
'1.113301'



)
group by 1,2,3,4,5)  a left join
(select date(a.server_time+ interval'8 hour') event_date ,
case when country_code in ( 'bh','qa','kw','om','in','sa','ae')   then country_code
else 'others' end country,
case when a.mid like '%.9.1%' then  split_part(a.mid,'.9.1',1) 
else split_part(a.mid,'.4.1',1)  end as mid,
case when (a.gender ='U' or a.gender is null) then 'unknow' else a.gender end as gender,
pid,
count(cid) pv_impression,
count(DISTINCT cid) uv_impression
from spectrum_schema.user_trace a 
where log_date>='{0}'
and log_date<='{3}'
and a.server_time between '{0} 16:00:00' and '{2} 15:59:59'
and event_type='product'
and a.mid in (

'1.113135.9.1',
'1.113136.9.1',
'1.113137.9.1',
'1.113138.9.1',
'1.113139.9.1',
'1.113140.9.1',
'1.113141.9.1',
'1.113142.9.1',
'1.113143.9.1',
'1.113144.9.1',
'1.113148.9.1',
'1.113149.9.1',
'1.113150.9.1',
'1.113151.9.1',
'1.113152.9.1',
'1.113153.9.1',
'1.113302.9.1',
'1.113155.9.1',
'1.113156.9.1',
'1.113157.9.1',
'1.113158.9.1',
'1.113159.9.1',
'1.113160.9.1',
'1.113161.9.1',
'1.113162.9.1',
'1.113164.9.1',
'1.113165.9.1',
'1.113166.9.1',
'1.113167.9.1',
'1.113168.9.1',
'1.113191.9.1',
'1.113192.9.1',
'1.113301.9.1'


) 
group by 1,2,3,4,5) b on a.pay_date=b.event_date and a.country=b.country and a.laiyuan=b.mid and a.gender=b.gender and a.pid=b.pid
left join 
(select date(a.server_time+ interval'8 hour') event_date ,
case when country_code in ( 'bh','qa','kw','om','in','sa','ae')   then country_code
else 'others' end country,
case when a.mid like '%.9.1%' then  split_part(a.mid,'.9.1',1) 
else split_part(a.mid,'.4.1',1)  end as mid,
case when (a.gender ='U' or a.gender is null) then 'unknow' else a.gender end as gender,
pid,
count(cid) pv_click,
count(DISTINCT cid) uv_click
from spectrum_schema.user_trace a 
where log_date>='{0}'
and log_date<='{3}'
and a.server_time between '{0} 16:00:00' and '{2} 15:59:59'
and event_type='click'
and mid like '%.9.1'
and a.mid in (

'1.113135.9.1',
'1.113136.9.1',
'1.113137.9.1',
'1.113138.9.1',
'1.113139.9.1',
'1.113140.9.1',
'1.113141.9.1',
'1.113142.9.1',
'1.113143.9.1',
'1.113144.9.1',
'1.113148.9.1',
'1.113149.9.1',
'1.113150.9.1',
'1.113151.9.1',
'1.113152.9.1',
'1.113153.9.1',
'1.113302.9.1',
'1.113155.9.1',
'1.113156.9.1',
'1.113157.9.1',
'1.113158.9.1',
'1.113159.9.1',
'1.113160.9.1',
'1.113161.9.1',
'1.113162.9.1',
'1.113164.9.1',
'1.113165.9.1',
'1.113166.9.1',
'1.113167.9.1',
'1.113168.9.1',
'1.113191.9.1',
'1.113192.9.1',
'1.113301.9.1'
)  
group by 1,2,3,4,5 ) c  on a.pay_date=c.event_date and a.country=c.country and a.laiyuan=c.mid and a.gender=c.gender and a.pid=c.pid
left join
(select date(a.time_stamp+ interval'8 hour') event_date ,
case when country_code in ( 'bh','qa','kw','om','in','sa','ae')   then country_code
else 'others' end country,
case when a.m_mid like '%.9.1%' then  split_part(a.m_mid,'.9.1',1) 
else split_part(a.m_mid,'.4.1',1)  end as mid,
case when (a.gender ='U' or a.gender is null) then 'unknow' else a.gender end as gender,pid,
count(DISTINCT cid) uv_add
from public.cart_buynow_result a 
where a.time_stamp between '{0} 16:00:00' and '{2} 15:59:59'
and a.m_mid in (

'1.113135.9.1',
'1.113136.9.1',
'1.113137.9.1',
'1.113138.9.1',
'1.113139.9.1',
'1.113140.9.1',
'1.113141.9.1',
'1.113142.9.1',
'1.113143.9.1',
'1.113144.9.1',
'1.113148.9.1',
'1.113149.9.1',
'1.113150.9.1',
'1.113151.9.1',
'1.113152.9.1',
'1.113153.9.1',
'1.113302.9.1',
'1.113155.9.1',
'1.113156.9.1',
'1.113157.9.1',
'1.113158.9.1',
'1.113159.9.1',
'1.113160.9.1',
'1.113161.9.1',
'1.113162.9.1',
'1.113164.9.1',
'1.113165.9.1',
'1.113166.9.1',
'1.113167.9.1',
'1.113168.9.1',
'1.113191.9.1',
'1.113192.9.1',
'1.113301.9.1'


)
group by 1,2,3,4,5) d  on a.pay_date=d.event_date and a.country=d.country and a.laiyuan=d.mid and a.gender=d.gender and a.pid=d.pid
left join jiayundw_dim.product_basic_info_df e on a.pid=e.pid
left join public.mid_map f on a.laiyuan=f.mid
join jiayundw_dim.product_basic_info_df h on e.item_no=h.item_no
left join (SELECT DISTINCT item_id,case when supply_chain_risk_flag=-1 then '黑名单'
when supply_chain_risk_flag=1 then '白名单'
when supply_chain_risk_flag=0 and is_grey_good=0 then '灰名单'
when supply_chain_risk_flag=0 and is_grey_good=1 then '偏白灰名单' end supply_chain_type
FROM supply_chain.supply_chain_risk_score_info_log 
where log_date='2019-11-12') as i on h.item_no=i.item_id
ORDER BY 1,2,3,4
""".format(start_yesterday,start_input,end_input,end_tomorrow)

if __name__ == '__main__':
    conn = get_redshift_test_conn()
    df=pd.read_sql(sql,conn)
    print(df)
    #df.to_excel('new_goods{0}.xls'.format(end_tomorrow),index=False )

    # 写入excel 操作
    writer = pd.ExcelWriter('专区数据' + time.strftime('%Y-%m-%d', time.localtime(time.time())) + '.xlsx')
    df.to_excel(writer, sheet_name='商品数据', index=False)

    os.chdir(r'/Users/apache/Downloads/A-python')
    writer.save()


