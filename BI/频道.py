# -*- coding: utf-8 -*-
"""
Created on Mon Apr 15 14:03:49 2019

@author: Administrator
"""

import psycopg2
import pymysql
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta



start_input='2019-05-27'
end_input='2019-06-02'


def get_redshift_test_conn():
    conn = psycopg2.connect(
            user='yinshushan',
            password='Yinshushan123',
            host='jiayundatatest.cls0csjdlwvj.us-west-2.redshift.amazonaws.com',
            port=5439,
            database='jiayundata')
    return conn


start_timestamp=time.mktime(time.strptime(start_input, '%Y-%m-%d')) # 1482286976.0
end_timestamp=time.mktime(time.strptime(end_input, '%Y-%m-%d'))

start_yesterday = datetime.fromtimestamp(start_timestamp-24*3600).strftime('%Y-%m-%d')
end_tomorrow = datetime.fromtimestamp(end_timestamp+24*3600).strftime('%Y-%m-%d')

print(start_yesterday,start_input,end_input,end_tomorrow)

sql="""
select a.pay_time as 日期, a.country as 国家,a.gender as 性别,a.mid as 页面ID, f.description as 描述, 
e.product_no as 货号, h.old_cate_one as 一级类目, h.old_cate_two as 二级类目, h.old_cate_three as 三级类目,
b.pv_impression as 曝光PV,b.uv_impression as 曝光UV,c.pv_click as 点击PV,c.uv_click as 点击UV,d.uv_add 加购UV,
a.uv_pay as 支付UV, a.so_num as 订单数,a.gmv as GMV
from 
(select pay_time ,
case when country in ( 'bh','qa','kw','om','in','sa','ae')   then country
else 'others' end country, mid,
case when (a.gender ='U' or a.gender is null) then 'unknow' else a.gender end as gender,pid,
count(DISTINCT cid) uv_pay,
count(DISTINCT order_name) so_num,
sum(coalesce(pno_num, '0')*price) gmv 
from analysts.user_pay_success_detail a
where a.pay_time between '{1}' and '{2}'
and pay_time=add_time
and mid in (

'1.103865',
'1.103861',
'1.103862',
'1.103863',
'1.103864',
'1.103860',
'1.103856',
'1.103857',
'1.103858',
'1.103859',
'1.102543',
'1.102544',
'1.102550',
'1.102554',
'1.102555',
'1.102563',
'1.102565',
'1.102567',
'1.102568',
'1.102572',
'1.102573',
'1.102575',
'1.102608',
'1.102943',
'1.103322',
'1.103323',
'1.103324',
'1.103325',
'1.103326',
'1.103327',
'1.103328'
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

'1.103865.9.1',
'1.103861.9.1',
'1.103862.9.1',
'1.103863.9.1',
'1.103864.9.1',
'1.103860.9.1',
'1.103856.9.1',
'1.103857.9.1',
'1.103858.9.1',
'1.103859.9.1',
'1.102543.9.1',
'1.102544.9.1',
'1.102550.9.1',
'1.102554.9.1',
'1.102555.9.1',
'1.102563.9.1',
'1.102565.9.1',
'1.102567.9.1',
'1.102568.9.1',
'1.102572.9.1',
'1.102573.9.1',
'1.102575.9.1',
'1.102608.9.1',
'1.102943.9.1',
'1.103322.9.1',
'1.103323.9.1',
'1.103324.9.1',
'1.103325.9.1',
'1.103326.9.1',
'1.103327.9.1',
'1.103328.9.1'

) 
group by 1,2,3,4,5) b on a.pay_time=b.event_date and a.country=b.country and a.mid=b.mid and a.gender=b.gender and a.pid=b.pid
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

'1.103865.9.1',
'1.103861.9.1',
'1.103862.9.1',
'1.103863.9.1',
'1.103864.9.1',
'1.103860.9.1',
'1.103856.9.1',
'1.103857.9.1',
'1.103858.9.1',
'1.103859.9.1',
'1.102543.9.1',
'1.102544.9.1',
'1.102550.9.1',
'1.102554.9.1',
'1.102555.9.1',
'1.102563.9.1',
'1.102565.9.1',
'1.102567.9.1',
'1.102568.9.1',
'1.102572.9.1',
'1.102573.9.1',
'1.102575.9.1',
'1.102608.9.1',
'1.102943.9.1',
'1.103322.9.1',
'1.103323.9.1',
'1.103324.9.1',
'1.103325.9.1',
'1.103326.9.1',
'1.103327.9.1',
'1.103328.9.1'



)  
group by 1,2,3,4,5 ) c  on a.pay_time=c.event_date and a.country=c.country and a.mid=c.mid and a.gender=c.gender and a.pid=c.pid
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

'1.103865.9.1',
'1.103861.9.1',
'1.103862.9.1',
'1.103863.9.1',
'1.103864.9.1',
'1.103860.9.1',
'1.103856.9.1',
'1.103857.9.1',
'1.103858.9.1',
'1.103859.9.1',
'1.102543.9.1',
'1.102544.9.1',
'1.102550.9.1',
'1.102554.9.1',
'1.102555.9.1',
'1.102563.9.1',
'1.102565.9.1',
'1.102567.9.1',
'1.102568.9.1',
'1.102572.9.1',
'1.102573.9.1',
'1.102575.9.1',
'1.102608.9.1',
'1.102943.9.1',
'1.103322.9.1',
'1.103323.9.1',
'1.103324.9.1',
'1.103325.9.1',
'1.103326.9.1',
'1.103327.9.1',
'1.103328.9.1'


) 
group by 1,2,3,4,5) d  on a.pay_time=d.event_date and a.country=d.country and a.mid=d.mid and a.gender=d.gender and a.pid=d.pid
left join odoo_own.product_template e on a.pid=e.id
left join public.mid_map f on a.mid=f.mid
join public.item_cate h on e.product_no=h.item_no
ORDER BY 1,2,3,4
""".format(start_yesterday,start_input,end_input,end_tomorrow)

conn = get_redshift_test_conn()
df=pd.read_sql(sql,conn)
df.to_excel('new_goods{0}.xls'.format(end_tomorrow),index=False )