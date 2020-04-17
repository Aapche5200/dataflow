import pandas as pd
import psycopg2
import os
import time


def get_redshift_test_conn():
    conn = psycopg2.connect(
        user='yinshushan',
        password='Yinshushan123',
        host='jiayundatatest.cls0csjdlwvj.us-west-2.redshift.amazonaws.com',
        port=5439,
        database='jiayundata')
    return conn


def get_data(conn):
    sql1 = '''
select bb.类目catid, bb.归因类目 as 一级类目, aa.点击pv, aa.点击uv,
       bb.曝光pv as 商品曝光pv, bb.曝光uv as 商品曝光uv, bb.点击pv as 商品点击pv , bb.点击uv as 商品点击uv,ee.加购uv as 商品加购uv,
       cc.下单uv, dd.支付uv, dd.支付订单数,dd.支付gmv
from (
select  b.front_id_leaf as 类目catid,
       b.front_id_leafname as 一级类目,
       count(a.cid) as 点击pv,
       count(DISTINCT a.cid) 点击uv
from spectrum_schema.user_trace a
left join analysts.front_id_name_wcm b on a.catid=b.front_id_leaf
where a.log_date>='2019-06-08'
and a.log_date<='2019-06-24'
and a.server_time BETWEEN '2019-06-08 16:00:00' and  '2019-06-24 15:59:59' 
and a.event_type in ('click')
and a.country_code = 'in'
and a.mid like '1.2.2.%'
and a.catid in ('51','52','53','54','55','56','57','58','59','60','61','62','63','64','65','66','67')
group by 1,2
order by 1,2) aa 
right join 
(select 
       c.front_id as 类目catid, 
       b.front_id_leafname 归因类目,
       --b. front_id_one 一级类目catid, b.front_one_name 一级类目,
       --b.front_id_two 二级类目catid, b.front_two_name 二级类目,
       count( case when c.event_type = 'product' then cid else null end) 曝光pv,
       count( distinct case when c.event_type = 'product' then cid else null end) 曝光uv,
       count( case when c.event_type = 'click' then cid else null end) 点击pv,
       count( distinct case when c.event_type = 'click' then cid else null end) 点击uv
  from (
  select split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) as front_id, 
      a.cid,a.server_time,a.event_type
  from spectrum_schema.user_trace a
  where mid = '1.8.9.1'
  and a.event_type in ('product','click')
  and a.country_code = 'in'
  and a.log_date>='2019-06-08'
  and a.log_date<='2019-06-24'
  and a.server_time BETWEEN '2019-06-08 16:00:00' and  '2019-06-24 15:59:59' 
  and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1))=2 )c
  left join analysts.front_id_name_wcm b on c.front_id = b. front_id_leaf
where front_id_leafname is not NULL
GROUP BY 1,2
ORDER BY 1,2) bb on bb.类目catid = aa.类目catid
left join 
(select  aa.front_id, c.front_id_leafname,
       count(distinct bb.cid) as 加购uv
from (
select split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) as front_id,a.cid,a.pid
from spectrum_schema.user_trace a
where mid = '1.8.9.1'
and event_type = 'click'
and a.country_code = 'in'
  and a.log_date>='2019-06-08'
  and a.log_date<='2019-06-24'
  and a.server_time BETWEEN '2019-06-08 16:00:00' and  '2019-06-24 15:59:59' 
and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) ) =2 ) aa
left join 
(select * from public.cart_buynow_result a
where a.time_stamp   between '2019-06-08  16:00:00' and '2019-06-24  15:59:59'
and a.m_mid='1.8.9.1' and a.m_fr='1.2') bb
on aa.cid = bb.cid
and aa.pid = bb.pid
left join analysts.front_id_name_wcm c on aa.front_id = c.front_id_leaf
GROUP BY 1,2) ee on bb.类目catid = ee.front_id 
left join (
select aa.front_id, 
       c.front_id_leafname,count(distinct aa.cid) as 下单uv
from (select split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) as front_id,a.cid,a.pid
from spectrum_schema.user_trace a
where mid = '1.8.9.1'
and event_type = 'click'
and a.country_code = 'in'
and a.log_date>='2019-06-08'
and a.log_date<='2019-06-24'
and a.server_time BETWEEN '2019-06-08 16:00:00' and  '2019-06-24 15:59:59' 
and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) ) = 2) aa
left join 
(select * from analysts.user_submit_success_detail 
where pay_time between '2019-06-08' and '2019-06-24'
and mid='1.2') bb
on aa.cid = bb.cid
and aa.pid = bb.pid
left join analysts.front_id_name_wcm c on aa.front_id = c.front_id_leaf
GROUP BY 1,2
order by 1,2) cc
on bb.类目catid = cc.front_id 
left join (select aa.front_id, c.front_id_leafname,
       count(distinct aa.cid) as 支付uv,
       count(distinct aa.order_name) as 支付订单数,
       sum(coalesce(aa.pno_num, '0')*aa.price) as 支付gmv
from (select split_part(split_part(replace(a.m_url,'//',''),'categoryId=',2),'&',1) as front_id,a.cid,a.pid,order_name,pno_num,price
from analysts.user_pay_success_detail_0626  a
where length(split_part(split_part(replace(a.m_url,'//',''),'categoryId=',2),'&',1) ) =2
and mid='1.2'
and pay_time  between '2019-06-08' and '2019-06-24'
) aa
--and aa.server_time = date(bb.order_at + interval '8 hour')
left join analysts.front_id_name_wcm c on aa.front_id = c.front_id_leaf
GROUP BY 1,2
order by 1,2)dd on bb.类目catid = dd.front_id 
order by 1,2

 '''

    sql2 = '''

select  bb.类目catid, bb.归因类目 as 二级类目, bb.一级类目catid,bb.一级类目, aa.点击pv, aa.点击uv,
       bb.曝光pv as 商品曝光pv, bb.曝光uv as 商品曝光uv, bb.点击pv as 商品点击pv , bb.点击uv as 商品点击uv,ee.加购uv as 商品加购uv,
       cc.下单uv, dd.支付uv, dd.支付订单数,dd.支付gmv
from (
select 
       b.front_id_one as 一级类目catid,
       b.front_one_name as 一级类目,
       b.front_id_leaf as 类目catid,
       b.front_id_leafname as 二级类目,
       count(a.cid) as 点击pv,
       count(DISTINCT a.cid) 点击uv from 
(select DISTINCT cid,catid from spectrum_schema.user_trace a
where a.log_date>='2019-06-08'
and a.log_date<='2019-06-24'
and a.server_time BETWEEN '2019-06-08 16:00:00' and  '2019-06-24 15:59:59' 
and a.event_type in ('click')
and a.country_code = 'in'
and a.mid like '1.2.4.%'
and a.catid in (select front_id_leaf
                from analysts.front_id_name_wcm
                where length(front_id_leaf) in (5,6)))  a
left join analysts.front_id_name_wcm b on a.catid=b.front_id_leaf
group by 1,2,3,4
order by 1,2,3,4) aa 
right join 
(select 
       c.front_id as 类目catid, 
       b.front_id_leafname 归因类目,
       b. front_id_one 一级类目catid, b.front_one_name 一级类目,
       --b.front_id_two 二级类目catid, b.front_two_name 二级类目,
       count( case when c.event_type = 'product' then cid else null end) 曝光pv,
       count( distinct case when c.event_type = 'product' then cid else null end) 曝光uv,
       count( case when c.event_type = 'click' then cid else null end) 点击pv,
       count( distinct case when c.event_type = 'click' then cid else null end) 点击uv
  from (
  select split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) as front_id, 
      a.cid,a.server_time,a.event_type
  from spectrum_schema.user_trace a
  where mid = '1.8.9.1'
  and a.event_type in ('product','click')
  and a.country_code = 'in'
and a.log_date>='2019-06-08'
and a.log_date<='2019-06-24'
and a.server_time BETWEEN '2019-06-08 16:00:00' and  '2019-06-24 15:59:59' 
  and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1)) in (5,6) )c
  left join analysts.front_id_name_wcm b on c.front_id = b. front_id_leaf
where front_id_leafname is not NULL
GROUP BY 1,2,3,4
) bb
on bb.类目catid = aa.类目catid 
left  JOIN

(select  aa.front_id, c.front_id_leafname,
       count(distinct bb.cid) as 加购uv
from (
select split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) as front_id,a.cid,a.pid
from spectrum_schema.user_trace a
where mid = '1.8.9.1'
and event_type = 'click'
and a.country_code = 'in'
and a.log_date>='2019-06-08'
and a.log_date<='2019-06-24'
and a.server_time BETWEEN '2019-06-08 16:00:00' and  '2019-06-24 15:59:59' 
and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) ) in (5,6)) aa
left join 
(select * from public.cart_buynow_result a
where a.time_stamp   between '2019-06-08  16:00:00' and '2019-06-24  15:59:59'
and a.m_mid='1.8.9.1' and a.m_fr='1.2') bb
on aa.cid = bb.cid
and aa.pid = bb.pid
left join analysts.front_id_name_wcm c on aa.front_id = c.front_id_leaf
GROUP BY 1,2) ee on aa.类目catid = ee.front_id 
left join (
select aa.front_id, c.front_id_leafname,count(distinct aa.cid) as 下单uv from 
(select split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) as front_id,a.cid,a.pid
from spectrum_schema.user_trace a
where mid = '1.8.9.1'
and event_type = 'click'
and a.country_code = 'in'
and a.log_date>='2019-06-08'
and a.log_date<='2019-06-24'
and a.server_time BETWEEN '2019-06-08 16:00:00' and  '2019-06-24 15:59:59' 
and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) ) in (5,6)) aa
left join 
(select * from analysts.user_submit_success_detail 
where pay_time between '2019-06-08' and '2019-06-24'
and mid='1.2') bb
on aa.cid= bb.cid
and aa.pid = bb.pid
left join analysts.front_id_name_wcm c on aa.front_id = c.front_id_leaf
GROUP BY 1,2
order by 1,2) cc
on bb.类目catid = cc.front_id 
left join (select aa.front_id, c.front_id_leafname,
       count(distinct aa.cid) as 支付uv,
       count(distinct aa.order_name) as 支付订单数,
       sum(coalesce(aa.pno_num, '0')*aa.price) as 支付gmv
from (select split_part(split_part(replace(a.m_url,'//',''),'categoryId=',2),'&',1) as front_id,a.cid,a.pid,order_name,pno_num,price
from analysts.user_pay_success_detail_0626  a
where length(split_part(split_part(replace(a.m_url,'//',''),'categoryId=',2),'&',1) ) in (5,6)
and mid='1.2'
and pay_time  between '2019-06-08' and '2019-06-24'
) aa
--and aa.server_time = date(bb.order_at + interval '8 hour')
left join analysts.front_id_name_wcm c on aa.front_id = c.front_id_leaf
GROUP BY 1,2
order by 1,2)dd
on bb.类目catid = dd.front_id 
order by 1,2,3,4

'''

    sql3 = '''

select bb.类目catid, bb.归因类目 as 三级类目, bb.一级类目catid,bb.一级类目, 
       bb.二级类目catid, bb.二级类目,
       aa.点击pv, aa.点击uv,
       bb.曝光pv as 商品曝光pv, bb.曝光uv as 商品曝光uv, bb.点击pv as 商品点击pv , bb.点击uv as 商品点击uv,ee.加购uv as 商品加购uv,
       cc.下单uv,dd.支付uv, dd.支付订单数,dd.支付gmv
from (
select b.front_id_one as 一级类目catid,
       b.front_one_name as 一级类目,
       b.front_id_two as 二级类目catid,
       b.front_two_name as 二级类目,
       b.front_id_leaf as 类目catid,
       b.front_id_leafname as 三级类目,
       count(a.cid) as 点击pv,
       count(DISTINCT a.cid) 点击uv
from 
(select DISTINCT  cid,catid 
from spectrum_schema.user_trace a
where a.log_date>='2019-06-08'
and a.log_date<='2019-06-24'
and a.server_time BETWEEN '2019-06-08 16:00:00' and  '2019-06-24 15:59:59'  
and a.event_type in ('click')
and a.country_code = 'in'
and a.mid like '1.2.5.%'
and a.catid in (select front_id_leaf
                from analysts.front_id_name_wcm
                where length(front_id_leaf) =10)) a
left join analysts.front_id_name_wcm b on a.catid=b.front_id_leaf
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6) aa 
left join 
(select c.front_id as 类目catid, 
       b.front_id_leafname 归因类目,
       b. front_id_one 一级类目catid, b.front_one_name 一级类目,
       b.front_id_two 二级类目catid, b.front_two_name 二级类目,
       count( case when c.event_type = 'product' then cid else null end) 曝光pv,
       count( distinct case when c.event_type = 'product' then cid else null end) 曝光uv,
       count( case when c.event_type = 'click' then cid else null end) 点击pv,
       count( distinct case when c.event_type = 'click' then cid else null end) 点击uv
  from (
  select split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) as front_id, 
      a.cid,a.server_time,a.event_type
  from spectrum_schema.user_trace a
  where mid = '1.8.9.1'
  and a.event_type in ('product','click')
  and a.country_code = 'in'
and a.log_date>='2019-06-08'
and a.log_date<='2019-06-24'
and a.server_time BETWEEN '2019-06-08 16:00:00' and  '2019-06-24 15:59:59' 
  and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1))= 10)c
  left join analysts.front_id_name_wcm b on c.front_id = b. front_id_leaf
where front_id_leafname is not NULL
GROUP BY 1,2,3,4,5,6) bb on bb.类目catid = aa.类目catid 
left  JOIN

(select  aa.front_id, c.front_id_leafname,
       count(distinct bb.cid) as 加购uv
from (
select split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) as front_id,a.cid,a.pid
from spectrum_schema.user_trace a
where mid = '1.8.9.1'
and event_type = 'click'
and a.country_code = 'in'
and a.log_date>='2019-06-08'
and a.log_date<='2019-06-24'
and a.server_time BETWEEN '2019-06-08 16:00:00' and  '2019-06-24 15:59:59' 
and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) ) =10 ) aa
left join 
(select * from public.cart_buynow_result a
where a.time_stamp   between '2019-06-08  16:00:00' and '2019-06-24  15:59:59'
and a.m_mid='1.8.9.1' and a.m_fr='1.2') bb
on aa.cid = bb.cid
and aa.pid = bb.pid
left join analysts.front_id_name_wcm c on aa.front_id = c.front_id_leaf
GROUP BY 1,2) ee on aa.类目catid = ee.front_id 
left join (
select aa.front_id,c.front_id_leafname,count(distinct aa.cid) as 下单uv
from (
select split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) as front_id,
       a.cid,a.pid
from spectrum_schema.user_trace a
where mid = '1.8.9.1'
and event_type = 'click'
and a.country_code = 'in'
and a.log_date>='2019-06-08'
and a.log_date<='2019-06-24'
and a.server_time BETWEEN '2019-06-08 16:00:00' and  '2019-06-24 15:59:59' 
and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) )= 10) aa
left join 
(select * from analysts.user_submit_success_detail 
where pay_time between '2019-06-08' and '2019-06-24'
and mid='1.2') bb
on aa.cid = bb.cid
and aa.pid = bb.pid
left join analysts.front_id_name_wcm c on aa.front_id = c.front_id_leaf
GROUP BY 1,2
order by 1,2) cc
on bb.类目catid = cc.front_id
left join (select aa.front_id, c.front_id_leafname,
       count(distinct aa.cid) as 支付uv,
       count(distinct aa.order_name) as 支付订单数,
       sum(coalesce(aa.pno_num, '0')*aa.price) as 支付gmv
from (select split_part(split_part(replace(a.m_url,'//',''),'categoryId=',2),'&',1) as front_id,a.cid,a.pid,order_name,pno_num,price
from analysts.user_pay_success_detail_0626  a
where length(split_part(split_part(replace(a.m_url,'//',''),'categoryId=',2),'&',1) )= 10
and mid='1.2'
and pay_time  between '2019-06-08' and '2019-06-24'
) aa
--and aa.server_time = date(bb.order_at + interval '8 hour')
left join analysts.front_id_name_wcm c on aa.front_id = c.front_id_leaf
GROUP BY 1,2
order by 1,2)dd
on bb.类目catid = dd.front_id
order by 1,2,3,4,5,6


'''


    data1 = pd.read_sql(sql1, conn)
    data2 = pd.read_sql(sql2, conn)
    data3 = pd.read_sql(sql3, conn)

    return data1, data2, data3


if __name__ == '__main__':
    con = get_redshift_test_conn()
    data1, data2, data3 = get_data(con)

    writer = pd.ExcelWriter('Category_re' + time.strftime('%Y-%m-%d', time.localtime(time.time())) + '.xlsx')
    data1.to_excel(writer, sheet_name='1级icon', index=False)
    data2.to_excel(writer, sheet_name='2级icon', index=False)
    data3.to_excel(writer, sheet_name='3级icon', index=False)

    os.chdir("F:/Python")
    writer.save()



