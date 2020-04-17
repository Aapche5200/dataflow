
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
select date(a.server_time + interval'8 hour') event_date,
count(a.cid) as 点击pv,
count(DISTINCT a.cid) 点击uv
from spectrum_schema.user_trace a
where a.log_date>='2019-06-16'
and a.log_date<='2019-06-23'
and a.server_time BETWEEN '2019-06-16 16:00:00' and  '2019-06-23 15:59:59' 
and a.event_type in ('click')
and a.country_code = 'in'
and a.mid = '1.1.6.2'
group by 1
order by 1

 '''


    sql2 = '''

select bb.日期, bb.类目catid, bb.归因类目 as 一级类目, aa.点击pv, aa.点击uv,
       bb.曝光pv as 商品曝光pv, bb.曝光uv as 商品曝光uv, bb.点击pv as 商品点击pv , bb.点击uv as 商品点击uv,
       cc.下单uv, cc.下单gmv, cc.订单数,dd.支付uv, dd.支付订单数,dd.支付gmv
from (
select date(a.server_time + interval'8 hour') as 日期, 
       b.front_id_leaf as 类目catid,
       b.front_id_leafname as 一级类目,
       count(a.cid) as 点击pv,
       count(DISTINCT a.cid) 点击uv
from spectrum_schema.user_trace a
left join analysts.front_id_name_wcm b on a.catid=b.front_id_leaf
where a.log_date>='2019-06-16'
and a.log_date<='2019-06-23'
and a.server_time BETWEEN '2019-06-16 16:00:00' and  '2019-06-23 15:59:59' 
and a.event_type in ('click')
and a.country_code = 'in'
and a.mid like '1.2.2.%'
and a.catid in ('51','52','53','54','55','56','57','58','59','60','61','62','63','64','65','66','67')
group by 1,2,3
order by 1,2,3) aa 
right join 
(select date(server_time + interval'8 hour') as 日期, 
       c.front_id as 类目catid, 
       b.front_id_leafname 归因类目,

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
  and a.log_date>='2019-06-16'
  and a.log_date<='2019-06-23'
  and a.server_time BETWEEN '2019-06-16 16:00:00' and  '2019-06-23 15:59:59' 
  and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1))=2 )c
  left join analysts.front_id_name_wcm b on c.front_id = b. front_id_leaf
where front_id_leafname is not NULL
GROUP BY 1,2,3
ORDER BY 1,2,3) bb
on bb.类目catid = aa.类目catid and aa.日期 = bb.日期 
left join (
select aa.server_time, aa.front_id, 
       c.front_id_leafname,count(distinct aa.cid) as 下单uv,count(distinct bb.order_name) as 订单数,
       sum(bb.origin_total) as 下单gmv
from (
select date(server_time + interval '8 hour') as server_time,
       split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) as front_id,
       a.cid,a.pid,b.uid
from spectrum_schema.user_trace a
left join  ( select cid,max(uid) as uid
             from public.wp_cid_collect_union b
             GROUP BY 1 ) b on a.cid = b.cid
where mid = '1.8.9.1'
and event_type = 'click'
and a.country_code = 'in'
and a.log_date>='2019-06-16'
and a.log_date<='2019-06-23'
and a.server_time BETWEEN '2019-06-16 16:00:00' and  '2019-06-23 15:59:59' 
and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) ) = 2) aa
inner join ( select date (t1.order_at + interval'8 hour') as order_at , t1.order_name,
                   t1.user_id,t2. item_no,t3.pid, t2.origin_qty, sum(t2.origin_qty*t2.origin_real)origin_total
            from jiayundw_dm.sale_order_info_df t1
            left join jiayundw_dm.sale_order_line_df t2 on t1. order_name = t2.order_name
            left join dwd.product_info t3 on t2.item_no = t3.item_no
            where t1.order_at between  '2019-06-16 16:00:00' and  '2019-06-23 15:59:59' 
            group by 1,2,3,4,5,6)bb
on aa.uid = bb.user_id
and aa.pid = bb.pid
and aa.server_time = date(bb.order_at + interval '8 hour')
left join analysts.front_id_name_wcm c on aa.front_id = c.front_id_leaf
GROUP BY 1,2,3
order by 1,2,3) cc
on bb.类目catid = cc.front_id and bb.日期 = cc.server_time
left join (select aa.server_time, aa.front_id, c.front_id_leafname,
       count(distinct aa.cid) as 支付uv,count(distinct bb.order_name) as 支付订单数,
       sum(bb.origin_total) as 支付gmv
from (
select date(server_time + interval '8 hour') as server_time,
       split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) as front_id,
       a.cid,a.pid,b.uid
from spectrum_schema.user_trace a
left join  ( select cid,max(uid) as uid
             from public.wp_cid_collect_union b
             GROUP BY 1 ) b on a.cid = b.cid
where mid = '1.8.9.1'
and event_type = 'click'
and a.country_code = 'in'
and a.log_date>='2019-06-16'
and a.log_date<='2019-06-23'
and a.server_time BETWEEN '2019-06-16 16:00:00' and  '2019-06-23 15:59:59' 
and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) ) =2) aa
inner join ( select date(t1.create_at + interval '8 hour') as create_at , t1.order_name,

                   t1.user_id,t2. item_no,t3.pid, t2.origin_qty, sum(t2.origin_qty*t2.origin_real)origin_total
            from jiayundw_dm.sale_order_info_df t1
            left join jiayundw_dm.sale_order_line_df t2 on t1. order_name = t2.order_name
            left join dwd.product_info t3 on t2.item_no = t3.item_no
            where t1.create_at between  '2019-06-16 16:00:00' and  '2019-06-23 15:59:59' 
            group by 1,2,3,4,5,6)bb
on aa.uid = bb.user_id
and aa.pid = bb.pid
and aa.server_time = date(bb.create_at + interval '8 hour')
--and aa.server_time = date(bb.order_at + interval '8 hour')
left join analysts.front_id_name_wcm c on aa.front_id = c.front_id_leaf
GROUP BY 1,2,3
order by 1,2,3)dd
on bb.类目catid = dd.front_id and bb.日期 = dd.server_time
order by 1,2,3

'''

    sql3 = '''

select bb.日期, bb.类目catid, bb.归因类目 as 二级类目, bb.一级类目catid,bb.一级类目, aa.点击pv, aa.点击uv,
       bb.曝光pv as 商品曝光pv, bb.曝光uv as 商品曝光uv, bb.点击pv as 商品点击pv , bb.点击uv as 商品点击uv,
       cc.下单uv, cc.下单gmv, cc.订单数,dd.支付uv, dd.支付订单数,dd.支付gmv
from (
select date(a.server_time + interval'8 hour') as 日期, 
       b.front_id_one as 一级类目catid,
       b.front_one_name as 一级类目,
       b.front_id_leaf as 类目catid,
       b.front_id_leafname as 二级类目,
       count(a.cid) as 点击pv,
       count(DISTINCT a.cid) 点击uv
from spectrum_schema.user_trace a
left join analysts.front_id_name_wcm b on a.catid=b.front_id_leaf
where a.log_date>='2019-06-16'
and a.log_date<='2019-06-23'
and a.server_time BETWEEN '2019-06-16 16:00:00' and  '2019-06-23 15:59:59' 
and a.event_type in ('click')
and a.country_code = 'in'
and a.mid like '1.2.4.%'
and a.catid in (select front_id_leaf
                from analysts.front_id_name_wcm
                where length(front_id_leaf) in (5,6))
group by 1,2,3,4,5
order by 1,2,3,4,5) aa 
right join 
(select date(server_time + interval'8 hour') as 日期, 
       c.front_id as 类目catid, 
       b.front_id_leafname 归因类目,
       b. front_id_one 一级类目catid, b.front_one_name 一级类目,
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
  and a.log_date>='2019-06-16'
  and a.log_date<='2019-06-23'
  and a.server_time BETWEEN '2019-06-16 16:00:00' and  '2019-06-23 15:59:59' 
  and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1)) in (5,6) )c
  left join analysts.front_id_name_wcm b on c.front_id = b. front_id_leaf
where front_id_leafname is not NULL
GROUP BY 1,2,3,4,5
) bb
on bb.类目catid = aa.类目catid and aa.日期 = bb.日期 
left join (
select aa.server_time, aa.front_id, 
       c.front_id_leafname,count(distinct aa.cid) as 下单uv,count(distinct bb.order_name) as 订单数,
       sum(bb.origin_total) as 下单gmv
from (
select date(server_time + interval '8 hour') as server_time,
       split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) as front_id,
       a.cid,a.pid,b.uid
from spectrum_schema.user_trace a
left join  ( select cid,max(uid) as uid
             from public.wp_cid_collect_union b
             GROUP BY 1 ) b on a.cid = b.cid
where mid = '1.8.9.1'
and event_type = 'click'
and a.country_code = 'in'
and a.log_date>='2019-06-16'
and a.log_date<='2019-06-23'
and a.server_time BETWEEN '2019-06-16 16:00:00' and  '2019-06-23 15:59:59' 
and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) ) in (5,6)) aa
inner join ( select date (t1.order_at + interval'8 hour') as order_at , t1.order_name,
                   t1.user_id,t2. item_no,t3.pid, t2.origin_qty, sum(t2.origin_qty*t2.origin_real)origin_total
            from jiayundw_dm.sale_order_info_df t1
            left join jiayundw_dm.sale_order_line_df t2 on t1. order_name = t2.order_name
            left join dwd.product_info t3 on t2.item_no = t3.item_no
            where t1.order_at between  '2019-06-16 16:00:00' and  '2019-06-23 15:59:59' 
            group by 1,2,3,4,5,6)bb
on aa.uid = bb.user_id
and aa.pid = bb.pid
and aa.server_time = date(bb.order_at + interval '8 hour')
left join analysts.front_id_name_wcm c on aa.front_id = c.front_id_leaf
GROUP BY 1,2,3
order by 1,2,3) cc
on bb.类目catid = cc.front_id and bb.日期 = cc.server_time
left join (select aa.server_time, aa.front_id, c.front_id_leafname,
       count(distinct aa.cid) as 支付uv,count(distinct bb.order_name) as 支付订单数,
       sum(bb.origin_total) as 支付gmv
from (
select date(server_time + interval '8 hour') as server_time,
       split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) as front_id,
       a.cid,a.pid,b.uid
from spectrum_schema.user_trace a
left join  ( select cid,max(uid) as uid
             from public.wp_cid_collect_union b
             GROUP BY 1 ) b on a.cid = b.cid
where mid = '1.8.9.1'
and event_type = 'click'
and a.country_code = 'in'
and a.log_date>='2019-06-16'
and a.log_date<='2019-06-23'
and a.server_time BETWEEN '2019-06-16 16:00:00' and  '2019-06-23 15:59:59' 
and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) ) in (5,6)) aa
inner join ( select date(t1.create_at + interval '8 hour') as create_at , t1.order_name,
                 
                   t1.user_id,t2. item_no,t3.pid, t2.origin_qty, sum(t2.origin_qty*t2.origin_real)origin_total
            from jiayundw_dm.sale_order_info_df t1
            left join jiayundw_dm.sale_order_line_df t2 on t1. order_name = t2.order_name
            left join dwd.product_info t3 on t2.item_no = t3.item_no
            where t1.create_at between  '2019-06-16 16:00:00' and  '2019-06-23 15:59:59' 
        
            group by 1,2,3,4,5,6)bb
on aa.uid = bb.user_id
and aa.pid = bb.pid
and aa.server_time = date(bb.create_at + interval '8 hour')
left join analysts.front_id_name_wcm c on aa.front_id = c.front_id_leaf
GROUP BY 1,2,3
order by 1,2,3)dd
on bb.类目catid = dd.front_id and bb.日期 = dd.server_time
order by 1,2,3,4,5


'''

    sql4 = '''

select bb.日期, bb.类目catid, bb.归因类目 as 三级类目, bb.一级类目catid,bb.一级类目, 
       bb.二级类目catid, bb.二级类目,
       aa.点击pv, aa.点击uv,
       bb.曝光pv as 商品曝光pv, bb.曝光uv as 商品曝光uv, bb.点击pv as 商品点击pv , bb.点击uv as 商品点击uv,
       cc.下单uv, cc.下单gmv, cc.订单数,dd.支付uv, dd.支付订单数,dd.支付gmv
from (
select date(a.server_time + interval'8 hour') as 日期, 
       b.front_id_one as 一级类目catid,
       b.front_one_name as 一级类目,
       b.front_id_two as 二级类目catid,
       b.front_two_name as 二级类目,
       b.front_id_leaf as 类目catid,
       b.front_id_leafname as 三级类目,
       count(a.cid) as 点击pv,
       count(DISTINCT a.cid) 点击uv
from spectrum_schema.user_trace a
left join analysts.front_id_name_wcm b on a.catid=b.front_id_leaf
where a.log_date>='2019-06-16'
and a.log_date<='2019-06-23'
and a.server_time BETWEEN '2019-06-16 16:00:00' and  '2019-06-23 15:59:59' 
and a.event_type in ('click')
and a.country_code = 'in'
and a.mid like '1.2.5.%'
and a.catid in (select front_id_leaf
                from analysts.front_id_name_wcm
                where length(front_id_leaf) =10)
group by 1,2,3,4,5,6,7
order by 1,2,3,4,5,6,7) aa 
right join 
(select date(server_time + interval'8 hour') as 日期, 
       c.front_id as 类目catid, 
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
  and a.log_date>='2019-06-16'
  and a.log_date<='2019-06-23'
  and a.server_time BETWEEN '2019-06-16 16:00:00' and  '2019-06-23 15:59:59' 
  and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1))= 10)c
  left join analysts.front_id_name_wcm b on c.front_id = b. front_id_leaf
where front_id_leafname is not NULL
GROUP BY 1,2,3,4,5,6,7
) bb
on bb.类目catid = aa.类目catid and aa.日期 = bb.日期 
left join (
select aa.server_time, aa.front_id, 
       c.front_id_leafname,count(distinct aa.cid) as 下单uv,count(distinct bb.order_name) as 订单数,
       sum(bb.origin_total) as 下单gmv
from (
select date(server_time + interval '8 hour') as server_time,
       split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) as front_id,
       a.cid,a.pid,b.uid
from spectrum_schema.user_trace a
left join  ( select cid,max(uid) as uid
             from public.wp_cid_collect_union b
             GROUP BY 1 ) b on a.cid = b.cid
where mid = '1.8.9.1'
and event_type = 'click'
and a.country_code = 'in'
and a.log_date>='2019-06-16'
and a.log_date<='2019-06-23'
and a.server_time BETWEEN '2019-06-16 16:00:00' and  '2019-06-23 15:59:59' 
and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) )= 10) aa
inner join ( select date (t1.order_at + interval'8 hour') as order_at , t1.order_name,
                   t1.user_id,t2. item_no,t3.pid, t2.origin_qty, sum(t2.origin_qty*t2.origin_real)origin_total
            from jiayundw_dm.sale_order_info_df t1
            left join jiayundw_dm.sale_order_line_df t2 on t1. order_name = t2.order_name
            left join dwd.product_info t3 on t2.item_no = t3.item_no
            where t1.order_at between  '2019-06-16 16:00:00' and  '2019-06-23 15:59:59' 
            group by 1,2,3,4,5,6)bb
on aa.uid = bb.user_id
and aa.pid = bb.pid
and aa.server_time = date(bb.order_at + interval '8 hour')
left join analysts.front_id_name_wcm c on aa.front_id = c.front_id_leaf
GROUP BY 1,2,3
order by 1,2,3) cc
on bb.类目catid = cc.front_id and bb.日期 = cc.server_time
left join (select aa.server_time, aa.front_id, c.front_id_leafname,
       count(distinct aa.cid) as 支付uv,count(distinct bb.order_name) as 支付订单数,
       sum(bb.origin_total) as 支付gmv
from (
select date(server_time + interval '8 hour') as server_time,
       split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) as front_id,
       a.cid,a.pid,b.uid
from spectrum_schema.user_trace a
left join  ( select cid,max(uid) as uid
             from public.wp_cid_collect_union b
             GROUP BY 1 ) b on a.cid = b.cid
where mid = '1.8.9.1'
and event_type = 'click'
and a.country_code = 'in'
and a.log_date>='2019-06-16'
and a.log_date<='2019-06-23'
and a.server_time BETWEEN '2019-06-16 16:00:00' and  '2019-06-23 15:59:59' 
and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) ) =10 ) aa
inner join ( select date(t1.create_at + interval '8 hour') as create_at , t1.order_name,
                   --date (t1.order_at + interval'8 hour') as order_at,
                   t1.user_id,t2. item_no,t3.pid, t2.origin_qty, sum(t2.origin_qty*t2.origin_real)origin_total
            from jiayundw_dm.sale_order_info_df t1
            left join jiayundw_dm.sale_order_line_df t2 on t1. order_name = t2.order_name
            left join dwd.product_info t3 on t2.item_no = t3.item_no
            where t1.create_at between  '2019-06-16 16:00:00' and  '2019-06-23 15:59:59' 
            group by 1,2,3,4,5,6)bb
on aa.uid = bb.user_id
and aa.pid = bb.pid
and aa.server_time = date(bb.create_at + interval '8 hour')
--and aa.server_time = date(bb.order_at + interval '8 hour')
left join analysts.front_id_name_wcm c on aa.front_id = c.front_id_leaf
GROUP BY 1,2,3
order by 1,2,3)dd
on bb.类目catid = dd.front_id and bb.日期 = dd.server_time
order by 1,2,3,4,5,6,7

'''

    data1 = pd.read_sql(sql1,conn)
    data2 = pd.read_sql(sql2,conn)
    data3 = pd.read_sql(sql3,conn)
    data4 = pd.read_sql(sql4,conn)

    return data1,data2,data3,data4


if __name__ == '__main__':

    con = get_redshift_test_conn()
    data1,data2,data3,data4 = get_data(con)


    writer = pd.ExcelWriter('Category'+time.strftime('%Y-%m-%d',time.localtime(time.time()))+'.xlsx')
    data1.to_excel(writer, sheet_name='Category_Tab', index=False)
    data2.to_excel(writer, sheet_name='1级点击pv_uv', index=False)
    data3.to_excel(writer, sheet_name='2级点击pv_uv', index=False)
    data4.to_excel(writer, sheet_name='3级点击pv_uv', index=False)

    os.chdir("F:/Python")
    writer.save()



