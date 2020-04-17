
import pandas as pd
import psycopg2
import os
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

select 
count(a.cid) as 点击pv,
count(DISTINCT a.cid) 点击uv
from spectrum_schema.user_trace a
where a.log_date>='2019-06-02'
and a.log_date<='2019-06-09'
and a.server_time BETWEEN '2019-06-09 16:00:00' and  '2019-06-16 15:59:59'
and a.event_type in ('click')
and a.country_code = 'in'
and a.mid = '1.1.6.2'
 '''


    sql2 = '''
select 
       b.front_id_leaf as 类目catid,
       b.front_id_leafname as 一级类目,
       count(a.cid) as 点击pv,
       count(DISTINCT a.cid) 点击uv
from spectrum_schema.user_trace a
left join analysts.front_id_name_wcm b on a.catid=b.front_id_leaf
where a.log_date>='2019-06-09'
and a.log_date<='2019-06-16'
and a.server_time BETWEEN '2019-06-09 16:00:00' and  '2019-06-16 15:59:59'
and a.event_type in ('click')
and a.country_code = 'in'
and a.mid like '1.2.2.%'
and a.catid in ('51','52','53','54','55','56','57','58','59','60','61','62','63','64','65','66','67')
group by 1,2
order by 1,2
'''

    sql3 = '''
select 
       b.front_id_leaf as 类目catid,
       b.front_id_leafname as 二级类目,
       count(a.cid) as 点击pv,
       count(DISTINCT a.cid) 点击uv
from spectrum_schema.user_trace a
left join analysts.front_id_name_wcm b on a.catid=b.front_id_leaf
where a.log_date>='2019-06-09'
and a.log_date<='2019-06-16'
and a.server_time BETWEEN '2019-06-09 16:00:00' and  '2019-06-16 15:59:59'
and a.event_type in ('click')
and a.country_code = 'in'
and a.mid like '1.2.4.%'
and a.catid in (select front_id_leaf
                from analysts.front_id_name_wcm
                where length(front_id_leaf) in (5,6))
group by 1,2
order by 1,2
'''

    sql4 = '''
select
       b.front_id_leaf as 类目catid,
       b.front_id_leafname as 三级类目,
       count(a.cid) as 点击pv,
       count(DISTINCT a.cid) 点击uv
from spectrum_schema.user_trace a
left join analysts.front_id_name_wcm b on a.catid=b.front_id_leaf
where a.log_date>='2019-06-09'
and a.log_date<='2019-06-16'
and a.server_time BETWEEN '2019-06-09 16:00:00' and  '2019-06-16 15:59:59'
and a.event_type in ('click')
and a.country_code = 'in'
and a.mid like '1.2.5.%'
and a.catid in (select front_id_leaf
                from analysts.front_id_name_wcm
                where length(front_id_leaf) =10)
group by 1,2
order by 1,2
'''

    sql5 = '''
select 
       c.front_id, b.front_id_leafname,
       count(cid) 曝光pv,
       count(distinct cid) 曝光uv
from (
select split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) as front_id, a.cid,a.server_time
from spectrum_schema.user_trace a
where mid = '1.8.9.1'
and event_type = 'product'
and a.country_code = 'in'
and a.log_date>='2019-06-09'
and a.log_date<='2019-06-16'
and a.server_time BETWEEN '2019-06-09 16:00:00' and  '2019-06-16 15:59:59'
and split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) is not null )c
left join analysts.front_id_name_wcm b on c.front_id = b. front_id_leaf
where front_id_leafname is not NULL
GROUP BY 1,2
ORDER BY 1,2
'''

    sql6 = '''
select 
       b.front_id_one, b.front_one_name,
       count(cid) 曝光pv,
       count(distinct cid) 曝光uv
from (
select split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) as front_id, a.cid,a.server_time
from spectrum_schema.user_trace a
where mid = '1.8.9.1'
and event_type = 'product'
and a.country_code = 'in'
and a.log_date>='2019-06-09'
and a.log_date<='2019-06-16'
and a.server_time BETWEEN '2019-06-09 16:00:00' and  '2019-06-16 15:59:59'
and split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) is not null )c
left join analysts.front_id_name_wcm b on c.front_id = b. front_id_leaf
where front_id_leafname is not NULL
GROUP BY 1,2
ORDER BY 1,2
'''

    sql7 = '''
select 
       c.front_id,
       b.front_id_leafname,
       count(distinct cid) as 点击uv
from (
select split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) as front_id,a.cid,a.server_time
from spectrum_schema.user_trace a
where mid = '1.8.9.1'
and event_type = 'click'
and a.country_code = 'in'
and a.log_date>='2019-06-09'
and a.log_date<='2019-06-16'
and a.server_time BETWEEN '2019-06-09 16:00:00' and  '2019-06-16 15:59:59'
and split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) is not null )c
left join analysts.front_id_name_wcm b on c.front_id = b. front_id_leaf
where front_id_leafname is not NULL
GROUP BY 1,2
ORDER BY 1
'''

    sql8 = '''
select 
       b.front_id_one,
       b.front_one_name,
       count(distinct cid) as 点击uv
from (
select split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) as front_id,a.cid,a.server_time
from spectrum_schema.user_trace a
where mid = '1.8.9.1'
and event_type = 'click'
and a.country_code = 'in'
and a.log_date>='2019-06-09'
and a.log_date<='2019-06-16'
and a.server_time BETWEEN '2019-06-09 16:00:00' and  '2019-06-16 15:59:59'
and split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) is not null )c
left join analysts.front_id_name_wcm b on c.front_id = b. front_id_leaf
where front_id_leafname is not NULL
GROUP BY 1,2
ORDER BY 1
'''


    sql9 = '''

select 
       aa.front_id, 
       c.front_id_leafname,
       count(distinct aa.cid) as 下单uv,
       count(distinct bb.order_name) as 订单数,
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
and a.log_date>='2019-06-09'
and a.log_date<='2019-06-16'
and a.server_time BETWEEN '2019-06-09 16:00:00' and  '2019-06-16 15:59:59' 
and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) )>0) aa
inner join ( select date (t1.order_at + interval'8 hour') as order_at , t1.order_name,
                   t1.user_id,t2. item_no,t3.pid, t2.origin_qty, sum(t2.origin_qty*t2.origin_real)origin_total
            from jiayundw_dm.sale_order_info_df t1
            left join jiayundw_dm.sale_order_line_df t2 on t1. order_name = t2.order_name
            left join dwd.product_info t3 on t2.item_no = t3.item_no
            where t1.order_at between  '2019-06-09 16:00:00' and  '2019-06-16 15:59:59' 
            group by 1,2,3,4,5,6)bb
on aa.uid = bb.user_id
and aa.pid = bb.pid
and aa.server_time = date(bb.order_at + interval '8 hour')
left join analysts.front_id_name_wcm c on aa.front_id = c.front_id_leaf
GROUP BY 1,2
order by 1,2



'''

    sql11 = '''

select 
       c.front_id_one, 
       c.front_one_name,
       count(distinct aa.cid) as 下单uv,
       count(distinct bb.order_name) as 订单数,
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
and a.log_date>='2019-06-09'
and a.log_date<='2019-06-16'
and a.server_time BETWEEN '2019-06-09 16:00:00' and  '2019-06-16 15:59:59' 
and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) )>0) aa
inner join ( select date (t1.order_at + interval'8 hour') as order_at , t1.order_name,
                   t1.user_id,t2. item_no,t3.pid, t2.origin_qty, sum(t2.origin_qty*t2.origin_real)origin_total
            from jiayundw_dm.sale_order_info_df t1
            left join jiayundw_dm.sale_order_line_df t2 on t1. order_name = t2.order_name
            left join dwd.product_info t3 on t2.item_no = t3.item_no
            where t1.order_at between  '2019-06-09 16:00:00' and  '2019-06-16 15:59:59' 
            group by 1,2,3,4,5,6)bb
on aa.uid = bb.user_id
and aa.pid = bb.pid
and aa.server_time = date(bb.order_at + interval '8 hour')
left join analysts.front_id_name_wcm c on aa.front_id = c.front_id_leaf
GROUP BY 1,2
order by 1,2



'''

    sql12 = '''

select 
       aa.front_id, 
       c.front_id_leafname,
       count(distinct aa.cid) as 支付uv,
       count(distinct bb.order_name) as 订单数,
       sum(bb.origin_total) as gmv
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
and a.log_date>='2019-06-09'
and a.log_date<='2019-06-16'
and a.server_time BETWEEN '2019-06-09 16:00:00' and  '2019-06-16 15:59:59' 
and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) )>0) aa
inner join ( select date(t1.create_at + interval '8 hour') as create_at , t1.order_name,
            
                   t1.user_id,t2. item_no,t3.pid, t2.origin_qty, sum(t2.origin_qty*t2.origin_real)origin_total
            from jiayundw_dm.sale_order_info_df t1
            left join jiayundw_dm.sale_order_line_df t2 on t1. order_name = t2.order_name
            left join dwd.product_info t3 on t2.item_no = t3.item_no
            where t1.create_at between  '2019-06-09 16:00:00' and  '2019-06-16 15:59:59' 

            group by 1,2,3,4,5,6)bb
on aa.uid = bb.user_id
and aa.pid = bb.pid
and aa.server_time = date(bb.create_at + interval '8 hour')

left join analysts.front_id_name_wcm c on aa.front_id = c.front_id_leaf
GROUP BY 1,2
order by 1,2



'''

    sql13 = '''
select 
       c.front_id_one, 
       c.front_one_name,
       count(distinct aa.cid) as 支付uv,
       count(distinct bb.order_name) as 订单数,
       sum(bb.origin_total) as gmv
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
and a.log_date>='2019-06-09'
and a.log_date<='2019-06-16'
and a.server_time BETWEEN '2019-06-09 16:00:00' and  '2019-06-16 15:59:59' 
and length(split_part(split_part(replace(a.url,'//',''),'categoryId=',2),'&',1) )>0) aa
inner join ( select date(t1.create_at + interval '8 hour') as create_at , t1.order_name,
                   t1.user_id,t2. item_no,t3.pid, t2.origin_qty, sum(t2.origin_qty*t2.origin_real)origin_total
            from jiayundw_dm.sale_order_info_df t1
            left join jiayundw_dm.sale_order_line_df t2 on t1. order_name = t2.order_name
            left join dwd.product_info t3 on t2.item_no = t3.item_no
            where t1.create_at between  '2019-06-09 16:00:00' and  '2019-06-16 15:59:59' 
            group by 1,2,3,4,5,6)bb
on aa.uid = bb.user_id
and aa.pid = bb.pid
and aa.server_time = date(bb.create_at + interval '8 hour')
left join analysts.front_id_name_wcm c on aa.front_id = c.front_id_leaf
GROUP BY 1,2
order by 1,2



'''

    data1 = pd.read_sql(sql1,conn)
    data2 = pd.read_sql(sql2,conn)
    data3 = pd.read_sql(sql3,conn)
    data4 = pd.read_sql(sql4,conn)
    data5 = pd.read_sql(sql5,conn)
    data6 = pd.read_sql(sql6,conn)
    data7 = pd.read_sql(sql7,conn)
    data8 = pd.read_sql(sql8,conn)
    data9 = pd.read_sql(sql9,conn)
    data11 = pd.read_sql(sql11,conn)
    data12 = pd.read_sql(sql12,conn)
    data13 = pd.read_sql(sql13,conn)

    return data1,data2,data3,data4,data5,data6,data7,data8,data9,data11,data12,data13


if __name__ == '__main__':

    con = get_redshift_test_conn()
    data1,data2,data3,data4,data5,data6,data7,data8,data9,data11,data12,data13 = get_data(con)


    writer = pd.ExcelWriter('Category_week.xlsx')
    data1.to_excel(writer, sheet_name='Category_Tab', index=False)
    data2.to_excel(writer, sheet_name='1级点击pv_uv', index=False)
    data3.to_excel(writer, sheet_name='2级点击pv_uv', index=False)
    data4.to_excel(writer, sheet_name='3级点击pv_uv', index=False)
    data5.to_excel(writer, sheet_name='商品曝光_23级', index=False)
    data6.to_excel(writer, sheet_name='商品曝光_1级', index=False)
    data7.to_excel(writer, sheet_name='商品点击_23级', index=False)
    data8.to_excel(writer, sheet_name='商品点击_1级', index=False)
    data9.to_excel(writer, sheet_name='商品下单_23级', index=False)
    data11.to_excel(writer, sheet_name='商品下单_1级', index=False)
    data12.to_excel(writer, sheet_name='商品支付_23级', index=False)
    data13.to_excel(writer, sheet_name='商品支付_1级', index=False)
    os.chdir("F:/Python")
    writer.save()



