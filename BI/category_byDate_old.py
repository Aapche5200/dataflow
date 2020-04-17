#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division

import logging
import os
from datetime import datetime, timedelta
from functools import reduce

import boto3
import pandas as pd

from tools import db_util

dags_ago = 0
current_time = datetime.now() - timedelta(days=dags_ago)

today = current_time.date()

yesterday = current_time.date() - timedelta(days=1)

start=yesterday

month_start=datetime(start.year,start.month,1)

uv_start = (current_time + timedelta(-2)).strftime('%Y-%m-%d')+" 16:00:00"
uv_end = (current_time+ timedelta(-1)).strftime('%Y-%m-%d')+" 16:00:00"
log_start=(current_time + timedelta(-3)).strftime('%Y-%m-%d')
log_end=(current_time+ timedelta(-0)).strftime('%Y-%m-%d')

interval=(datetime.now().date()-start).days



def get_cateIndexByDate(redshift_conn):
    sql_cateOne="""
    select 'total' seller_type,uv_all.create_date,uv_all.old_cate_one,impression_num,impression_item_num,click_num,uv,
            pay_order_num,pay_user_num,m_user_num,f_user_num,
            case when uv=0 then 0 else pay_user_num::NUMERIC/uv::NUMERIC end  cvr,
            case when impression_num=0 then 0 else origin_qty::NUMERIC/impression_num::NUMERIC end impression_cvr, 
            case when sale_ok_num=0 then 0 else pay_item_num::numeric/sale_ok_num::numeric end sale_rate,
            origin_amount,origin_qty,pay_item_num,sale_ok_num,sale_ok_in_num,new_item_num,mon_pay.acc_pay_item_num,
            order_all.place_order_num,order_all.place_product_qty,order_all.place_user_num,
            case when uv=0 then 0 else place_user_num::NUMERIC/uv::NUMERIC end  placeorderrate,
            return_all.return_quantity,return_all.refund_total
    from 

    (select  ua.create_date,
             case when front_cate_one is null then 'is_null' else front_cate_one end as old_cate_one,
             count(distinct case when event_type in ('product','impression') and mid='1.5.1.1' then cid else null end) as uv,
             count(case when  event_type='product' and mid like '%.9.1' then cid else null end) as impression_num,
             count(case when  event_type='click' and mid like '%.9.1' then cid else null end) as click_num,
             count(distinct ua.pid) as impression_item_num
    from 
            (select date(server_time+interval '8 hour') create_date,cid,pid,mid,event_type,country_code
             from spectrum_schema.user_trace 
             where event_type in ('product','click','impression')  
             and log_date >='{3}' and log_date <='{4}' 
             and server_time>='{1}' and server_time<'{2}'
            ) as ua 
    join jiayundw_dim.product_basic_info_df as pro on pro.pid=ua.pid
    group by 1,2
    ) as uv_all

    left join 
    (select case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            count(*) as sale_ok_num,count(case when a.product_no is null then pro.item_no else null end) sale_ok_in_num
    from jiayundw_dim.product_basic_info_df pro 
    left join 
    (select distinct product_no
     from  (select product_no,tag_fb_nations,update_time
            from   odoo.tag_product_record X
            where  update_time=(select max(update_time) from ods_odoo.tag_product_record Y where X.product_no = Y.product_no))
     where tag_fb_nations like '%in%') a on a.product_no=pro.item_no
    where pro.active=1 
    group by 1) as sale_item 
    on uv_all.old_cate_one=sale_item.old_cate_one

    left join 
    (select case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,count(*) as new_item_num
    from jiayundw_dim.product_basic_info_df  pro
    where active=1 and create_date='{0}' 
    group by 1) as new_item 
    on uv_all.old_cate_one=new_item.old_cate_one

    left join 
    (select date(so.create_at+interval '8 hour') as create_date,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            count(distinct so.order_name) as pay_order_num,
            sum(sol.origin_qty*sol.price_unit) as origin_amount,
            sum(sol.origin_qty) as origin_qty,
            count(distinct sol.item_no) as pay_item_num,
            count(distinct so.user_id) as pay_user_num,
            count(distinct case when so.gender='men' then so.user_id end) as m_user_num,
            count(distinct case when so.gender='women' then so.user_id end) as f_user_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join jiayundw_dim.product_basic_info_df as pro on sol.item_no=pro.item_no
    where sol.is_delivery=0 and date(so.create_at+interval '8 hour') = '{0}' 
    group by 1,2
   ) as sale_all
    on uv_all.create_date=sale_all.create_date and uv_all.old_cate_one=sale_all.old_cate_one

    left join 
    (select current_date-{6} as create_date,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            count(distinct sol.item_no) as acc_pay_item_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join jiayundw_dim.product_basic_info_df as pro on sol.item_no=pro.item_no 
    where sol.is_delivery=0 and date(so.create_at+interval '8 hour') between '{5}' and '{0}' 
    group by 1,2
    ) mon_pay
    on uv_all.create_date=mon_pay.create_date and uv_all.old_cate_one=mon_pay.old_cate_one

    left join
    (select  date(so.order_at+interval '8 hour') as create_date,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            count(distinct so.order_name) as place_order_num,
            sum(sol.product_qty*sol.price_unit) as place_product_amount,
            sum(sol.product_qty) as place_product_qty,
            count(distinct so.user_id) as place_user_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join jiayundw_dim.product_basic_info_df as pro on sol.item_no=pro.item_no 
    where sol.is_delivery=0 and date(so.order_at+interval '8 hour') = '{0}' 
    group by 1,2) order_all
    on uv_all.create_date=order_all.create_date and uv_all.old_cate_one=order_all.old_cate_one

    left join
    (select date(update_at+interval '8 hour') as create_date,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
    		sum(quantity) return_quantity,sum(refund_total) refund_total
    from order_center.return_request_line as rrl
    join jiayundw_dim.product_basic_info_df as pro on pro.item_no=rrl.product_no
    where state = 4 and flag = 0 and date(update_at+interval '8 hour')='{0}'
    group by 1,2 )  return_all
    on uv_all.create_date=return_all.create_date and uv_all.old_cate_one=return_all.old_cate_one

union 

select uv_all.seller_type,uv_all.create_date,uv_all.old_cate_one,impression_num,impression_item_num,click_num,uv,
            pay_order_num,pay_user_num,m_user_num,f_user_num,
            case when uv=0 then 0 else pay_user_num::NUMERIC/uv::NUMERIC end  cvr,
            case when impression_num=0 then 0 else origin_qty::NUMERIC/impression_num::NUMERIC end impression_cvr, 
            case when sale_ok_num=0 then 0 else pay_item_num::numeric/sale_ok_num::numeric end sale_rate,
            origin_amount,origin_qty,pay_item_num,sale_ok_num,sale_ok_in_num,new_item_num,mon_pay.acc_pay_item_num,
            order_all.place_order_num,order_all.place_product_qty,order_all.place_user_num,
            case when uv=0 then 0 else place_user_num::NUMERIC/uv::NUMERIC end  placeorderrate,
            return_all.return_quantity,return_all.refund_total
    from 

    (select  ua.create_date,pro.seller_type,
             case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
             count(distinct case when event_type in ('product','impression') and mid='1.5.1.1' then cid else null end) as uv,
             count(case when  event_type='product' and mid like '%.9.1' then cid else null end) as impression_num,
             count(case when  event_type='click' and mid like '%.9.1' then cid else null end) as click_num,
             count(distinct ua.pid) as impression_item_num
    from 
            (select date(server_time+interval '8 hour') create_date,cid,pid,mid,event_type,country_code
             from spectrum_schema.user_trace 
             where event_type in ('product','click','impression')  
             and log_date >='{3}' and log_date <='{4}' 
             and server_time>='{1}' and server_time<'{2}'
            ) as ua 
    join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.pid=ua.pid
    group by 1,2,3
    ) as uv_all

    left join 
    (select pro.seller_type,
            case when pro.front_cate_one is null then 'is_null' else pro.front_cate_one end old_cate_one,
            count(*) as sale_ok_num,count(case when a.product_no is null then pro.item_no else null end) sale_ok_in_num
    from 
    (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from jiayundw_dim.product_basic_info_df ) as pro
    left join 
    (select distinct product_no
     from  (select product_no,tag_fb_nations,update_time
            from   odoo.tag_product_record X
            where  update_time=(select max(update_time) from ods_odoo.tag_product_record Y where X.product_no = Y.product_no))
     where tag_fb_nations like '%in%') a on pro.item_no=a.product_no
    where pro.active=1 
    group by 1,2) as sale_item 
    on uv_all.old_cate_one=sale_item.old_cate_one and uv_all.seller_type=sale_item.seller_type

    left join 
    (select pro.seller_type,case when pro.front_cate_one is null then 'is_null' else pro.front_cate_one end old_cate_one,count(*) as new_item_num
    from (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df)  pro 
    where pro.active=1 and pro.create_date='{0}' 
    group by 1,2) as new_item 
    on uv_all.old_cate_one=new_item.old_cate_one and uv_all.seller_type=new_item.seller_type

    left join 
    (select date(so.create_at+interval '8 hour') as create_date,pro.seller_type,
            case when pro.front_cate_one is null then 'is_null' else pro.front_cate_one end old_cate_one,
            count(distinct so.order_name) as pay_order_num,
            sum(sol.origin_qty*sol.price_unit) as origin_amount,
            sum(sol.origin_qty) as origin_qty,
            count(distinct sol.item_no) as pay_item_num,
            count(distinct so.user_id) as pay_user_num,
            count(distinct case when so.gender='men' then so.user_id end) as m_user_num,
            count(distinct case when so.gender='women' then so.user_id end) as f_user_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.item_no=sol.item_no
    where sol.is_delivery=0 and date(so.create_at+interval '8 hour') = '{0}' 
    group by 1,2,3
   ) as sale_all
    on uv_all.create_date=sale_all.create_date and uv_all.old_cate_one=sale_all.old_cate_one and uv_all.seller_type=sale_all.seller_type

    left join 
    (select current_date-{6} as create_date,pro.seller_type,
            case when pro.front_cate_one is null then 'is_null' else pro.front_cate_one end old_cate_one,
            count(distinct sol.item_no) as acc_pay_item_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.item_no=sol.item_no
    where sol.is_delivery=0 and date(so.create_at+interval '8 hour') between '{5}' and '{0}' 
    group by 1,2,3
    ) mon_pay
    on uv_all.create_date=mon_pay.create_date and uv_all.old_cate_one=mon_pay.old_cate_one and uv_all.seller_type=mon_pay.seller_type

    left join
    (select  date(so.order_at+interval '8 hour') as create_date,pro.seller_type,
            case when pro.front_cate_one is null then 'is_null' else pro.front_cate_one end old_cate_one,
            count(distinct so.order_name) as place_order_num,
            sum(sol.product_qty*sol.price_unit) as place_product_amount,
            sum(sol.product_qty) as place_product_qty,
            count(distinct so.user_id) as place_user_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.item_no=sol.item_no
    where sol.is_delivery=0 and date(so.order_at+interval '8 hour') = '{0}' 
    group by 1,2,3) order_all
    on uv_all.create_date=order_all.create_date and uv_all.old_cate_one=order_all.old_cate_one and uv_all.seller_type=order_all.seller_type

    left join
    (select date(update_at+interval '8 hour') as create_date,pro.seller_type,
            case when pro.front_cate_one is null then 'is_null' else pro.front_cate_one end old_cate_one,
    		sum(quantity) return_quantity,sum(refund_total) refund_total
    from order_center.return_request_line as rrl
    join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.item_no=rrl.product_no
    where state = 4 and flag = 0 and date(update_at+interval '8 hour')='{0}'
    group by 1,2,3 )  return_all
    on uv_all.create_date=return_all.create_date and uv_all.old_cate_one=return_all.old_cate_one and uv_all.seller_type=return_all.seller_type
    order by seller_type,create_date,old_cate_one;
    """.format(start,uv_start,uv_end,log_start,log_end,month_start,interval)

    sql_cateTwo="""
    select 'total' seller_type,uv_all.create_date,uv_all.old_cate_one,uv_all.old_cate_two,impression_num,impression_item_num,click_num,uv,
            pay_order_num,pay_user_num,m_user_num,f_user_num,
            case when uv=0 then 0 else pay_user_num::NUMERIC/uv::NUMERIC end  cvr,
            case when impression_num=0 then 0 else origin_qty::NUMERIC/impression_num::NUMERIC end impression_cvr, 
            case when sale_ok_num=0 then 0 else pay_item_num::numeric/sale_ok_num::numeric end sale_rate,
            origin_amount,origin_qty,pay_item_num,sale_ok_num,sale_ok_in_num,new_item_num,mon_pay.acc_pay_item_num,
            order_all.place_order_num,order_all.place_product_qty,order_all.place_user_num,
            case when uv=0 then 0 else place_user_num::NUMERIC/uv::NUMERIC end  placeorderrate,
            return_all.return_quantity,return_all.refund_total
    from 

    (select  ua.create_date,
             case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
             case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
             count(distinct case when event_type in ('product','impression') and mid='1.5.1.1' then cid else null end) as uv,
             count(case when  event_type='product' and mid like '%.9.1' then cid else null end) as impression_num,
             count(case when  event_type='click' and mid like '%.9.1' then cid else null end) as click_num,
             count(distinct ua.pid) as impression_item_num
    from 
            (select date(server_time+interval '8 hour') create_date,cid,pid,mid,event_type,country_code
             from spectrum_schema.user_trace 
             where event_type in ('product','click','impression')  
             and log_date >='{3}' and log_date <='{4}' 
             and server_time>='{1}' and server_time<'{2}'
            ) as ua 
    join jiayundw_dim.product_basic_info_df as pro on pro.pid=ua.pid
    group by 1,2,3
    ) as uv_all

    left join 
    (select case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
             case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
             count(*) as sale_ok_num,
        count(case when a.product_no is null then pro.item_no else null end) sale_ok_in_num
    from jiayundw_dim.product_basic_info_df pro 
    left join 
    (select distinct product_no
     from  (select product_no,tag_fb_nations,update_time
            from   odoo.tag_product_record X
            where  update_time=(select max(update_time) from ods_odoo.tag_product_record Y where X.product_no = Y.product_no))
     where tag_fb_nations like '%in%') a on a.product_no=pro.item_no
    where pro.active=1 
    group by 1,2) as sale_item 
    on uv_all.old_cate_one=sale_item.old_cate_one and uv_all.old_cate_two=sale_item.old_cate_two

    left join 
    (select case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
             case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
             count(*) as new_item_num
    from jiayundw_dim.product_basic_info_df  pro
    where pro.active=1 and pro.create_date='{0}'
    group by 1,2 ) as new_item 
    on uv_all.old_cate_one=new_item.old_cate_one and uv_all.old_cate_two=new_item.old_cate_two

    left join 
    (select date(so.create_at+interval '8 hour') as create_date,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            count(distinct so.order_name) as pay_order_num,
            sum(sol.origin_qty*sol.price_unit) as origin_amount,
            sum(sol.origin_qty) as origin_qty,
            count(distinct sol.item_no) as pay_item_num,
            count(distinct so.user_id) as pay_user_num,
            count(distinct case when so.gender='men' then so.user_id end) as m_user_num,
            count(distinct case when so.gender='women' then so.user_id end) as f_user_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join jiayundw_dim.product_basic_info_df as ic on sol.item_no=ic.item_no
    where sol.is_delivery=0 and date(so.create_at+interval '8 hour') = '{0}' 
    group by 1,2,3
   ) as sale_all
    on uv_all.create_date=sale_all.create_date and uv_all.old_cate_one=sale_all.old_cate_one and uv_all.old_cate_two=sale_all.old_cate_two

    left join 
    (select current_date-{6} as create_date,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            count(distinct sol.item_no) as acc_pay_item_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join jiayundw_dim.product_basic_info_df as ic on sol.item_no=ic.item_no
    where sol.is_delivery=0 and date(so.create_at+interval '8 hour') between '{5}' and '{0}' 
    group by 1,2,3
    ) mon_pay
    on uv_all.create_date=mon_pay.create_date and uv_all.old_cate_one=mon_pay.old_cate_one and uv_all.old_cate_two=mon_pay.old_cate_two

    left join
    (select  date(so.order_at+interval '8 hour') as create_date,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            count(distinct so.order_name) as place_order_num,
            sum(sol.product_qty*sol.price_unit) as place_product_amount,
            sum(sol.product_qty) as place_product_qty,
            count(distinct so.user_id) as place_user_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join jiayundw_dim.product_basic_info_df as ic on sol.item_no=ic.item_no
    where sol.is_delivery=0 and date(so.order_at+interval '8 hour') = '{0}' 
    group by 1,2,3) order_all
    on uv_all.create_date=order_all.create_date and uv_all.old_cate_one=order_all.old_cate_one and uv_all.old_cate_two=order_all.old_cate_two

    left join
    (select date(update_at+interval '8 hour') as create_date,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            sum(quantity) return_quantity,sum(refund_total) refund_total
    from order_center.return_request_line as rrl
    join jiayundw_dim.product_basic_info_df as ic on rrl.product_no=ic.item_no
    where state = 4 and flag = 0 and date(update_at+interval '8 hour')='{0}'
    group by 1,2,3 )  return_all
    on uv_all.create_date=return_all.create_date and uv_all.old_cate_one=return_all.old_cate_one and uv_all.old_cate_two=return_all.old_cate_two

union 

select uv_all.seller_type,uv_all.create_date,uv_all.old_cate_one,uv_all.old_cate_two,impression_num,impression_item_num,click_num,uv,
            pay_order_num,pay_user_num,m_user_num,f_user_num,
            case when uv=0 then 0 else pay_user_num::NUMERIC/uv::NUMERIC end  cvr,
            case when impression_num=0 then 0 else origin_qty::NUMERIC/impression_num::NUMERIC end impression_cvr, 
            case when sale_ok_num=0 then 0 else pay_item_num::numeric/sale_ok_num::numeric end sale_rate,
            origin_amount,origin_qty,pay_item_num,sale_ok_num,sale_ok_in_num,new_item_num,mon_pay.acc_pay_item_num,
            order_all.place_order_num,order_all.place_product_qty,order_all.place_user_num,
            case when uv=0 then 0 else place_user_num::NUMERIC/uv::NUMERIC end  placeorderrate,
            return_all.return_quantity,return_all.refund_total
    from 

    (select  ua.create_date,pro.seller_type,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            count(distinct case when event_type in ('product','impression') and mid='1.5.1.1' then cid else null end) as uv,
             count(case when  event_type='product' and mid like '%.9.1' then cid else null end) as impression_num,
             count(case when  event_type='click' and mid like '%.9.1' then cid else null end) as click_num,
             count(distinct ua.pid) as impression_item_num
    from 
            (select date(server_time+interval '8 hour') create_date,cid,pid,mid,event_type,country_code
             from spectrum_schema.user_trace 
             where event_type in ('product','click','impression')  
             and log_date >='{3}' and log_date <='{4}' 
             and server_time>='{1}' and server_time<'{2}'
            ) as ua 
    join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.pid=ua.pid
    group by 1,2,3,4
    ) as uv_all

    left join 
    (select pro.seller_type,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            count(*) as sale_ok_num,count(case when a.product_no is null then pro.item_no else null end) sale_ok_in_num
    from (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from jiayundw_dim.product_basic_info_df) as pro
    left join 
    (select distinct product_no
     from  (select product_no,tag_fb_nations,update_time
            from   odoo.tag_product_record X
            where  update_time=(select max(update_time) from ods_odoo.tag_product_record Y where X.product_no = Y.product_no))
     where tag_fb_nations like '%in%') a on a.product_no=pro.item_no
    where pro.active=1 
    group by 1,2,3) as sale_item 
    on uv_all.old_cate_one=sale_item.old_cate_one and uv_all.seller_type=sale_item.seller_type and uv_all.old_cate_two=sale_item.old_cate_two

    left join 
    (select pro.seller_type,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            count(*) as new_item_num
    from (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) pro
    where pro.active=1 and pro.create_date='{0}' 
    group by 1,2,3) as new_item 
    on uv_all.old_cate_one=new_item.old_cate_one and uv_all.seller_type=new_item.seller_type and uv_all.old_cate_two=new_item.old_cate_two

    left join 
    (select date(so.create_at+interval '8 hour') as create_date,pro.seller_type,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            count(distinct so.order_name) as pay_order_num,
            sum(sol.origin_qty*sol.price_unit) as origin_amount,
            sum(sol.origin_qty) as origin_qty,
            count(distinct sol.item_no) as pay_item_num,
            count(distinct so.user_id) as pay_user_num,
            count(distinct case when so.gender='men' then so.user_id end) as m_user_num,
            count(distinct case when so.gender='women' then so.user_id end) as f_user_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.item_no=sol.item_no
    where sol.is_delivery=0 and date(so.create_at+interval '8 hour') = '{0}' 
    group by 1,2,3,4
   ) as sale_all
    on uv_all.create_date=sale_all.create_date and uv_all.old_cate_one=sale_all.old_cate_one and uv_all.seller_type=sale_all.seller_type and uv_all.old_cate_two=sale_all.old_cate_two

    left join 
    (select current_date-{6} as create_date,pro.seller_type,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            count(distinct sol.item_no) as acc_pay_item_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.item_no=sol.item_no
    where sol.is_delivery=0 and date(so.create_at+interval '8 hour') between '{5}' and '{0}' 
    group by 1,2,3,4
    ) mon_pay
    on uv_all.create_date=mon_pay.create_date and uv_all.old_cate_one=mon_pay.old_cate_one and uv_all.seller_type=mon_pay.seller_type and uv_all.old_cate_two=mon_pay.old_cate_two

    left join
    (select  date(so.order_at+interval '8 hour') as create_date,pro.seller_type,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            count(distinct so.order_name) as place_order_num,
            sum(sol.product_qty*sol.price_unit) as place_product_amount,
            sum(sol.product_qty) as place_product_qty,
            count(distinct so.user_id) as place_user_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.item_no=sol.item_no
    where sol.is_delivery=0 and date(so.order_at+interval '8 hour') = '{0}' 
    group by 1,2,3,4) order_all
    on uv_all.create_date=order_all.create_date and uv_all.old_cate_one=order_all.old_cate_one and uv_all.seller_type=order_all.seller_type and uv_all.old_cate_two=order_all.old_cate_two

    left join
    (select date(update_at+interval '8 hour') as create_date,pro.seller_type,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            sum(quantity) return_quantity,sum(refund_total) refund_total
    from order_center.return_request_line as rrl
    join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.item_no=rrl.product_no
    where state = 4 and flag = 0 and date(update_at+interval '8 hour')='{0}'
    group by 1,2,3,4 )  return_all
    on uv_all.create_date=return_all.create_date and uv_all.old_cate_one=return_all.old_cate_one and uv_all.seller_type=return_all.seller_type and uv_all.old_cate_two=return_all.old_cate_two
    order by seller_type,create_date,old_cate_one,old_cate_two;
    """.format(start,uv_start,uv_end,log_start,log_end,month_start, interval)

    sql_all="""
    select 'total' seller_type,uv_all.create_date,impression_num,impression_item_num,click_num,uv,
            pay_order_num,pay_user_num,m_user_num,f_user_num,
            case when uv=0 then 0 else pay_user_num::NUMERIC/uv::NUMERIC end  cvr,
            case when impression_num=0 then 0 else origin_qty::NUMERIC/impression_num::NUMERIC end impression_cvr, 
            case when sale_ok_num=0 then 0 else pay_item_num::numeric/sale_ok_num::numeric end sale_rate,
            origin_amount,origin_qty,pay_item_num,sale_ok_num,sale_ok_in_num,new_item_num,mon_pay.acc_pay_item_num,
            order_all.place_order_num,order_all.place_product_qty,order_all.place_user_num,
            case when uv=0 then 0 else place_user_num::NUMERIC/uv::NUMERIC end  placeorderrate,
            return_all.return_quantity,return_all.refund_total,fp_user.fp_user_num
    from 

    (select  ua.create_date,
             count(distinct case when event_type in ('product','impression') and mid='1.5.1.1' then cid else null end) as uv,
             count(case when  event_type='product' and mid like '%.9.1' then cid else null end) as impression_num,
             count(case when  event_type='click' and mid like '%.9.1' then cid else null end) as click_num,
             count(distinct ua.pid) as impression_item_num
    from 
            (select date(server_time+interval '8 hour') create_date,cid,pid,mid,event_type,country_code
             from spectrum_schema.user_trace 
             where event_type in ('product','click','impression')  
             and log_date >='{4}' and log_date <='{5}' 
             and server_time>='{2}' and server_time<'{3}'
            ) as ua 
    join jiayundw_dim.product_basic_info_df as pro on pro.pid=ua.pid
    group by 1
    ) as uv_all

    left join
    (select CURRENT_DATE-{1} as create_date,count(*) as sale_ok_num,count(case when a.product_no is null then pro.item_no else null end) sale_ok_in_num
    from jiayundw_dim.product_basic_info_df pro 
    left join 
    (select distinct product_no
     from  (select product_no,tag_fb_nations,update_time
            from   odoo.tag_product_record X
            where  update_time=(select max(update_time) from ods_odoo.tag_product_record Y where X.product_no = Y.product_no))
     where tag_fb_nations like '%in%' ) a on a.product_no=pro.item_no
    where pro.active=1 
    ) as sale_item on uv_all.create_date=sale_item.create_date

    left join 
    (select CURRENT_DATE-{1} as create_date,count(*) as new_item_num
    from jiayundw_dim.product_basic_info_df  pro 
    where pro.active=1 and pro.create_date='{0}' 
    ) as new_item on uv_all.create_date=new_item.create_date

    left join
    (select date(so.create_at+interval '8 hour') as create_date,
            count(distinct so.order_name) as pay_order_num,
            sum(sol.origin_qty*sol.price_unit) as origin_amount,
            sum(sol.origin_qty) as origin_qty,
            count(distinct sol.item_no) as pay_item_num,
            count(distinct so.user_id) as pay_user_num,
            count(distinct case when so.gender='men' then so.user_id end) as m_user_num,
            count(distinct case when so.gender='women' then so.user_id end) as f_user_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join jiayundw_dim.product_basic_info_df pro on pro.item_no=sol.item_no
    where sol.is_delivery=0 and date(so.create_at+interval '8 hour') = '{0}'  
    group by 1
    ) as sale_all
    on uv_all.create_date=sale_all.create_date

    left join 
    (select current_date-{1} as create_date,
            count(distinct sol.item_no) as acc_pay_item_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join jiayundw_dim.product_basic_info_df pro on pro.item_no=sol.item_no
    where sol.is_delivery=0 and date(so.create_at+interval '8 hour') between '{6}' and '{0}' 
    group by 1
    ) mon_pay
    on uv_all.create_date=mon_pay.create_date 

    left join
    (select  date(so.order_at+interval '8 hour') as create_date,
            count(distinct so.order_name) as place_order_num,
            sum(sol.product_qty*sol.price_unit) as place_product_amount,
            sum(sol.product_qty) as place_product_qty,
            count(distinct so.user_id) as place_user_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join jiayundw_dim.product_basic_info_df pro on pro.item_no=sol.item_no
    where sol.is_delivery=0 and date(so.order_at+interval '8 hour') = '{0}' 
    group by 1) order_all
    on uv_all.create_date=order_all.create_date 

    left join
    (select date(update_at+interval '8 hour') as create_date,
    		sum(quantity) return_quantity,sum(refund_total) refund_total
    from order_center.return_request_line as rrl
    where state = 4 and flag = 0 and  date(update_at+interval '8 hour')='{0}'
    group by 1 )  return_all
    on uv_all.create_date=return_all.create_date 

    left join
    (select date(b.create_at + interval '8 hour' )as create_date,
           count(distinct case when date(b.create_at + interval'8 hour')>c.min_date then null else b.user_id end) fp_user_num
     from  jiayundw_dm.sale_order_info_df b 
     join  jiayundw_dm.sale_order_line_df sol on sol.order_name = b.order_name
     left join  (select m.user_id, min(m.min_date) as min_date
                  from analysts.user_order_min m
                 GROUP BY 1) c on b.user_id = c.user_id
     where date(b.create_at+interval '8 hour') = '{0}'
     group by 1) fp_user on uv_all.create_date=return_all.create_date

union 

select uv_all.seller_type,uv_all.create_date,impression_num,impression_item_num,click_num,uv,
            pay_order_num,pay_user_num,m_user_num,f_user_num,
            case when uv=0 then 0 else pay_user_num::NUMERIC/uv::NUMERIC end  cvr,
            case when impression_num=0 then 0 else origin_qty::NUMERIC/impression_num::NUMERIC end impression_cvr, 
            case when sale_ok_num=0 then 0 else pay_item_num::numeric/sale_ok_num::numeric end sale_rate,
            origin_amount,origin_qty,pay_item_num,sale_ok_num,sale_ok_in_num,new_item_num,mon_pay.acc_pay_item_num,
            order_all.place_order_num,order_all.place_product_qty,order_all.place_user_num,
            case when uv=0 then 0 else place_user_num::NUMERIC/uv::NUMERIC end  placeorderrate,
            return_all.return_quantity,return_all.refund_total,fp_user.fp_user_num
    from 

    (select  ua.create_date,pro.seller_type,
             count(distinct case when event_type in ('product','impression') and mid='1.5.1.1' then cid else null end) as uv,
             count(case when  event_type='product' and mid like '%.9.1' then cid else null end) as impression_num,
             count(case when  event_type='click' and mid like '%.9.1' then cid else null end) as click_num,
             count(distinct ua.pid) as impression_item_num
    from 
            (select date(server_time+interval '8 hour') create_date,cid,pid,mid,event_type,country_code
             from spectrum_schema.user_trace 
             where event_type in ('product','click','impression')  
             and log_date >='{4}' and log_date <='{5}' 
             and server_time>='{2}' and server_time<'{3}'
            ) as ua 
    join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.pid=ua.pid
    group by 1,2
    ) as uv_all

    left join 
    (select pro.seller_type,count(*) as sale_ok_num,count(case when a.product_no is null then pro.item_no else null end) sale_ok_in_num
    from (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from jiayundw_dim.product_basic_info_df) as pro 
    left join  
    (select distinct product_no
     from  (select product_no,tag_fb_nations,update_time
            from   odoo.tag_product_record X
            where  update_time=(select max(update_time) from ods_odoo.tag_product_record Y where X.product_no = Y.product_no))
     where tag_fb_nations like '%in%' ) a on a.product_no=pro.item_no
    where pro.active=1  
    group by 1) as sale_item on  uv_all.seller_type=sale_item.seller_type

    left join 
    (select pro.seller_type,count(*) as new_item_num
    from (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro 
    where pro.active=1 and pro.create_date='{0}' 
    group by 1) as new_item on uv_all.seller_type=new_item.seller_type

    left join 
    (select date(so.create_at+interval '8 hour') as create_date,pro.seller_type,
            count(distinct so.order_name) as pay_order_num,
            sum(sol.origin_qty*sol.price_unit) as origin_amount,
            sum(sol.origin_qty) as origin_qty,
            count(distinct sol.item_no) as pay_item_num,
            count(distinct so.user_id) as pay_user_num,
            count(distinct case when so.gender='men' then so.user_id end) as m_user_num,
            count(distinct case when so.gender='women' then so.user_id end) as f_user_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    left join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.item_no=sol.item_no
    where sol.is_delivery=0 and date(so.create_at+interval '8 hour') = '{0}' 
    group by 1,2
   ) as sale_all
    on uv_all.create_date=sale_all.create_date and uv_all.seller_type=sale_all.seller_type

    left join 
    (select current_date-{1} as create_date,pro.seller_type,
            count(distinct sol.item_no) as acc_pay_item_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    left join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.item_no=sol.item_no
    where sol.is_delivery=0 and date(so.create_at+interval '8 hour') between '{6}' and '{0}' 
    group by 1,2
    ) mon_pay
    on uv_all.create_date=mon_pay.create_date and uv_all.seller_type=mon_pay.seller_type

    left join
    (select  date(so.order_at+interval '8 hour') as create_date,pro.seller_type,
            count(distinct so.order_name) as place_order_num,
            sum(sol.product_qty*sol.price_unit) as place_product_amount,
            sum(sol.product_qty) as place_product_qty,
            count(distinct so.user_id) as place_user_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    left join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.item_no=sol.item_no
    where sol.is_delivery=0 and date(so.order_at+interval '8 hour') = '{0}' 
    group by 1,2) order_all
    on uv_all.create_date=order_all.create_date and uv_all.seller_type=order_all.seller_type

    left join
    (select date(update_at+interval '8 hour') as create_date,pro.seller_type,
    		sum(quantity) return_quantity,sum(refund_total) refund_total
    from order_center.return_request_line as rrl
    join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.item_no=rrl.product_no
    where state = 4 and flag = 0 and date(update_at+interval '8 hour')='{0}'
    group by 1,2 )  return_all
    on uv_all.create_date=return_all.create_date and uv_all.seller_type=return_all.seller_type

    left join
    (select date(b.create_at + interval '8 hour' )as create_date,pro.seller_type,
           count(distinct case when date(b.create_at + interval'8 hour')>c.min_date then null else b.user_id end) fp_user_num
     from  jiayundw_dm.sale_order_info_df b 
     join  jiayundw_dm.sale_order_line_df sol on sol.order_name = b.order_name
     left join (select m.user_id, min(m.min_date) as min_date
                  from analysts.user_order_min m
                 GROUP BY 1) c on b.user_id = c.user_id
     left join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.item_no=sol.item_no
     where date(b.create_at+interval '8 hour') = '{0}'
     group by 1,2) fp_user on uv_all.create_date=fp_user.create_date and uv_all.seller_type=fp_user.seller_type;
    """.format(start,interval,uv_start,uv_end,log_start,log_end,month_start)
    
    sql_cateThree="""
    select 'total' seller_type,uv_all.create_date,uv_all.old_cate_one,uv_all.old_cate_two,uv_all.old_cate_three,impression_num,
						impression_item_num,click_num,uv,pay_order_num,pay_user_num,m_user_num,f_user_num,
            case when uv=0 then 0 else pay_user_num::NUMERIC/uv::NUMERIC end  cvr,
            case when impression_num=0 then 0 else origin_qty::NUMERIC/impression_num::NUMERIC end impression_cvr, 
            case when sale_ok_num=0 then 0 else pay_item_num::numeric/sale_ok_num::numeric end sale_rate,
            origin_amount,origin_qty,pay_item_num,sale_ok_num,sale_ok_in_num,new_item_num,mon_pay.acc_pay_item_num,
            order_all.place_order_num,order_all.place_product_qty,order_all.place_user_num,
            case when uv=0 then 0 else place_user_num::NUMERIC/uv::NUMERIC end  placeorderrate,
            return_all.return_quantity,return_all.refund_total
    from 

    (select  ua.create_date,
             case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
             case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end old_cate_two,
             case when front_cate_one is null and front_cate_three is null then 'is_null' else front_cate_three end old_cate_three,
             count(distinct case when event_type in ('product','impression') and mid='1.5.1.1' then cid else null end) as uv,
             count(case when  event_type='product' and mid like '%.9.1' then cid else null end) as impression_num,
             count(case when  event_type='click' and mid like '%.9.1' then cid else null end) as click_num,
             count(distinct ua.pid) as impression_item_num
    from 
            (select date(server_time+interval '8 hour') create_date,cid,pid,mid,event_type,country_code
             from spectrum_schema.user_trace 
             where event_type in ('product','click','impression')  
             and log_date >='{3}' and log_date <='{4}' 
             and server_time>='{1}' and server_time<'{2}'
            ) as ua 
    join jiayundw_dim.product_basic_info_df as pro on pro.pid=ua.pid
    group by 1,2,3,4
    ) as uv_all

    left join 
    (select case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            case when front_cate_one is null and front_cate_three is null then 'is_null' else front_cate_three end  old_cate_three,
            count(*) as sale_ok_num,
    		count(case when a.product_no is null then pro.item_no else null end) sale_ok_in_num
    from jiayundw_dim.product_basic_info_df pro 
    left join 
    (select distinct product_no
     from  (select product_no,tag_fb_nations,update_time
            from   odoo.tag_product_record X
            where  update_time=(select max(update_time) from ods_odoo.tag_product_record Y where X.product_no = Y.product_no))
     where tag_fb_nations like '%in%') a on a.product_no=pro.item_no
    where pro.active=1  
    group by 1,2,3) as sale_item 
    on uv_all.old_cate_one=sale_item.old_cate_one and uv_all.old_cate_two=sale_item.old_cate_two and uv_all.old_cate_three=sale_item.old_cate_three

    left join 
    (select case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            case when front_cate_one is null and front_cate_three is null then 'is_null' else front_cate_three end  old_cate_three,count(*) as new_item_num
    from jiayundw_dim.product_basic_info_df pro
    where pro.active=1 and pro.create_date='{0}' 
    group by 1,2,3) as new_item 
    on uv_all.old_cate_one=new_item.old_cate_one and uv_all.old_cate_two=new_item.old_cate_two and uv_all.old_cate_three=new_item.old_cate_three

    left join 
    (select date(so.create_at+interval '8 hour') as create_date,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            case when front_cate_one is null and front_cate_three is null then 'is_null' else front_cate_three end  old_cate_three,
            count(distinct so.order_name) as pay_order_num,
            sum(sol.origin_qty*sol.price_unit) as origin_amount,
            sum(sol.origin_qty) as origin_qty,
            count(distinct sol.item_no) as pay_item_num,
            count(distinct so.user_id) as pay_user_num,
            count(distinct case when so.gender='men' then so.user_id end) as m_user_num,
            count(distinct case when so.gender='women' then so.user_id end) as f_user_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join jiayundw_dim.product_basic_info_df  as ic on sol.item_no=ic.item_no
    where sol.is_delivery=0 and date(so.create_at+interval '8 hour') = '{0}' 
    group by 1,2,3,4
   ) as sale_all
    on uv_all.create_date=sale_all.create_date and uv_all.old_cate_one=sale_all.old_cate_one and uv_all.old_cate_two=sale_all.old_cate_two and uv_all.old_cate_three=sale_all.old_cate_three

    left join 
    (select current_date-{6} as create_date,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
           case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
           case when front_cate_one is null and front_cate_three is null then 'is_null' else front_cate_three end  old_cate_three,
            count(distinct sol.item_no) as acc_pay_item_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join jiayundw_dim.product_basic_info_df as ic on sol.item_no=ic.item_no
    where sol.is_delivery=0 and date(so.create_at+interval '8 hour') between '{5}' and '{0}' 
    group by 1,2,3,4
    ) mon_pay
    on uv_all.create_date=mon_pay.create_date and uv_all.old_cate_one=mon_pay.old_cate_one and uv_all.old_cate_two=mon_pay.old_cate_two and uv_all.old_cate_three=mon_pay.old_cate_three

    left join
    (select  date(so.order_at+interval '8 hour') as create_date,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            case when front_cate_one is null and front_cate_three is null then 'is_null' else front_cate_three end  old_cate_three,
            count(distinct so.order_name) as place_order_num,
            sum(sol.product_qty*sol.price_unit) as place_product_amount,
            sum(sol.product_qty) as place_product_qty,
            count(distinct so.user_id) as place_user_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join jiayundw_dim.product_basic_info_df as ic on sol.item_no=ic.item_no
    where sol.is_delivery=0 and date(so.order_at+interval '8 hour') = '{0}' 
    group by 1,2,3,4) order_all
    on uv_all.create_date=order_all.create_date and uv_all.old_cate_one=order_all.old_cate_one and uv_all.old_cate_two=order_all.old_cate_two and uv_all.old_cate_three=order_all.old_cate_three

    left join
    (select date(update_at+interval '8 hour') as create_date,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            case when front_cate_one is null and front_cate_three is null then 'is_null' else front_cate_three end  old_cate_three,
    		sum(quantity) return_quantity,sum(refund_total) refund_total
    from order_center.return_request_line as rrl
    join jiayundw_dim.product_basic_info_df as ic on rrl.product_no=ic.item_no
    where state = 4 and flag = 0 and date(update_at+interval '8 hour')='{0}'
    group by 1,2,3,4 )  return_all
    on uv_all.create_date=return_all.create_date and uv_all.old_cate_one=return_all.old_cate_one and uv_all.old_cate_two=return_all.old_cate_two and uv_all.old_cate_three=return_all.old_cate_three

union 

select uv_all.seller_type,uv_all.create_date,uv_all.old_cate_one,uv_all.old_cate_two,uv_all.old_cate_three,impression_num,impression_item_num,click_num,uv,
            pay_order_num,pay_user_num,m_user_num,f_user_num,
            case when uv=0 then 0 else pay_user_num::NUMERIC/uv::NUMERIC end  cvr,
            case when impression_num=0 then 0 else origin_qty::NUMERIC/impression_num::NUMERIC end impression_cvr, 
            case when sale_ok_num=0 then 0 else pay_item_num::numeric/sale_ok_num::numeric end sale_rate,
            origin_amount,origin_qty,pay_item_num,sale_ok_num,sale_ok_in_num,new_item_num,mon_pay.acc_pay_item_num,
            order_all.place_order_num,order_all.place_product_qty,order_all.place_user_num,
            case when uv=0 then 0 else place_user_num::NUMERIC/uv::NUMERIC end  placeorderrate,
            return_all.return_quantity,return_all.refund_total
    from 

    (select  ua.create_date,pro.seller_type,
             case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
             case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end old_cate_two,
             case when front_cate_one is null and front_cate_three is null then 'is_null' else front_cate_three end old_cate_three,
             count(distinct case when event_type in ('product','impression') and mid='1.5.1.1' then cid else null end) as uv,
             count(case when  event_type='product' and mid like '%.9.1' then cid else null end) as impression_num,
             count(case when  event_type='click' and mid like '%.9.1' then cid else null end) as click_num,
             count(distinct ua.pid) as impression_item_num
    from 
            (select date(server_time+interval '8 hour') create_date,cid,pid,mid,event_type,country_code
             from spectrum_schema.user_trace 
             where event_type in ('product','click','impression')  
             and log_date >='{3}' and log_date <='{4}' 
             and server_time>='{1}' and server_time<'{2}'
            ) as ua 
    join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.pid=ua.pid
    group by 1,2,3,4,5
    ) as uv_all

    left join 
    (select pro.seller_type,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            case when front_cate_one is null and front_cate_three is null then 'is_null' else front_cate_three end  old_cate_three,count(*) as sale_ok_num,
    		count(case when a.product_no is null then pro.item_no else null end) sale_ok_in_num
    from (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from jiayundw_dim.product_basic_info_df) as pro 
    left join 
    (select distinct product_no
     from  (select product_no,tag_fb_nations,update_time
            from   odoo.tag_product_record X
            where  update_time=(select max(update_time) from ods_odoo.tag_product_record Y where X.product_no = Y.product_no))
     where tag_fb_nations like '%in%') a on a.product_no=pro.item_no
    where pro.active=1 
    group by 1,2,3,4) as sale_item 
    on uv_all.old_cate_one=sale_item.old_cate_one and uv_all.seller_type=sale_item.seller_type and uv_all.old_cate_two=sale_item.old_cate_two and uv_all.old_cate_three=sale_item.old_cate_three

    left join 
    (select pro.seller_type,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            case when front_cate_one is null and front_cate_three is null then 'is_null' else front_cate_three end  old_cate_three,count(*) as new_item_num
    from (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro 
    where pro.active=1 and pro.create_date='{0}' 
    group by 1,2,3,4) as new_item 
    on uv_all.old_cate_one=new_item.old_cate_one and uv_all.seller_type=new_item.seller_type and uv_all.old_cate_two=new_item.old_cate_two and uv_all.old_cate_three=new_item.old_cate_three

    left join 
    (select date(so.create_at+interval '8 hour') as create_date,pro.seller_type,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            case when front_cate_one is null and front_cate_three is null then 'is_null' else front_cate_three end  old_cate_three,
            count(distinct so.order_name) as pay_order_num,
            sum(sol.origin_qty*sol.price_unit) as origin_amount,
            sum(sol.origin_qty) as origin_qty,
            count(distinct sol.item_no) as pay_item_num,
            count(distinct so.user_id) as pay_user_num,
            count(distinct case when so.gender='men' then so.user_id end) as m_user_num,
            count(distinct case when so.gender='women' then so.user_id end) as f_user_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.item_no=sol.item_no
    where sol.is_delivery=0 and date(so.create_at+interval '8 hour') = '{0}' 
    group by 1,2,3,4,5
   ) as sale_all
    on uv_all.create_date=sale_all.create_date and uv_all.old_cate_one=sale_all.old_cate_one and uv_all.seller_type=sale_all.seller_type and uv_all.old_cate_two=sale_all.old_cate_two and uv_all.old_cate_three=sale_all.old_cate_three

    left join 
    (select current_date-{6} as create_date,pro.seller_type,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            case when front_cate_one is null and front_cate_three is null then 'is_null' else front_cate_three end  old_cate_three,
            count(distinct sol.item_no) as acc_pay_item_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.item_no=sol.item_no
    where sol.is_delivery=0 and date(so.create_at+interval '8 hour') between '{5}' and '{0}' 
    group by 1,2,3,4,5
    ) mon_pay
    on uv_all.create_date=mon_pay.create_date and uv_all.old_cate_one=mon_pay.old_cate_one and uv_all.seller_type=mon_pay.seller_type and uv_all.old_cate_two=mon_pay.old_cate_two and uv_all.old_cate_three=mon_pay.old_cate_three

    left join
    (select  date(so.order_at+interval '8 hour') as create_date,pro.seller_type,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            case when front_cate_one is null and front_cate_three is null then 'is_null' else front_cate_three end  old_cate_three,
            count(distinct so.order_name) as place_order_num,
            sum(sol.product_qty*sol.price_unit) as place_product_amount,
            sum(sol.product_qty) as place_product_qty,
            count(distinct so.user_id) as place_user_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.item_no=sol.item_no
    where sol.is_delivery=0 and date(so.order_at+interval '8 hour') = '{0}' 
    group by 1,2,3,4,5) order_all
    on uv_all.create_date=order_all.create_date and uv_all.old_cate_one=order_all.old_cate_one and uv_all.seller_type=order_all.seller_type and uv_all.old_cate_two=order_all.old_cate_two and uv_all.old_cate_three=order_all.old_cate_three

    left join
    (select date(update_at+interval '8 hour') as create_date,pro.seller_type,
            case when front_cate_one is null then 'is_null' else front_cate_one end old_cate_one,
            case when front_cate_one is null and front_cate_two is null then 'is_null' else front_cate_two end  old_cate_two,
            case when front_cate_one is null and front_cate_three is null then 'is_null' else front_cate_three end  old_cate_three,
    		sum(quantity) return_quantity,sum(refund_total) refund_total
    from order_center.return_request_line as rrl
    join (select *,case write_uid when 5 then 'seller' else 'cf' end seller_type from  jiayundw_dim.product_basic_info_df) as pro on pro.item_no=rrl.product_no
    where state = 4 and flag = 0 and date(update_at+interval '8 hour')='{0}'
    group by 1,2,3,4,5 )  return_all
    on uv_all.create_date=return_all.create_date and uv_all.old_cate_one=return_all.old_cate_one and uv_all.seller_type=return_all.seller_type and uv_all.old_cate_two=return_all.old_cate_two and uv_all.old_cate_three=return_all.old_cate_three
    order by seller_type,create_date,old_cate_one,old_cate_two,old_cate_three;
    """.format(start,uv_start,uv_end,log_start,log_end,month_start,interval)
    
    df_cateOne=pd.read_sql(sql_cateOne,redshift_conn)
    df_cateTwo=pd.read_sql(sql_cateTwo,redshift_conn)
    df_all=pd.read_sql(sql_all,redshift_conn)
    df_cateThree=pd.read_sql(sql_cateThree,redshift_conn)



    df_all=df_all[['seller_type','create_date','impression_num','impression_item_num','click_num','uv','pay_order_num','pay_user_num','m_user_num','f_user_num','cvr','impression_cvr','sale_rate','origin_amount','origin_qty','pay_item_num','sale_ok_num','sale_ok_in_num','new_item_num','fp_user_num','acc_pay_item_num','place_order_num','place_product_qty','place_user_num','return_quantity','refund_total','placeorderrate']]
    df_cateOne= df_cateOne[['seller_type','create_date','old_cate_one','impression_num','impression_item_num','click_num','uv','pay_order_num','pay_user_num','m_user_num','f_user_num','cvr','impression_cvr','sale_rate','origin_amount','origin_qty','pay_item_num','sale_ok_num','sale_ok_in_num','new_item_num','acc_pay_item_num','place_order_num','place_product_qty','place_user_num','return_quantity','refund_total','placeorderrate']]
    df_cateTwo=df_cateTwo[['seller_type','create_date','old_cate_one','old_cate_two','impression_num','impression_item_num','click_num','uv','pay_order_num','pay_user_num','m_user_num','f_user_num','cvr','impression_cvr','sale_rate','origin_amount','origin_qty','pay_item_num','sale_ok_num','sale_ok_in_num','new_item_num','acc_pay_item_num','place_order_num','place_product_qty','place_user_num','return_quantity','refund_total','placeorderrate']]
    df_cateThree=df_cateThree[['seller_type','create_date','old_cate_one','old_cate_two','old_cate_three','impression_num','impression_item_num','click_num','uv','pay_order_num','pay_user_num','m_user_num','f_user_num','cvr','impression_cvr','sale_rate','origin_amount','origin_qty','pay_item_num','sale_ok_num','sale_ok_in_num','new_item_num','acc_pay_item_num','place_order_num','place_product_qty','place_user_num','return_quantity','refund_total','placeorderrate']]
    
    
    df_all.rename(columns={'seller_type':u'业务类型','create_date':u'日期','impression_num':u'曝光数','impression_item_num':u'曝光商品数','click_num':u'点击数','uv':u'独立访客','place_user_num':u'下单买家数','place_order_num':u'下单量','place_product_qty':u'下单件数','placeorderrate':u'下单转化率','pay_user_num':u'支付买家数','m_user_num':u'男买家数','f_user_num':u'女买家数','pay_order_num':u'支付订单量','fp_user_num':u'新买家数','origin_qty': u'支付件数','origin_amount':u'支付金额','cvr':u'支付转化率','impression_cvr':u'曝光转化率','pay_item_num':u'支付商品数','acc_pay_item_num':u'月累计支付商品数','sale_ok_num':u'在售商品数','sale_ok_in_num':'印度在售商品数','new_item_num':u'新增商品数','sale_rate':u'动销率','return_quantity':u'退货件数','refund_total':u'退货金额'},inplace=True)
    df_cateOne.rename(columns={'seller_type':u'业务类型','create_date':u'日期','old_cate_one':u'一级类目','impression_num':u'曝光数','impression_item_num':u'曝光商品数','click_num':u'点击数','uv':u'独立访客','place_user_num':u'下单买家数','place_order_num':u'下单量','place_product_qty':u'下单件数','placeorderrate':u'下单转化率','pay_user_num':u'支付买家数','m_user_num':u'男买家数','f_user_num':u'女买家数','pay_order_num':u'支付订单量','origin_qty': u'支付件数','origin_amount':u'支付金额','cvr':u'支付转化率','impression_cvr':u'曝光转化率','pay_item_num':u'支付商品数','acc_pay_item_num':u'月累计支付商品数','sale_ok_num':u'在售商品数','sale_ok_in_num':'印度在售商品数','new_item_num':u'新增商品数','sale_rate':u'动销率','return_quantity':u'退货件数','refund_total':u'退货金额'},inplace=True)
    df_cateTwo.rename(columns={'seller_type':u'业务类型','create_date':u'日期','old_cate_one':u'一级类目','old_cate_two':u'二级类目','impression_num':u'曝光数','impression_item_num':u'曝光商品数','click_num':u'点击数','uv':u'独立访客','place_user_num':u'下单买家数','place_order_num':u'下单量','place_product_qty':u'下单件数','placeorderrate':u'下单转化率','pay_user_num':u'支付买家数','m_user_num':u'男买家数','f_user_num':u'女买家数','pay_order_num':u'支付订单量','origin_qty': u'支付件数','origin_amount':u'支付金额','cvr':u'支付转化率','impression_cvr':u'曝光转化率','pay_item_num':u'支付商品数','acc_pay_item_num':u'月累计支付商品数','sale_ok_num':u'在售商品数','sale_ok_in_num':'印度在售商品数','new_item_num':u'新增商品数','sale_rate':u'动销率','return_quantity':u'退货数量','refund_total':u'退货金额'},inplace=True)
    df_cateThree.rename(columns={'seller_type':u'业务类型','create_date':u'日期','old_cate_one':u'一级类目','old_cate_two':u'二级类目','old_cate_three':u'三级类目','impression_num':u'曝光数','impression_item_num':u'曝光商品数','click_num':u'点击数','uv':u'独立访客','place_user_num':u'下单买家数','place_order_num':u'下单量','place_product_qty':u'下单件数','placeorderrate':u'下单转化率','pay_user_num':u'支付买家数','m_user_num':u'男买家数','f_user_num':u'女买家数','pay_order_num':u'支付订单量','origin_qty': u'支付件数','origin_amount':u'支付金额','cvr':u'支付转化率','impression_cvr':u'曝光转化率','pay_item_num':u'支付商品数','acc_pay_item_num':u'月累计支付商品数','sale_ok_num':u'在售商品数','sale_ok_in_num':'印度在售商品数','new_item_num':u'新增商品数','sale_rate':u'动销率','return_quantity':u'退货数量','refund_total':u'退货金额'},inplace=True)
    return df_cateOne,df_cateTwo,df_all,df_cateThree


def main(**kwargs):
    redshift_conn = db_util.get_redshift_conn()

    df_cateOne, df_cateTwo, df_all, df_cateThree=get_cateIndexByDate(redshift_conn)
    
    writer = pd.ExcelWriter(u'/data/chulingling/'+str(start)+'前台类目交易日报.xlsx')
    df_all.reset_index().to_excel(writer,u'总体',index = False,encoding='utf8')
    df_cateOne.reset_index().to_excel(writer,u'一级类目',index = False,encoding='utf8')
    df_cateTwo.reset_index().to_excel(writer,u'二级类目',index = False,encoding='utf8')
    df_cateThree.reset_index().to_excel(writer,u'三级类目',index = False,encoding='utf8')
    writer.save()
    
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('/data/chulingling/'+str(start)+'前台类目交易日报.xlsx', 'data-warehouse-report', 'chulingling/'+ str(start)+'前台类目交易日报.xlsx')

    try:
        os.remove(u'/data/chulingling/' + str(start) + '前台类目交易日报.xlsx')
    except Exception:
        logging.info(u'failed to delete file /data/chulingling/' + str(start) + '前台类目交易日报.xlsx')


    redshift_conn.close()


