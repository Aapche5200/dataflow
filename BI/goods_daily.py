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

start = yesterday

uv_start = (current_time + timedelta(-2)).strftime('%Y-%m-%d') + " 16:00:00"
uv_end = (current_time + timedelta(-1)).strftime('%Y-%m-%d') + " 16:00:00"
log_start=(current_time + timedelta(-3)).strftime('%Y-%m-%d')
log_end=(current_time+ timedelta(-0)).strftime('%Y-%m-%d')




def get_data(redshift_conn):
    sql="""
    select a.item_no,pro.cate_one_en as cate_one,pro.cate_two_en as cate_two,pro.cate_three_en as cate_three,front_cate_one as old_cate_one,
			pro.create_date as launch_date,pro.active as sale_ok,pro.price as list_price,pro.product_level,pro.illegal_tags,pro.write_uid,pro.discount,
            pro.rating,pro.rating_num,
            impression_num,uv,click_num,add_num,wishlist_num,
            place_order_num,place_user_num,place_product_qty,place_product_amount,
            pay_order_num,pay_user_num,origin_qty,origin_amount,m_user_num,f_user_num,
            return_quantity,refund_total,
            case when supply_chain_risk_flag=-1 then '黑名单'
                  when supply_chain_risk_flag=1 then '白名单'
                  when supply_chain_risk_flag=0 and is_grey_good=0 then '灰名单'
                  when supply_chain_risk_flag=0 and is_grey_good=1 then '偏白灰名单' end supply_chain_type,
            rebuy_30d_a
    from
    (select item_no,
               count(distinct case when event_type in ('product','impression') and mid='1.5.1.1' then cid else null end) as uv,
               count(case when  event_type='product' and mid like '%.9.1' then cid else null end) as impression_num,
               count(case when  event_type='click' and mid like '%.9.1' then cid else null end) as click_num, 
               count(case when  event_type='click' and mid='1.5.4.4' then cid else null end) as add_num,
               count(case when  event_type='click' and mid='1.5.4.1'  then cid else null end) as wishlist_num
        from 
            (select date(server_time+interval '8 hour') create_date,cid,pid,mid,event_type,country_code
             from spectrum_schema.user_trace 
             where event_type in ('product','click','impression')  
             and log_date >='{3}' and log_date <='{4}' 
             and server_time>='{1}' and server_time<'{2}'
            ) as ua 
        join jiayundw_dim.product_basic_info_df as pro on pro.pid=ua.pid
        group by 1
        having count(case when  event_type='product' and mid like '%.9.1' then cid else null end)>500
    ) as a 
    left join 
    (select item_no,
            count(distinct so.order_name)  as place_order_num,
            count(distinct so.user_id)  as place_user_num,
            sum(sol.product_qty)  as place_product_qty,
            sum(sol.product_qty*sol.price_unit) as  place_product_amount
        from jiayundw_dm.sale_order_info_df as so 
        join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
        where  sol.is_delivery=0  and date(so.order_at+interval '8 hour') ='{0}'
        group by 1
    ) as b on b.item_no=a.item_no
    left join 
    (select item_no,
            count(distinct so.order_name)  as pay_order_num,
            count(distinct so.user_id)  as pay_user_num,
            sum(sol.origin_qty)  as origin_qty,
            sum(sol.origin_qty*sol.price_unit) as origin_amount,
            count(distinct case when so.gender='men' then so.user_id end) as m_user_num,
            count(distinct case when so.gender='women' then so.user_id end) as f_user_num
    from jiayundw_dm.sale_order_info_df as so 
    join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
    where  sol.is_delivery=0  and date(so.create_at+interval '8 hour') ='{0}'
    group by 1
    ) as c on c.item_no=a.item_no
    left join 
    (select a.product_no as item_no,
                    sum(a.quantity) as return_quantity,
                    sum(a.refund_total) as refund_total
    from order_center.return_request_line as a 
    join order_center.return_request as b on b.return_id=a.return_id and b.order_name=a.order_name
    where a.state = 4 and a.flag = 0 and b.return_type in (0,1) and date(a.update_at+interval '8 hour')='{0}'
    group by product_no
    ) as d on d.item_no=a.item_no
    left join jiayundw_dim.product_basic_info_df as pro on pro.item_no=a.item_no
    left join
    (select distinct item_id,supply_chain_risk_flag,is_grey_good 
     from supply_chain.supply_chain_risk_score_info_log
     where log_date='{0}' group by 1,2,3
    ) as sc on sc.item_id=a.item_no
    left join 
    (select product_no,rebuy_30d_a
    from supply_chain.supply_chain_pno_rebuy_rating
    where date(timestamptz)=trunc(getdate())
    ) as rebuy on rebuy.product_no=a.item_no
    """.format(start,uv_start,uv_end,log_start,log_end)
    df=pd.read_sql(sql,redshift_conn)
    return df


def get_stat(redshift_conn):

    df=get_data(redshift_conn)
    
    df['click_rate']=df['click_num']/df['impression_num']       
    df['wishlist_rate']=df['wishlist_num']/df['impression_num']
    df['add_rate'] = df['add_num'] / df['impression_num']
    df['placeOrderRate'] = df['place_user_num'] / df['uv']     
    df['cvr']=df['pay_user_num']/df['uv']
    df['create_date']=start
    df.drop_duplicates('item_no', inplace=True)
    
    df=df[['create_date','item_no','launch_date','sale_ok','supply_chain_type','list_price','cate_one','cate_two','cate_three','old_cate_one','product_level','illegal_tags','write_uid','discount','impression_num','uv','click_num','wishlist_num','add_num','click_rate','wishlist_rate','add_rate','place_user_num','place_order_num','place_product_qty','placeOrderRate',
           'pay_user_num','m_user_num','f_user_num','pay_order_num','origin_qty','origin_amount','cvr','return_quantity','refund_total','rating_num','rating','rebuy_30d_a']]
    df.rename(columns={'create_date':u'日期','item_no':u'货号','cate_one':u'一级类目','cate_two':u'二级类目','cate_three':u'三级类目','old_cate_one':u'前台一级类目','launch_date':u'上架日期','sale_ok':u'是否在架','supply_chain_type':u'供应链标签','list_price':u'售价','product_level':u'商品等级','illegal_tags':u'商品标签','write_uid':u'上货来源','discount':u'折扣','impression_num':u'曝光量','uv':u'访客数','click_num':u'点击数','wishlist_num':u'收藏数','add_num':u'加购数',
                       'click_rate':u'点击率','wishlist_rate':u'收藏率','add_rate':u'加购率','place_user_num':u'下单买家数','place_order_num':u'下单量','place_product_qty':u'下单件数','placeOrderRate':u'下单转化率',
                       'pay_user_num':u'支付买家数','m_user_num':u'男买家数','f_user_num':u'女买家数','pay_order_num':u'支付订单量','origin_qty':u'支付件数','origin_amount':u'支付金额','cvr':u'支付转化率','return_quantity':u'退货件数','refund_total':u'退货金额','rating_num':u'历史评论数量','rating':u'历史评分','rebuy_30d_a':u'30日复购率'},inplace=True)
    return df

def main(**kwargs):
    redshift_conn = db_util.get_redshift_conn()
    df=get_stat(redshift_conn)

    df.to_excel(u'/data/chulingling/'+str(start)+'商品日报.xlsx',encoding= 'utf8',index=False)
    
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('/data/chulingling/'+str(start)+'商品日报.xlsx', 'data-warehouse-report', 'chulingling/'+str(start)+'商品日报.xlsx')

    try:
        os.remove('/data/chulingling/'+str(start)+'商品日报.xlsx')
    except Exception:
        logging.info(u'failed to delete file /data/chulingling/'+str(start)+'商品日报.xlsx')
    
    redshift_conn.close()


