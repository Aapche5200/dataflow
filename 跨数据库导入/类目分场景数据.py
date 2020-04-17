import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from pyhive import hive

con_hive = hive.Connection(host="ec2-34-222-53-168.us-west-2.compute.amazonaws.com", port=10000, username="hadoop")

sql_cate_changj = ('''
create table analysts.aoi_category_pages_ss as
select DISTINCT
       c.front_cate_one,
			 c.date_id,
			 c.channel,
			 c.shangpingbaoguanguv,
			 d.shangpingdianjiuv,
			 e.shangpingzhifuuv,
			 e.shangpingxiaoliang,
			 e.shangpinggmv
from
 (select
               b.front_cate_one,date_id,
							 case when mid='1.1.9.1' then '首页feed流'
                    when mid='1.3.9.1' then '购物车推荐'
                    when mid='1.5.9.1' then '商详页推荐'
                    when mid='1.10.9.1' then '闪购页'
                    when mid='1.15.9.1' then '搜索结果页'
                    when mid='1.105873.9.1' then '广告落地页'
                    when mid='1.108222.11.1' then '新人专区'
                    when mid='1.59.9.1' then 'seller页'    
							 end  as channel,
							 count(distinct cid) as shangpingbaoguanguv 
					  from jiayundw_dwd.flow_user_trace_da as a
						JOIN jiayundw_dim.product_basic_info_df  as b on a.pid = b.pid
					  where date_id>='2020-02-28'and date_id<='2020-03-17'
						      and server_time>='2020-02-28 16:00:00' and server_time <='2020-03-17 15:59:59' 
									and mid in ('1.1.9.1','1.3.9.1','1.5.9.1','1.10.9.1','1.15.9.1','1.105873.9.1','1.108222.11.1','1.59.9.1') and event_type='product'
						group by b.front_cate_one,date_id,
							 case when mid='1.1.9.1' then '首页feed流'
                    when mid='1.3.9.1' then '购物车推荐'
                    when mid='1.5.9.1' then '商详页推荐'
                    when mid='1.10.9.1' then '闪购页'
                    when mid='1.15.9.1' then '搜索结果页'
                    when mid='1.105873.9.1' then '广告落地页'
                    when mid='1.108222.11.1' then '新人专区'
                    when mid='1.59.9.1' then 'seller页'    
							 end
						union all
						select
                d.front_cate_one,date_id,
							  '整站app' as channel,
							  count(distinct cid) as shangpingbaoguanguv 
					  from jiayundw_dwd.flow_user_trace_da  as c
						JOIN jiayundw_dim.product_basic_info_df  as d on c.pid=d.pid
						where date_id>='2020-02-28'and date_id<='2020-03-17'
						      and server_time>='2020-02-28 16:00:00' and server_time <='2020-03-17 15:59:59'
									and (mid like'%9.1' or mid='1.108222.11.1') and event_type='product' 
						group by d.front_cate_one,date_id
						union all
						select
                f.front_cate_one,date_id,
							  'category页' as channel,
							  count(distinct cid) as shangpingbaoguanguv 
					  from jiayundw_dwd.flow_user_trace_da  as e
						JOIN jiayundw_dim.product_basic_info_df  as f on e.pid=f.pid
						where date_id>='2020-02-28'and date_id<='2020-03-17'
						      and server_time>='2020-02-28 16:00:00' and server_time <='2020-03-17 15:59:59'
									and mid='1.8.9.1' and fr='1.2' and event_type='product' 
						group by f.front_cate_one,date_id )c 
left join (select
               b.front_cate_one,date_id,
							 case when mid='1.1.9.1' then '首页feed流'
                    when mid='1.3.9.1' then '购物车推荐'
                    when mid='1.5.9.1' then '商详页推荐'
                    when mid='1.10.9.1' then '闪购页'
                    when mid='1.15.9.1' then '搜索结果页'
                    when mid='1.105873.9.1' then '广告落地页'
                    when mid='1.108222.11.1' then '新人专区'
                    when mid='1.59.9.1' then 'seller页'    
							 end  as channel,
							 count(distinct cid) as shangpingdianjiuv 
					  from jiayundw_dwd.flow_user_trace_da as a
						JOIN jiayundw_dim.product_basic_info_df  as b on a.pid=b.pid 
					  where date_id>='2020-02-28'and date_id<='2020-03-17'
						      and server_time>='2020-02-28 16:00:00' and server_time <='2020-03-17 15:59:59' 
									and mid in ('1.1.9.1','1.3.9.1','1.5.9.1','1.10.9.1','1.15.9.1','1.105873.9.1','1.108222.11.1','1.59.9.1') and event_type='click'
						group by b.front_cate_one,date_id,
							 case when mid='1.1.9.1' then '首页feed流'
                    when mid='1.3.9.1' then '购物车推荐'
                    when mid='1.5.9.1' then '商详页推荐'
                    when mid='1.10.9.1' then '闪购页'
                    when mid='1.15.9.1' then '搜索结果页'
                    when mid='1.105873.9.1' then '广告落地页'
                    when mid='1.108222.11.1' then '新人专区'
                    when mid='1.59.9.1' then 'seller页'    
							 end 
						union all
						select
                d.front_cate_one,date_id,
							  '整站app' as channel,
							  count(distinct cid) as shangpingdianjiuv 
					  from jiayundw_dwd.flow_user_trace_da as c
						JOIN jiayundw_dim.product_basic_info_df  as d on c.pid=d.pid
						where date_id>='2020-02-28'and date_id<='2020-03-17'
						      and server_time>='2020-02-28 16:00:00' and server_time <='2020-03-17 15:59:59'
									and (mid like'%9.1' or mid='1.108222.11.1') and event_type='click' 
						group by d.front_cate_one,date_id
						union all
						select
                f.front_cate_one,date_id,
							  'category页' as channel,
							  count(distinct cid) as shangpingdianjiuv 
					  from jiayundw_dwd.flow_user_trace_da  as e
						JOIN jiayundw_dim.product_basic_info_df  as f on e.pid=f.pid
						where date_id>='2020-02-28'and date_id<='2020-03-17'
						      and server_time>='2020-02-28 16:00:00' and server_time <='2020-03-17 15:59:59'
									and mid='1.8.9.1' and fr='1.2' and event_type='click' 
						group by f.front_cate_one,date_id) d on c.front_cate_one = d.front_cate_one and c.channel = d.channel and c.date_id=d.date_id
left join (select
               b.front_cate_one,pay_date date_id,
							 case when laiyuan='1.1' then '首页feed流'
							      when laiyuan='1.2' then 'category页'
                    when laiyuan='1.3' then '购物车推荐'
                    when laiyuan='1.5' then '商详页推荐'
                    when laiyuan='1.10' then '闪购页'
                    when laiyuan='1.15' then '搜索结果页'
                    when laiyuan='1.105873' then '广告落地页'
                    when laiyuan='1.108222' then '新人专区'
                    when laiyuan='1.59' then 'seller页'    
							 end  as channel,
							 count(distinct user_id) as shangpingzhifuuv,
							 sum(origin_qty)as shangpingxiaoliang,
							 sum(origin_qty*price_real) as shangpinggmv
            from analysts.user_pay_success_detail_cate_wcm as a
						JOIN jiayundw_dim.product_basic_info_df  as b on a.pid=b.pid 
					  where pay_date>='2020-02-28' and pay_date<='2020-03-17' 
									and laiyuan in ('1.1','1.2','1.3','1.5','1.10','1.15','1.105873','1.108222','1.59')
						group by b.front_cate_one,pay_date,
							 case when laiyuan='1.1' then '首页feed流'
							      when laiyuan='1.2' then 'category页'
                    when laiyuan='1.3' then '购物车推荐'
                    when laiyuan='1.5' then '商详页推荐'
                    when laiyuan='1.10' then '闪购页'
                    when laiyuan='1.15' then '搜索结果页'
                    when laiyuan='1.105873' then '广告落地页'
                    when laiyuan='1.108222' then '新人专区'
                    when laiyuan='1.59' then 'seller页'    
							 end
						union all
						select
						    pro.front_cate_one,date(so.create_at + interval '8' hour) date_id,
								'整站app' as channel,
								count(distinct so.user_id) as shangpingzhifuuv,
								sum(sol.origin_qty)as shangpingxiaoliang,
								sum(sol.origin_qty*sol.price_unit) as shangpinggmv
						from jiayundw_dm.sale_order_info_df so
            left join  jiayundw_dm.sale_order_line_df sol on so.order_name=sol.order_name
						join jiayundw_dim.product_basic_info_df pro on sol.item_no= pro.item_no
						where date(so.create_at + interval '8' hour)>='2020-02-28' and date(so.create_at + interval '8' hour)<='2020-03-17' 
						group by pro.front_cate_one,date(so.create_at + interval '8' hour))e on c.front_cate_one = e.front_cate_one and c.channel = e.channel and c.date_id=e.date_id
	WHERE c.date_id BETWEEN '2020-02-28' and '2020-03-17'
''')

data_cate_changj = pd.read_sql(sql_cate_changj, con_hive)

sql_cate_page = ('''
select * from analysts.aoi_category_pages_ss
''')
data_cate_page = pd.read_sql(sql_cate_page, con_hive)
print(data_cate_changj.head(10))

engine_ms = create_engine("mssql+pymssql://sa:yssshushan2008@172.16.92.2:1433/CFflows?charset=utf8")
data_cate_changj.to_sql('CategoryPages', con=engine_ms, if_exists='append', index=False)

sql_cate_pages = ('''
select top * from CategoryPages
order by log_date DESC
''')

data_cate_pages = pd.read_sql(sql_cate_pages, engine_ms)
print("导入成功")
