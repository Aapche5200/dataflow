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

SELECT a.item_no,a.front_cate_one,a.front_cate_two,a.front_cate_three,a.write_uid,product_level as 商品等级,b.pay_order_num as 支付订单量,b.origin_qty as 支付件数,
b.origin_amount as 支付金额,aa.商品曝光,aa.商品访客,aa.商品点击,a.illegal_tags as 商品标签,a.rating as 历史评分,a.price as 商品价格
FROM dwd.product_info as a
left join
(select item_no,
       count(distinct so.order_name)  as pay_order_num,
			 count(distinct so.user_id)  as pay_user_num,
			 sum(sol.origin_qty)  as origin_qty,
			 sum(sol.origin_qty*sol.origin_real) as origin_amount,
			 sol.sku
from jiayundw_dm.sale_order_info_df as so 
join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
where is_delivery=0 and date(so.create_at+interval '8 hour') between '20190610' and '20190710' 
group by 1,6)as b 
on b.item_no=a.item_no
LEFT JOIN
(
select a.pid,b.item_no,b.front_cate_one,b.front_cate_two,b.front_cate_three,
       count(case when event_type='product' then a.cid else null end) as 商品曝光,
       count(DISTINCT case when event_type='product' then a.cid else null end) as 商品访客,
       count(DISTINCT case when event_type='click' then a.cid else null end) as 商品点击
from spectrum_schema.user_trace a
       left join dwd.product_info b on a.pid = b.pid
       where a.log_date>='2019-06-09'
       and a.log_date<='2019-07-11'
       and a.country_code = 'in'
       and a.server_time BETWEEN '2019-06-09 16:00:00' and '2019-07-10 15:59:59' 
       and a.event_type in ( 'product','click')
       and mid like '%.9.1'
       and a.pid>0
       group by 1,2,3,4,5
) as aa
on a.item_no=aa.item_no
WHERE a.active=1 and a.front_cate_one in ('Women''s Clothing','Men''s Clothing',
'Women''s Shoes','Women''s Bags','Watches','Men''s Bags','Men''s Shoes')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15


 '''

    data1 = pd.read_sql(sql1, conn)

    return data1


if __name__ == '__main__':
    con = get_redshift_test_conn()
    data1 = get_data(con)

    writer = pd.ExcelWriter('商品支付流量' + time.strftime('%Y-%m-%d', time.localtime(time.time())) + '.xlsx')
    data1.to_excel(writer, sheet_name='商品支付流量', index=False)

    # print(data1)

    os.chdir("F:/Python")
    writer.save()



