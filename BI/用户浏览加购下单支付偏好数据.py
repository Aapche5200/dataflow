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

select date(a.server_time + interval'8 hour') fw_date,'0单' as user_type,
b.cate_one_cn,b.cate_two_cn,b.cate_three_cn,
case when b.price>=0 and b.price<=5 then '0-5'
when b.price>5 and b.price<=10 then '5-10'
when b.price>10 and b.price<=20 then '10-20'
when b.price>20 and b.price<=30 then '20-30'
when b.price>30 and b.price<=40 then '30-40'
when b.price>40 and b.price<=50 then '40-50'
when b.price>50 and b.price<=60 then '50-60'
else '>60' end as price_range,
count(DISTINCT case when event_type='product' and mid like '%.9.1' then cid else null end) as impression_uv,
count(DISTINCT case when event_type='click' and mid like '%.9.1' then cid else null end) as click_uv,
count(DISTINCT case when event_type='product' and mid ='1.5.1.1' then cid else null end) as uv,
count(DISTINCT case when event_type='click' and mid ='1.5.4.4' then cid else null end) as add_uv
from  public.user_trace_cmp  a 
join  dwd.product_info b on a.pid=b.pid
where a.log_date>='2019-06-14'
and a.log_date<= '2019-07-16'
and a.server_time  between '20190614 16:00:00' and '20190715 15:59:59'
and event_type in ('product','click')
and country_code='in'
and a.cid in 
(select DISTINCT cid from analysts.shushan_goods_users where so_num_history=0)
group by 1,2,3,4,5,6


 '''

    data1 = pd.read_sql(sql1, conn)

    return data1


if __name__ == '__main__':
    con = get_redshift_test_conn()
    data1 = get_data(con)

    writer = pd.ExcelWriter('用户浏览加购等偏好数据' + time.strftime('%Y-%m-%d', time.localtime(time.time())) + '.xlsx')
    data1.to_excel(writer, sheet_name='用户偏好数据', index=False)

    # print(data1)

    os.chdir("F:/Python")
    writer.save()



