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


SELECT a.item_no,a.front_cate_one,a.write_uid,a.illegal_tags,product_level as 商品等级,b.pay_order_num as 支付订单量,b.origin_qty as 支付件数,
b.origin_amount as 支付金额
FROM dwd.product_info as a
left join
(select item_no,
       count(distinct so.order_name)  as pay_order_num,
			 count(distinct so.user_id)  as pay_user_num,
			 sum(sol.origin_qty)  as origin_qty,
			 sum(sol.origin_qty*sol.origin_real) as origin_amount
from jiayundw_dm.sale_order_info_df as so 
join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
where so.is_delivery=0 and date(so.create_at+interval '8 hour') between '20190331' and '20190529' 
group by 1)as b 
on b.item_no=a.item_no
WHERE a.active=1 and a.write_uid in ('1','2','3') AND product_level in ('3')
GROUP BY 1,2,3,4,5,6,7,8

 '''

    data1 = pd.read_sql(sql1, conn)

    return data1


if __name__ == '__main__':
    con = get_redshift_test_conn()
    data1 = get_data(con)

    writer = pd.ExcelWriter('zoupei' + time.strftime('%Y-%m-%d', time.localtime(time.time())) + '.xlsx')
    data1.to_excel(writer, sheet_name='zhoupei', index=False)

    # print(data1)

    os.chdir("F:/Python")
    writer.save()



