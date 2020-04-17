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

select a.item_no,write_uid,front_cate_one,front_cate_two,front_cate_three,cate_one_cn,cate_two_cn,cate_three_cn,product_level,
    case when b.item_no is not null then 'stock_item' else null end as stock_type
 from 
dwd.product_info a
left join (select distinct item_no from analysts.idle_sku_union where date(updatetime)=date(getdate()-1)) b
on a.item_no = b.item_no
where a.item_no not in 
(select distinct item_no
from
 (select product_no item_no,tag_fb_nations,update_time
 from odoo.tag_product_record X
 where update_time = (select max(update_time) from odoo.tag_product_record Y where X.product_no = Y.product_no)
)
where tag_fb_nations like '%in%') and a.active=1


 '''

    data1 = pd.read_sql(sql1, conn)

    return data1


if __name__ == '__main__':
    con = get_redshift_test_conn()
    data1 = get_data(con)

    writer = pd.ExcelWriter('实际在售商品' + time.strftime('%Y-%m-%d', time.localtime(time.time())) + '.xlsx')
    data1.to_excel(writer, sheet_name='印度实际在售商品', index=False)

    # print(data1)

    os.chdir("F:/Python")
    writer.save()



