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

select
    biz_date
    ,ic.front_cate_one
    ,ic.front_cate_two
    ,campaign_type
    ,cn.id
    ,cn.category_english_name
    ,cn.category_chinese_name
    ,sum(spend) as cost
    ,sum(mobile_app_install) as installs
    ,sum(fb_mobile_purchase+fb_pixel_purchase) as purchase
    ,sum(fb_mobile_purchase_value+fb_pixel_purchase_value) as purchaseValiue
    ,sum(impressions) as impression
    ,sum(clicks_all) as clicks 
    ,country_code as country FROM
(select * from marketing.facebook_dpa_elimination where biz_date>='2019-08-11' and biz_date<='2019-08-24') t1
       left join odoo_own.product_template pro on t1.product_id = pro.product_no
       left join odoo_own_public.category_new_product_template_rel re on re.product_template_id = pro.id
       left join odoo_own_public.category_new cn on cn.id = re.category_new_id 
       LEFT JOIN dwd.product_info ic ON ic.item_no = t1.product_id
group by 
     biz_date
    ,ic.front_cate_one
    ,ic.front_cate_two
    ,campaign_type
    ,cn.id
    ,cn.category_english_name
    ,cn.category_chinese_name
    ,country
order by biz_date,ic.front_cate_one,ic.front_cate_two,cn.id,campaign_type



 '''

    data1 = pd.read_sql(sql1, conn)

    return data1


if __name__ == '__main__':
    con = get_redshift_test_conn()
    data1 = get_data(con)

    writer = pd.ExcelWriter('类目推广' + time.strftime('%Y-%m-%d', time.localtime(time.time())) + '.xlsx')
    data1.to_excel(writer, sheet_name='FB数据', index=False)

    # print(data1)

    os.chdir("D:/Python")
    writer.save()



