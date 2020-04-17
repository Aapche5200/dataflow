# ----数据库操作----
import pymssql
import pandas as pd
import psycopg2
# redshift 数据库连接
con1 = psycopg2.connect(
        user='operation',
        password='Operation123',
        host='jiayundatapro.cls0csjdlwvj.us-west-2.redshift.amazonaws.com',
        port=5439,
        database='jiayundata')

# redshift 取数
sql1=('''
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
(select * from marketing.facebook_dpa_elimination where biz_date>='2019-12-10' 
and biz_date<='2019-12-11') t1
left join odoo_own.product_template pro on t1.product_id = pro.product_no
left join odoo_own_public.category_new_product_template_rel re on 
re.product_template_id = pro.id
left join odoo_own_public.category_new cn on cn.id = re.category_new_id
LEFT JOIN jiayundw_dim.product_basic_info_df as ic ON ic.item_no = t1.product_id
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
 ''')
data1 = pd.read_sql(sql1,con1)
print(data1.head(10))
# sql server 数据库连接
from sqlalchemy import create_engine
con2 = pymssql.connect("172.16.92.6","sa","yssshushan2008","CFflows",charset="utf8")
# 如果用pandas对sql server进行导入数据的话，必须使用下面方法连接数据库
engine = create_engine("mssql+pymssql://sa:yssshushan2008@172.16.92.6:1433/CFflows?charset=utf8")
# sql server 取数
data1.to_sql('FB推广数据表',con=engine,if_exists='append',index=False)# 注意：这里要跟R区分开，Python只需要填写表名就可以
    # 对于R 可以填写上dbo类名
data2 = pd.read_sql('''SELECT top 10 * from CFflows.dbo.FB推广数据表 WHERE biz_date >='2019-08-29' 
                    ORDER BY biz_date DESC''',engine)
print(data2.head(10))