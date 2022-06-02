import prestodb
import pandas as pd
import os
from sqlalchemy.engine import create_engine

conn = prestodb.dbapi.connect(
    host='ec2-54-68-88-224.us-west-2.compute.amazonaws.com',
    port=80,
    user='hadoop',
    catalog='hive',
    schema='default',
)

start = '2020-07-14'

sql = """

select 
item_no,
log_date,
online_date,
cate_one_cn,
cate_two_cn,
cate_three_cn,
front_cate_one,
front_cate_two,
front_cate_three,
product_level,
illegal_tags,
write_uid,
rating,
rating_num,
seller_name,
active,
price,
supply_chain_type,
supply_chain_risk_reason,
uv,
click_num,
impression_num,
pay_order_num,
origin_qty,
pay_user_num,
origin_amount,
place_order_num,
place_origin_qty,
place_user_num,
palce_cvr,
pay_cvr,
click_cvr
from analysts.yss_goods_daily_report
where pt='{0}'

""".format(start)
cursor = conn.cursor()
cursor.execute(sql)
data = cursor.fetchall()
column_descriptions = cursor.description
if data:
    df = pd.DataFrame(data)
    df.columns = [c[0] for c in column_descriptions]
else:
    df = pd.DataFrame()

df['加购数'] = ''
df['收藏数'] = ''
df['收藏率'] = ''
df['加购率'] = ''
df['男买家数'] = ''
df['女买家数'] = ''

df.rename(
    columns={
        'log_date': '数据日期',
        'item_no': '货号',
        'online_date': '上架时间',
        'active': '状态',
        'cate_one_cn': '一级类目',
        'cate_two_cn': '二级类目',
        'cate_three_cn': '三级类目',
        'front_cate_one': '前台一级类目',
        'front_cate_two': '前台二级',
        'front_cate_three': '前台三级',
        'price': '售价',
        'product_level': '商品等级',
        'illegal_tags': '商品标签',
        'write_uid': '上货来源',
        'seller_name': '商家名称',
        'supply_chain_type': '供应链标签',
        'supply_chain_risk_reason': '黑名单原因',
        'impression_num': '曝光量',
        'click_num': '点击数',
        '加购数': '加购数',
        '收藏数': '收藏数',
        'uv': '访客数',
        'click_cvr': '点击率',
        '收藏率': '收藏率',
        '加购率': '加购率',
        'place_user_num': '下单买家数',
        'place_order_num': '下单量',
        'place_origin_qty': '下单件数',
        'palce_cvr': '下单转化率',
        'pay_user_num': '支付买家数',
        '男买家数': '男买家数',
        '女买家数': '女买家数',
        'pay_order_num': '支付订单量',
        'origin_qty': '支付件数',
        'origin_amount': '支付金额',
        'pay_cvr': '支付转化率',
        'rating_num': '历史评论数',
        'rating': '历史评分',
    },
    inplace=True)

order = ['数据日期', '货号', '上架时间', '状态', '一级类目', '二级类目', '三级类目', '前台一级类目',
         '前台二级', '前台三级', '售价', '商品等级', '商品标签', '上货来源', '商家名称', '供应链标签',
         '黑名单原因', '曝光量', '点击数', '加购数', '收藏数', '访客数', '点击率', '收藏率', '加购率',
         '下单买家数', '下单量', '下单件数', '下单转化率', '支付买家数', '男买家数', '女买家数',
         '支付订单量', '支付件数', '支付金额', '支付转化率', '历史评论数', '历史评分', ]

df = df[order]
print(df)

engine_ms = create_engine("mssql+pymssql://sa:yssshushan2008@172.16.92.2:1433/CFgoodsday?charset=utf8")
df.to_sql('商品数据', con=engine_ms, if_exists='append', index=False)

sql_ms = ('''
select top 10 数据日期 from CFgoodsday.dbo.商品数据
order by 数据日期 desc
''')

df_ms = pd.read_sql(sql_ms, engine_ms)
print(df_ms)
