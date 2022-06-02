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

start = '2020-05-27'

sql = """
select 
 pt "日期",
            item_no    "货号",
            pid    "商品id",
            name    "商品标题",
            cf_url    "cf商品url",
            cate_one_en    "后台一级类目",
            cate_two_en    "后台二级类目",
            cate_three_en    "后台三级类目",
            front_cate_one    "前台一级类目",
            front_cate_two    "前台二级类目",
            front_cate_three    "前台三级类目",
            create_date    "首次上架时间",
            active    "在架状态",
            illegal_tags    "商品标签",
            write_uid    "上货来源",
            tag_fb_nations    "禁售国家",
            tag_tr_nations    "禁运国家",
            sku_num    "sku数量",
            active_sku_num    "在架sku数量",
            cast(max_price_local as decimal(16,2))   "在架sku最高售价（英镑）",
            cast(min_price_local as decimal(16,2))   "在架sku最低售价（英镑）",
            cast(max_price_local_vip  as decimal(16,2))  "在架sku最高会员价（英镑）",
            cast(min_price_local_vip  as decimal(16,2))   "在架sku最低会员价（英镑）",
            cast(max_price_usd  as decimal(16,2)) "在架sku最高售价（美元）",
            cast(min_price_usd as decimal(16,2))   "在架sku最低售价（美元）",
            cast(max_price_usd_vip as decimal(16,2))  "在架sku最高会员价（美元）",
            cast(min_price_usd_vip as decimal(16,2))   "在架sku最低会员价（美元）",
            old_product_id    "cf商品id",
            cast(cf_max_price_usd as decimal(16,2))   "cf站点在架sku最高售价（美元）",
            cast(cf_min_price_usd as decimal(16,2))  "cf站点在架sku最低售价（美元）",
            cast(discount as decimal(16,2))   "wholee会员价折扣（与cf站点对比）",
            cast(max_sku_weight as decimal(16,2))   "在架sku最大重量（g）",
            cast(min_sku_weight as decimal(16,2))  "在架sku最小重量（g）",
            cast(max_sku_purchase_price as  decimal(16,2))  "在架sku最大采购价（rmb）",
            cast(min_sku_purchase_price as decimal(16,2))   "在架sku最小采购价（rmb）",
            impression_num    "曝光",
            click_num    "点击",
            add_num    "加购",
            uv    "商详访客",
            click_rate    "点击率",
            add_rate    "加购率",
            place_order_num    "下单量",
            place_user_num    "下单买家数",
            place_product_qty    "下单件数",
            cast(place_product_amount as decimal(16,2)) as "下单金额",
            order_rate    "下单转化率",
            pay_order_num    "支付订单量",
            pay_user_num    "支付买家数",
            origin_qty    "支付件数",
            cast(origin_amount as decimal(16,2)) as "支付金额",
            pay_cvr    "支付转化率",
            imp_cvr    "曝光转化率"
from analysts.wholee_product_daily_report_zyh
where pt='2020-07-30' and (impression_num>0 or origin_qty>0)

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

print(df)

writer = pd.ExcelWriter('lin' + '.xlsx')
df.to_excel(writer, sheet_name='商品数据', index=False)
os.chdir(r'/Users/apache/Downloads/A-python')
writer.save()
