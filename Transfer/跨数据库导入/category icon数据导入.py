# ----数据库操作----
import pymssql
import pandas as pd
import psycopg2
from pyhive import hive
from sqlalchemy import create_engine

con_hive = hive.Connection(host="ec2-34-222-53-168.us-west-2.compute.amazonaws.com", port=10000, username="hadoop")

sql_ca_icon_redf = ('''
select event_time,one_catid,one_name ,two_catid ,	
two_name,three_catid ,three_name ,click_pv ,click_uv,	
pro_imp_pv,pro_imp_uv,pro_click_pv,pro_click_uv,null as shangpingjiagouuv,
null as	buynow_uv,pay_uv,gmv,null as xiadanuv,	null as zhifudingdanshu,null as xiadangmv,null as dingdanshu
from analysts.cate_result_wcm
where pt >='2020-03-31' and pt <= '2020-04-01'
''')

df_ca_icon_redf = pd.read_sql(sql_ca_icon_redf, con_hive)
print("需要导入数据预览--获取成功")

df_ca_icon_redf.rename(columns={'event_time': '日期', 'one_catid': '一级类目catid', 'one_name': '一级类目',
                                'two_catid': '二级类目catid', 'two_name': '二级类目', 'three_catid': '三级类目catid',
                                'three_name': '三级类目', 'click_pv': '点击pv', 'click_uv': '点击uv',
                                'pro_imp_pv': '商品曝光pv', 'pro_imp_uv': '商品曝光uv', 'pro_click_pv': '商品点击pv',
                                'pro_click_uv': '商品点击uv', 'shangpingjiagouuv': '商品加购uv',
                                'buynow_uv': '商品buynow_uv', 'pay_uv': '支付uv', 'gmv': '支付gmv',
                                'xiadanuv': '下单uv', 'zhifudingdanshu': '支付订单数', 'xiadangmv': '下单gmv',
                                'dingdanshu': '订单数', }, inplace=True)

engine_ms = create_engine("mssql+pymssql://sa:yssshushan2008@172.16.92.2:1433/CFflows?charset=utf8")
df_ca_icon_redf.to_sql('Cateicon数据', con=engine_ms, if_exists='append', index=False)

df_ms_icon = pd.read_sql('''
SELECT TOP
	10 CONVERT ( VARCHAR ( 100 ),日期, 23 ) AS 日期1 ,* 
FROM
	CFflows.dbo.Cateicon数据
ORDER BY ([日期]) Desc''', engine_ms)

print(df_ms_icon.head(5))
