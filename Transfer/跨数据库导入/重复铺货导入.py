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

start = '2020-06-19'

sql = """
select * from analystsdev.yss_same_itm_rate
order by seller_id,cr_date

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

engine_ms = create_engine("mssql+pymssql://sa:yssshushan2008@172.16.92.2:1433/SellerData?charset=utf8")
df.to_sql('商家重复铺货率', con=engine_ms, if_exists='replace', index=False)

sql_ms = ('''
select top 10 cr_date from SellerData.dbo.商家重复铺货率
order by cr_date desc
''')

df_ms = pd.read_sql(sql_ms, engine_ms)
print(df_ms)
