import pymssql
import pandas as pd  # 数据处理例如：读入，插入需要用的包
import numpy as np  # 平均值中位数需要用的包
import os  # 设置路径需要用的包
import psycopg2
from datetime import datetime, timedelta  # 设置当前时间及时间间隔计算需要用的包
import prestodb
from sqlalchemy.engine import create_engine
from pyhive import hive
from impala.dbapi import connect

con_mssql = pymssql.connect("172.16.92.2", "sa", "yssshushan2008", "CFflows", charset="utf8")
con_redshift = psycopg2.connect(user='operation', password='Operation123',
                                host='jiayundatapro.cls0csjdlwvj.us-west-2.redshift.amazonaws.com',
                                port=5439, database='jiayundata')
con_hive = prestodb.dbapi.connect(host='ec2-54-213-119-155.us-west-2.compute.amazonaws.com', port=8889, user='hadoop',
                                  catalog='hive', schema='default', )

sql_hive_goods = ('''
select  * from analysts.aws_apply_return_reason
where delivered_date between date('2019-10-15') and date('2020-01-15') and
item_no in (

'DSD003167168N',
'MCT001765537N',
'MCC001979087N',
'MTT004879544N',
'MJA005139941N',
'MJA003173178N',
'MEA002303563N',
'MCC001979797N',
'XXX000604225N',
'MCC002078884N',
'MCC001550921N',
'XXX000120252N',
'MBS002380568N',
'MBS003183706N',
'MEB005210208N',
'MTT004879552N',
'XXX000501717N',
'XXX000665324N',
'MSH002905835N',
'MBS002934254N',
'MJA003481389N',
'MCC002050300N',
'MEB003535061N',
'MET004153734N',
'MCH002007213N',
'CSS001537066N',
'MCI000724799N',
'MEB002367939N',
'AMC000446171N',
'MCC002136222N',
'CDR000453970N',
'WSS002987991N',
'WOS002382478N',
'WDR003860684N',
'XXX000706081N',
'HCA002748389N',
'BBL007022638N',
'DSD003167168N',
'PDS002604293N',
'HAP003565110N',
'CTO001240543N',
'EMP006976332N'
)
order by delivered_date
''')

cursor = con_hive.cursor()

cursor.execute(sql_hive_goods)
data_hive_goods = cursor.fetchall()
column_descriptions = cursor.description
if data_hive_goods:
    data_hive_goods_df = pd.DataFrame(data_hive_goods)
    data_hive_goods_df.columns = [c[0] for c in column_descriptions]
else:
    data_hive_goods_df = pd.DataFrame()

print(data_hive_goods_df.head(10))

# 写入Excel操作
writer = pd.ExcelWriter('周学长-数据' + '.xlsx')
data_hive_goods_df.to_excel(writer, sheet_name='周-货号', index=False)

os.chdir(r'/Users/apache/Downloads/A-python')
writer.save()
