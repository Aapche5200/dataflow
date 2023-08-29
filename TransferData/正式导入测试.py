# -*- coding: utf-8 -*-
#coding=utf8
import time
import pymssql
import codecs
import psycopg2
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine

test_conn = create_engine('postgresql://data_etl:qwertyuiop@10.20.121.145:5432/report')

# oa_conn = create_engine('mssql+pymssql://sjfx:5VvXTWS%nfbPRex1Sb@10.20.120.114:1433/ABV5')
con_oa111 = create_engine(
        'mssql+pymssql://sjfx:5VvXTWS%nfbPRex1Sb@10.20.120.114:1433/ABV5?charset=utf8')
oa_conn = pymssql.connect(server='10.20.120.114', user='sjfx', password='5VvXTWS%nfbPRex1Sb', database='ABV5',
                          charset='UTF-8')

report_conn = psycopg2.connect(user='data_etl',
                               password='qwertyuiop',
                               host='10.20.121.145',
                               port=5432,
                               database='report')
report_sql = '''select 工号 ,
    姓名 ,身份,
    办事处 ,
    null 部门  ,
    大区名称  from ABV5.dbo.[XBBUserSaleStandardnameView]'''

report_data = pd.read_sql(report_sql, con_oa111)
if '身份' in report_data.columns:
    report_data['身份'] = report_data['身份'].apply(lambda x: x.encode('latin1').decode('gbk'))
print(report_data.head(10))

# cp_data = pd.ExcelFile(r'/Users/apache/Downloads/维护数据.xlsx')
# cpmodel_data = pd.read_excel(cp_data)

# sql ='select * from ds_itr_workorder_sign_out_config limit 10'
# df=pd.read_sql(sql,conn2)

# report_data.rename(columns={'年份': 'date_y', '季度': 'date_q', '月度': 'date_m', '确认日期': 'confirm_date',
#                             '大区': 'area', '考核单位': 'area_unit', '年度累计确认收入差值': 'income_diff',
#                             '当前待确认收入差值': 'unincome_diff'},inplace=True)
# report_data.to_sql('ods_py_xbbusersalestandardnameview', con=report_conn, if_exists='append', index=False)



report_data['部门'] = report_data['部门'].replace('', None)

report_data.to_sql('ods_py_xbbusersalestandardnameview', con=test_conn, if_exists='replace', index=False)

print("CHENGOGN")
