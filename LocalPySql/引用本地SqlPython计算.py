# -*- coding: utf-8 -*-
import time
import os
import psycopg2
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from ah.out_data.parket.db_con import DbCon

oa = DbCon.con_oa
pg = DbCon.con_gp

report_path = 'C:/Users/allan.yin/AppData/Roaming/JetBrains' \
              '/PyCharm2022.3/consoles/db/5fb6d55d-f4f7-461c-ad8e-de38cd59fdf3/'

report_file = 'test1.sql'

report_sql = open(report_path + report_file, 'r', encoding='utf8')
report_sql_txt = report_sql.readlines()
report_sql.close()
report_sql = "".join(report_sql_txt)
report_data = pd.read_sql(report_sql, pg)
# 指定列求和
print(report_data['确认收入'].sum())
# 北京的销售额求和
report_data[report_data['大区'] == '北区管理部']['确认收入'].sum()
# 北京的销售额求和，且满足年龄小于30岁
report_data[(report_data['大区'] == '北区管理部') & (report_data['考核单位'] == '甘肃办')]['确认收入'].sum()
# 指定列非空值计数
print(report_data['考核单位'].count())
# 计算空单元格个数
print(report_data['考核单位'].isnull().count())
# 计算不同性别的客户数
report_data['待确认收入类型'].value_counts()
# 计算北京女性的客户数
report_data[(report_data['大区'] == '北区管理部') & (report_data['考核单位'] == '甘肃办')]['待确认收入类型'].count()
