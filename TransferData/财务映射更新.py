# -*- coding: utf-8 -*-
import time
import psycopg2
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

report_conn = create_engine(
    'postgresql+psycopg2://python_etl:xFFUfuXDA4e5CVC1@10.20.121.145:5432/report')

Excel_data = pd.ExcelFile(
    r'C:\Users\allan.yin\Desktop\工作交接\数据治理\产品数据治理-完成.xlsx')
dtype = {
    'pro_model_id': str,
    'pro_id': str,
    'pro_class_id': str,
    'pro_line_id': str,
    'compare': str
}
report_data = pd.read_excel(Excel_data, '补充产线ID', dtype=dtype)

report_data.to_sql('ods_finance_map_relation', con=report_conn, if_exists='append',
                   index=False)

test_sql = 'select count(*) from ods_finance_map_relation'
print(pd.read_sql(test_sql, report_conn))
