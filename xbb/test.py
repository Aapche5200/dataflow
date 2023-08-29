#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import text, create_engine
import json
from compare.database.xbb import Dbcon_xbb
from compare.database.gp import DbCon_gp
from compare.excel import write2excel

conn2= create_engine('postgresql+psycopg2://data_etl:F3xgK#w9cO%40lWbk$@10.20.121.145:5432/report')
gp1=DbCon_gp('gp')


#xbb数据
gp=Dbcon_xbb('gp')
query_str1=text('SELECT data FROM tb_saas_customer limit 11')
df1=gp.read_sql(query_str1)


#xbb客户的data的一次解析
df1['data_json']=df1['data'].apply(lambda x:json.loads(x))

def parse(x):
    # 解析JSON
    _t = x['data_json']
    x['text_1'] = _t.get('text_1', '')
    x['text_24'] = _t.get('text_24', '')
    x['text_31'] = _t.get('text_31', '')
    x['text_5'] = _t.get('text_5', '')
    x['num_2'] = _t.get('num_2', '')
    x['num_5'] = _t.get('num_5', '')
    x['text_29'] = _t.get('text_29', '')
    x['text_15'] = _t.get('text_15', '')
    x['text_21'] = _t.get('text_21', '')
    x['text_23'] = _t.get('text_23', '')
    x['address_1'] = _t.get('address_1', '')
    x['text_25'] = _t.get('text_25', '')
    x['text_27'] = _t.get('text_27', '')
    x['text_28'] = _t.get('text_28', '')
    x['text_9'] = _t.get('text_9', '')
    x['text_13'] = _t.get('text_13', '')
    x['text_26'] = _t.get('text_26', '')
    x['text_41'] = _t.get('text_41', '')
    return x

df1 = df1.apply(parse, axis=1)
df=df1.drop(['data','data_json'],axis=1)
print(df)
print(df.dtypes)
print(df['address_1'])
df['address_1'] = df['address_1'].fillna('{}')

df['address_1'] = df['address_1'].str.replace("'", '"')
print(df['address_1'])
# df['address_json'] = df['address_1'].apply(lambda x: json.loads(x))
# print(df['address_1'])
    # def test1(x):
    #     print(x)
        #x=json.loads(x).

    #df['address_1']=df['address_1'].str.replace({"'":'"'})
    #print(df['address_1'].head())
    # df['test']=df['address_1'].apply(test1)
    # # df['test']=df['address_1'].apply(lambda x:json.loads(x).get('city'))
    # print(df.dtypes)
    # print(df['test'].head())


    # df.to_sql('test112', conn2, if_exists='replace', index=False)

    # print(df)


    # #address二次解析
    # df=pd.read_excel(r'C:\Users\kingsley.zhao\PycharmProjects\pythonProject\compare\excel\客户1.xlsx')
    # print(type(df['address_1']))
    #df2['address_1']=df2['address_1'].fillna('{}')
    # df2['address_1']=df2['address_1'].str.replace("'",'"')
    #
    # df2['address_json']=df2['address_1'].apply(lambda x:json.loads(x))
    #
    # def parse(x):
    #     # 解析JSON
    #     _t = x['address_json']
    #     x['city'] = _t.get('city', '')
    #     x['address'] = _t.get('address', '')
    #     x['district'] = _t.get('district', '')
    #     x['province'] = _t.get('province', '')
    #     return x
    #
    # #添加需要数据
    # customer = df2.apply(parse, axis=1)
    # # customer.insert(1,'serialNo',df2['serial_no'])
    # # customer.insert(3,'ownerId',df2['owner_id'])
    # df3=pd.concat(df,customer)
    # df4=df3.drop(['address_1','address_json'],axis=1)
    #
    # df4.to_sql('test112', conn2, if_exists='replace', index=False)