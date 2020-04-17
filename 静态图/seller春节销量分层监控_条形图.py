#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import psycopg2
import pymssql
import pandas as pd

#----当前值取数----
con1 = psycopg2.connect(user='operation',
        password='Operation123',
        host='jiayundatapro.cls0csjdlwvj.us-west-2.redshift.amazonaws.com',
        port=5439,
        database='jiayundata')

sql1 = (''' SELECT
	front_cate_one,
	qty_type,
	sum(num) as now_num
FROM
	(
	SELECT
		qty_type,
		front_cate_one,
		write_type,
		product_level,
		COUNT ( DISTINCT item_no ) AS num,
		SUM ( origin_amount ) AS gmv 
	FROM
		(
		SELECT A
			.item_no,
		CASE
				
				WHEN A.product_level = 5 THEN
				'头部商品' 
				WHEN A.product_level = 4 THEN
				'腰部商品' 
				WHEN A.product_level = 3 THEN
				'长尾商品' 
				WHEN A.product_level IN ( 1, 2, 6 ) THEN
				'测试商品' ELSE'其他' 
			END product_level,
CASE
	
	WHEN A.write_uid = 5 THEN
	'seller' ELSE'CF' 
	END write_type,
	A.front_cate_one,
CASE
		
		WHEN SUM ( sol.origin_qty ) >= 5 
		AND SUM ( sol.origin_qty ) < 7 THEN
			'[5--7)' 
			WHEN SUM ( sol.origin_qty ) >= 7 
			AND SUM ( sol.origin_qty ) < 14 THEN
				'[7--14)' 
				WHEN SUM ( sol.origin_qty ) >= 14 
				AND SUM ( sol.origin_qty ) < 35 THEN
					'[14--35)' 
					WHEN SUM ( sol.origin_qty ) >= 35 
					AND SUM ( sol.origin_qty ) < 70 THEN
						'[35--70)' 
						WHEN SUM ( sol.origin_qty ) >= 70 THEN
						'>=70' ELSE NULL 
					END qty_type,
	COUNT ( DISTINCT A.item_no ) AS sale_item_num,
	SUM ( sol.origin_qty ) AS origin_qty,
	COUNT ( DISTINCT so.user_id ) AS pay_user_num,
	SUM ( sol.origin_qty * sol.price_unit ) AS origin_amount 
FROM
	jiayundw_dim.product_basic_info_df
	AS A JOIN jiayundw_dm.sale_order_line_df AS sol ON sol.item_no = A.item_no
	JOIN jiayundw_dm.sale_order_info_df AS so ON sol.order_name = so.order_name 
WHERE
	sol.is_delivery = 0 
	AND DATE ( so.create_at + INTERVAL '8 hour' ) BETWEEN ( TRUNC( getdate ()) - 7 ) 
	AND ( TRUNC( getdate ()) - 1 ) 
GROUP BY
	1,
	2,
	3,
	4 
	) 
GROUP BY
	1,
	2,
	3,
	4 
	) 
WHERE
	write_type = 'seller' 
	AND qty_type IS NOT NULL 
	AND front_cate_one IS NOT NULL 
GROUP BY
	1,2 
ORDER BY
	1,2''') 
        
now_data = pd.read_sql(sql1,con1)
print(now_data.head(10))

#----目标值取数----
import pandas as pd
mubiao_data = pd.read_excel('/Users/apache/Downloads/7.xlsx',sheet_name = '处理表')
mubiao_data_1 = mubiao_data.drop('数量',axis=1) #删除列axis=1,删除行axis=0
mubiao_data_1.rename(columns={'算法3数量':'mubiao_num'},inplace=True) 
print(mubiao_data_1.head(10))

#----合并数据----
total_data =pd.merge(now_data,mubiao_data_1,on = ['front_cate_one','qty_type'])
print(total_data.head(10))

#----新增一列：完成率----
total_data["wanchenglv"] = total_data["now_num"]/total_data["mubiao_num"]
#total_data['wanchenglv'] = total_data['wanchenglv'].apply(lambda x: format(x,'.2%'))
print(total_data.head(10))

#----获取对应qty_type每组数据----
total_data_5_7 = total_data.loc[total_data['qty_type']=="[5--7)"]
print(total_data_5_7.head(10))
total_data_7_14 = total_data.loc[total_data['qty_type']=="[7--14)"]
total_data_14_35 = total_data.loc[total_data['qty_type']=="[14--35)"]
total_data_35_70 = total_data.loc[total_data['qty_type']=="[35--70)"]
total_data_70 = total_data.loc[total_data['qty_type']==">=70"]

#----先定义一个颜色组，保持所有类目维持统一的颜色----
colors = dict(zip(['is_null', 'other', 'Office & Books', 'Automobiles', 'Home Appliances',
                   'Men\'s Bags', 'Sports & Fitness', 'Women\'s Bags', 'Beauty & Health','Kids',
                   'Women\'s Shoes', 'Home', 'Men\'s Clothing', 'Women\'s Clothing', 'Men\'s Shoes',
                   'Mobiles & Accessories', 'Jewelry & Accessories', 'Watches', 'Electronics'],
                  ['#D62728', '#7F7F7F', '#1F77B4', '#AEC7E8', '#FF7F0E', '#FFBB78', '#2CA02C',
                   '#98DF8A', '#FF9896', '#9467BD', '#C5B0D5', '#8C564B', '#C49C94', '#E377C2',
                   '#F7B6D2', '#BCBD22', '#DBDB8D', '#17BECF', '#9EDAE5']))

#----开始作图-单个静态图----
from matplotlib import pyplot as plt
fig, ax = plt.subplots(ncols=5,figsize=(30,15))
plt.subplots_adjust(top=0.875,bottom=0.1,left=0.14,right=0.995,hspace=0.2,wspace=0.2)
plt.rcParams['font.sans-serif'] = ['SimHei'] #显示中文标签
plt.rcParams['axes.unicode_minus'] = False #显示正常符号
ax[2].text(0,12,'seller 销量分层监控',size=20,weight=600,ha='center')
ax[0].barh(total_data_5_7['front_cate_one'],total_data_5_7['wanchenglv'],
        color=[colors[x] for x in total_data_5_7['front_cate_one']])
ax[0].set_title('[5-7)') #设置子图标题
ax[0].set_xlim(0,max(total_data_5_7['wanchenglv']+0.2))
ax[0].set_xticks([]) #不显示X轴刻度
for i , (value,name) in enumerate(zip(total_data_5_7['wanchenglv'],
        total_data_5_7['now_num'])):
    ax[0].text(0.1,i,name,ha='left')
    ax[0].text(value,i,"{:.0%}".format(value),ha='left')

ax[1].barh(total_data_7_14['front_cate_one'],total_data_7_14['wanchenglv'],
        color=[colors[x] for x in total_data_7_14['front_cate_one']])
ax[1].set_title('[7-14)')
ax[1].set_xlim(0,max(total_data_7_14['wanchenglv'])+0.2)
ax[1].set_xticks([])
ax[1].set_yticks([])
for i , (value,name) in enumerate(zip(total_data_7_14['wanchenglv'],
        total_data_7_14['now_num'])):
    ax[1].text(0.1,i,name,ha='left')
    ax[1].text(value,i,"{:.0%}".format(value),ha='left')

ax[2].barh(total_data_14_35['front_cate_one'],total_data_14_35['wanchenglv'],
        color=[colors[x] for x in total_data_14_35['front_cate_one']])
ax[2].set_title('[14-35)')
ax[2].set_xlim(0,max(total_data_14_35['wanchenglv'])+0.2)
ax[2].set_xticks([])
ax[2].set_yticks([])
for i , (value,name) in enumerate(zip(total_data_14_35['wanchenglv'],
        total_data_14_35['now_num'])):
    ax[2].text(0.1,i,name,ha='left')
    ax[2].text(value,i,"{:.0%}".format(value),ha='left')

ax[3].barh(total_data_35_70['front_cate_one'],total_data_35_70['wanchenglv'],
         color=[colors[x] for x in total_data_35_70['front_cate_one']])
ax[3].set_title('[35--70)')
ax[3].set_xlim(0,max(total_data_35_70['wanchenglv'])+0.2)
ax[3].set_xticks([])
ax[3].set_yticks([])
for i , (value,name) in enumerate(zip(total_data_35_70['wanchenglv'],
        total_data_35_70['now_num'])):
    ax[3].text(0.1,i,name,ha='left')
    ax[3].text(value,i,"{:.0%}".format(value),ha='left')

ax[4].barh(total_data_70['front_cate_one'],total_data_70['wanchenglv'],
         color=[colors[x] for x in total_data_70['front_cate_one']])
ax[4].set_title('>=70')
ax[4].set_xlim(0,max(total_data_70['wanchenglv'])+0.2)
ax[4].set_xticks([])
ax[4].set_yticks([])
for i , (value,name) in enumerate(zip(total_data_70['wanchenglv'],
        total_data_70['now_num'])):
    ax[4].text(0.1,i,name,ha='left')
    ax[4].text(value,i,"{:.0%}".format(value),ha='left')
    
plt.show()

