import psycopg2
import pandas as pd
import pymssql
import prestodb
# ----时间处理----
from datetime import datetime, timedelta

end_time = '2020-07-08'
start_time = (datetime.strptime(end_time, '%Y-%m-%d') - timedelta(days=7)).strftime('%Y-%m-%d')  # 当前时间处理
print(start_time, end_time)

# ----redshift连接----
con_red = prestodb.dbapi.connect(
    host='ec2-54-68-88-224.us-west-2.compute.amazonaws.com',
    port=80,
    user='hadoop',
    catalog='hive',
    schema='default',
)

# ----redshift取数----
sql_red = ('''
select log_date,
case when write_uid=5 then 'seller' else 'CF' end laiyuan,
case when count(distinct product_no)=0 then 0 
    else cast(count(distinct case when product_level=3 then product_no else null end) as double)/cast(count(distinct product_no) as double) end rate1_3,
case when count(distinct product_no)=0 then 0 
    else cast(count(distinct case when product_level=-2 then product_no else null end) as double)/cast(count(distinct product_no) as double) end rate2_2
from
(select  DISTINCT h.log_date,h.product_no,
				h.product_level,
				g.product_level as status_1d,
				f.product_level as status_2d,
				e.product_level as status_3d,
				d.product_level as status_4d,
				c.product_level as status_5d,
				b.product_level as status_6d,
				a.product_level as status_7d,
				write_uid
from  (select DISTINCT log_date,product_no,product_level from jiayundw_dws.product_info_history_df where log_date BETWEEN '{0}' and '{1}' ) as a 
left join (select DISTINCT log_date,product_no,product_level from jiayundw_dws.product_info_history_df where log_date BETWEEN '{0}' and '{1}' ) as b 
on a.product_no=b.product_no and date(a.log_date) + interval '1' day=date(b.log_date)
left join (select DISTINCT log_date,product_no,product_level from jiayundw_dws.product_info_history_df where log_date BETWEEN '{0}' and '{1}' ) as c 
on a.product_no=c.product_no and date(a.log_date) + interval '2' day=date(c.log_date)
left join (select DISTINCT log_date,product_no,product_level from jiayundw_dws.product_info_history_df where log_date BETWEEN '{0}' and '{1}' ) as d 
on a.product_no=d.product_no and date(a.log_date) + interval '3' day =date(d.log_date)
left join (select DISTINCT log_date,product_no,product_level from jiayundw_dws.product_info_history_df where log_date BETWEEN '{0}' and '{1}' ) as e 
on a.product_no=e.product_no and date(a.log_date) + interval '4' day=date(e.log_date)
left join (select DISTINCT log_date,product_no,product_level from jiayundw_dws.product_info_history_df where log_date BETWEEN '{0}' and '{1}' ) as f 
on a.product_no=f.product_no and date(a.log_date) + interval '5' day=date(f.log_date)
left join (select DISTINCT log_date,product_no,product_level from jiayundw_dws.product_info_history_df where log_date BETWEEN '{0}' and '{1}' ) as g 
on a.product_no=g.product_no and date(a.log_date) + interval '6' day=date(g.log_date)
left join (select DISTINCT log_date,product_no,product_level from jiayundw_dws.product_info_history_df where log_date BETWEEN '{0}' and '{1}' ) as h 
on a.product_no=h.product_no and date(a.log_date) + interval '7' day=date(h.log_date)
left join jiayundw_dim.product_basic_info_df as  x on x.item_no=h.product_no
where h.log_date is not null) as a
where status_1d in (4,5) 
and status_2d in (4,5)
and status_3d in (4,5)
and status_4d in (4,5)
and status_5d in (4,5)
and status_6d in (4,5)
and status_7d in (4,5)
group by 1,2
ORDER BY 1
''').format(start_time, end_time)

cursor = con_red.cursor()
cursor.execute(sql_red)
data = cursor.fetchall()
column_descriptions = cursor.description
if data:
    data_red = pd.DataFrame(data)
    data_red.columns = [c[0] for c in column_descriptions]
else:
    data_red = pd.DataFrame()

print(data_red)

# ----通过下面方法连接sqlserver数据库----
from sqlalchemy import create_engine

engine = create_engine("mssql+pymssql://sa:yssshushan2008@172.16.92.2:1433/CFcategory?charset=utf8")

# ----向sqlserver导入数据----
data_red.to_sql('product_level_change_7days', con=engine, index=False, if_exists='append')

# ----sqlserver 表中取数 查看是否导入完成----
data_sqlserver = pd.read_sql('''select top 10 * from product_level_change_7days order by log_date DESC''', engine)
print(data_sqlserver.head(10))

# ----sqlserver 表中取数 用来做曲线图----
sql_sqlserver = ('''select * from CFcategory.dbo.product_level_change_7days where 
                 log_date BETWEEN getdate()-30 and getdate()-1''')

data_sqlserver_quxian = pd.read_sql(sql_sqlserver, engine)
print(data_sqlserver_quxian.head(10))

# ----对数据进行处理----
data_zuotu_seller = data_sqlserver_quxian[data_sqlserver_quxian['laiyuan'].eq('seller')]
data_zuotu_cf = data_sqlserver_quxian[data_sqlserver_quxian['laiyuan'].eq('CF')]
print(data_zuotu_cf.head(10))

# ----开始画图----
from matplotlib import pyplot as plt
import matplotlib.ticker as ticker  # 加载ticker设置坐标轴刻度

fig, ax = plt.subplots(nrows=2, figsize=(10, 10))
plt.subplots_adjust(hspace=0.5)
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']  # 显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 显示正常符号
plt.rcParams['font.size'] = 10  # 设置默认字体大小

plt.suptitle('前7天是4&5等级商品当日变成3或者-2趋势图', size=15, weight=400, ha='center', color='grey')
ax[0].plot(data_zuotu_seller['log_date'], data_zuotu_seller['rate1_3'], label='Seller')
ax[0].plot(data_zuotu_cf['log_date'], data_zuotu_cf['rate1_3'], label='CF')
ax[0].legend()
ax[0].yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:.1%}'))
ax[0].set_title('头腰流转到等级3趋势图', size=12, weight=300, ha='center')
ax[0].set_xticklabels(labels=data_zuotu_cf['log_date'], rotation=45, ha='right')

ax[1].plot(data_zuotu_seller['log_date'], data_zuotu_seller['rate2_2'], label='Seller')
ax[1].plot(data_zuotu_cf['log_date'], data_zuotu_cf['rate2_2'], label='CF')
ax[1].legend()
ax[1].yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:.1%}'))
ax[1].set_title('头腰流转到等级-2趋势图', size=12, weight=300, ha='center')
ax[1].set_xticklabels(labels=data_zuotu_cf['log_date'], rotation=45, ha='right')
plt.savefig('test2.jpg')  # 保存图片
plt.show()
