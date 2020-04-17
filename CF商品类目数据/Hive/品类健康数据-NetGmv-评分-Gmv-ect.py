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

# con_hivee = hive.Connection(host="ec2-52-40-41-119.us-west-2.compute.amazonaws.com", port=10000,
# username="yinshushan", database="default", auth="NOSASL")

# 一级类目sql
sql_hive_cateone_netgmv = '''
SELECT a.front_cate_one,a.seller_type,a.avg_rating,a.sum_rating_num,a.hei_num,a.bai_num,a.hui_num,a.baihui_num,b.net_gmv,b.total,b.refund_total
from
(
SELECT a.front_cate_one,case when write_uid=5 then 'seller' else 'cf' end seller_type,
AVG(a.rating) avg_rating,SUM(a.rating_num) sum_rating_num,
COUNT(DISTINCT case when b.supply_chain_risk_flag=-1 then item_id else null  end )  hei_num,
COUNT(DISTINCT case when b.supply_chain_risk_flag=1 then item_id else null  end )  bai_num,
COUNT(DISTINCT case when b.supply_chain_risk_flag=0 and is_grey_good=0 then item_id else null  end )  hui_num,
COUNT(DISTINCT case when b.supply_chain_risk_flag=-1 and is_grey_good=1 then item_id else null end  )  baihui_num
from jiayundw_dim.product_basic_info_df  a
left JOIN ods_supply_chain.supply_chain_risk_score_info_log  b on a.item_no=b.item_id
GROUP BY a.front_cate_one,2) as a
left JOIN
(select front_cate_one,a.seller_type,sum(a.total) as total,sum(a.refund_total) as refund_total,(sum(a.total)-sum(a.refund_total)) net_gmv
    from ( select a.create_date
        ,a.seller_type
        ,case when a.channel='cod' then 'cod'else'ppd'end channel
        ,a.warehouse
        ,a.item_no
        ,a.total
        ,a.refund_total
        ,case when a.is_delivery=0 then a.origin_qty else 0 end qty
        ,case when a.is_delivery=0 then a.refund_qty else 0 end refund_qty
        from analysts.tbl_order_detail a
        where a.create_date between date('2019-12-01') and  date('2019-12-31') 
            and a.pt = '2020-01-09'
        ) a
    left join jiayundw_dim.product_basic_info_df as b on b.item_no=a.item_no
    group by front_cate_one,a.seller_type) as b on a.front_cate_one=b.front_cate_one and a.seller_type=b.seller_type
'''

sql_mssql_cateone_rating = '''
SELECT datepart(year,日期)*100+datepart(MONTH,日期) as 月份,一级类目,业务类型 as seller_type ,SUM(独立访客) as 商详访客,sum(支付金额) as gmv,
sum(曝光数量) as 曝光量,sum(支付商品件数) as 销量,avg(支付转化率 ) as 日均转化率
from CFcategory.dbo.category_123_day
WHERE 业务类型!='total'  and (三级类目 = ' ' or 三级类目 is null) and (二级类目 = ' ' or 二级类目 is null) and 日期 BETWEEN '2019-12-01' and '2019-12-31'
GROUP BY datepart(year,日期)*100+datepart(MONTH,日期) ,一级类目,业务类型
ORDER BY datepart(year,日期)*100+datepart(MONTH,日期),一级类目,业务类型
'''

# 二级类目sql
sql_hive_catetwo_netgmv = '''
SELECT a.front_cate_one,a.front_cate_two,a.seller_type,a.avg_rating,a.sum_rating_num,a.hei_num,a.bai_num,a.hui_num,a.baihui_num,b.net_gmv,b.total,b.refund_total
from
(
SELECT a.front_cate_one,a.front_cate_two,case when write_uid=5 then 'seller' else 'cf' end seller_type,
AVG(a.rating) avg_rating,SUM(a.rating_num) sum_rating_num,
COUNT(DISTINCT case when b.supply_chain_risk_flag=-1 then item_id else null  end )  hei_num,
COUNT(DISTINCT case when b.supply_chain_risk_flag=1 then item_id else null  end )  bai_num,
COUNT(DISTINCT case when b.supply_chain_risk_flag=0 and is_grey_good=0 then item_id else null  end )  hui_num,
COUNT(DISTINCT case when b.supply_chain_risk_flag=-1 and is_grey_good=1 then item_id else null end  )  baihui_num
from jiayundw_dim.product_basic_info_df  a
left JOIN ods_supply_chain.supply_chain_risk_score_info_log  b on a.item_no=b.item_id
GROUP BY a.front_cate_one,a.front_cate_two,3) as a
left JOIN
(select front_cate_one,front_cate_two,a.seller_type,sum(a.total) as total,sum(a.refund_total) as refund_total,(sum(a.total)-sum(a.refund_total)) net_gmv
    from ( select a.create_date
        ,a.seller_type
        ,case when a.channel='cod' then 'cod'else'ppd'end channel
        ,a.warehouse
        ,a.item_no
        ,a.total
        ,a.refund_total
        ,case when a.is_delivery=0 then a.origin_qty else 0 end qty
        ,case when a.is_delivery=0 then a.refund_qty else 0 end refund_qty
        from analysts.tbl_order_detail a
        where a.create_date between date('2019-12-01') and  date('2019-12-31') 
            and a.pt = '2020-01-09'
        ) a
    left join jiayundw_dim.product_basic_info_df as b on b.item_no=a.item_no
    group by front_cate_one,front_cate_two,a.seller_type) as b on a.front_cate_one=b.front_cate_one and a.front_cate_two=b.front_cate_two and a.seller_type=b.seller_type
'''

sql_mssql_catetwo_rating = '''
SELECT datepart(year,日期)*100+datepart(MONTH,日期) as 月份,一级类目,[二级类目],业务类型 as seller_type ,SUM(独立访客) as 商详访客,sum(支付金额) as gmv,
sum(曝光数量) as 曝光量,sum(支付商品件数) as 销量,avg(支付转化率 ) as 日均转化率
from CFcategory.dbo.category_123_day
WHERE 业务类型!='total'  and (三级类目 = ' ' or 三级类目 is null) and (二级类目 != ' ' and 二级类目 is not null) and 日期 BETWEEN '2019-12-01' and '2019-12-31'
GROUP BY datepart(year,日期)*100+datepart(MONTH,日期) ,一级类目,二级类目,业务类型
ORDER BY datepart(year,日期)*100+datepart(MONTH,日期),一级类目,二级类目,业务类型
'''

# 三级类目sql
sql_hive_catethree_netgmv = '''
SELECT a.front_cate_one,a.front_cate_two,a.front_cate_three,a.seller_type,a.avg_rating,a.sum_rating_num,a.hei_num,a.bai_num,a.hui_num,a.baihui_num,b.net_gmv,b.total,b.refund_total
from
(
SELECT a.front_cate_one,a.front_cate_two,a.front_cate_three,case when write_uid=5 then 'seller' else 'cf' end seller_type,
AVG(a.rating) avg_rating,SUM(a.rating_num) sum_rating_num,
COUNT(DISTINCT case when b.supply_chain_risk_flag=-1 then item_id else null  end )  hei_num,
COUNT(DISTINCT case when b.supply_chain_risk_flag=1 then item_id else null  end )  bai_num,
COUNT(DISTINCT case when b.supply_chain_risk_flag=0 and is_grey_good=0 then item_id else null  end )  hui_num,
COUNT(DISTINCT case when b.supply_chain_risk_flag=-1 and is_grey_good=1 then item_id else null end  )  baihui_num
from jiayundw_dim.product_basic_info_df  a
left JOIN ods_supply_chain.supply_chain_risk_score_info_log  b on a.item_no=b.item_id
GROUP BY a.front_cate_one,a.front_cate_two,a.front_cate_three,4) as a
left JOIN
(select front_cate_one,front_cate_two,front_cate_three,a.seller_type,sum(a.total) as total,sum(a.refund_total) as refund_total,(sum(a.total)-sum(a.refund_total)) net_gmv
    from ( select a.create_date
        ,a.seller_type
        ,case when a.channel='cod' then 'cod'else'ppd'end channel
        ,a.warehouse
        ,a.item_no
        ,a.total
        ,a.refund_total
        ,case when a.is_delivery=0 then a.origin_qty else 0 end qty
        ,case when a.is_delivery=0 then a.refund_qty else 0 end refund_qty
        from analysts.tbl_order_detail a
        where a.create_date between date('2019-12-01') and  date('2019-12-31') 
            and a.pt = '2020-01-09'
        ) a
    left join jiayundw_dim.product_basic_info_df as b on b.item_no=a.item_no
    group by front_cate_one,front_cate_two,front_cate_three,a.seller_type) as b on a.front_cate_one=b.front_cate_one and
    a.front_cate_two=b.front_cate_two and a.front_cate_three=b.front_cate_three and a.seller_type=b.seller_type
'''

sql_mssql_catethree_rating = '''
SELECT datepart(year,日期)*100+datepart(MONTH,日期) as 月份,一级类目,[二级类目],三级类目,业务类型 as seller_type,SUM(独立访客) as 商详访客,sum(支付金额) as gmv,
sum(曝光数量) as 曝光量,sum(支付商品件数) as 销量,avg(支付转化率 ) as 日均转化率
from CFcategory.dbo.category_123_day
WHERE 业务类型!='total'  and (三级类目 != ' ' and 三级类目 is not null) and (二级类目 != ' ' and 二级类目 is not null) and 日期 BETWEEN '2019-12-01' and '2019-12-31'
GROUP BY datepart(year,日期)*100+datepart(MONTH,日期) ,一级类目,二级类目,三级类目,业务类型
ORDER BY datepart(year,日期)*100+datepart(MONTH,日期),一级类目,二级类目,三级类目,业务类型
'''

# hive数据库中net-gmv基础数据获取
cursor_one = con_hive.cursor()
# hive 一级类目net-gmv数据处理
cursor_one.execute(sql_hive_cateone_netgmv)
data_hive_cateone_netgmv = cursor_one.fetchall()
column_descriptions = cursor_one.description
if data_hive_cateone_netgmv:
    data_hive_cateone_netgmv_df = pd.DataFrame(data_hive_cateone_netgmv)
    data_hive_cateone_netgmv_df.columns = [c[0] for c in column_descriptions]
else:
    data_hive_cateone_netgmv_df = pd.DataFrame()

# hive 二级类目net-gmv数据处理
cursor_two = con_hive.cursor()
cursor_two.execute(sql_hive_catetwo_netgmv)
data_hive_catetwo_netgmv = cursor_two.fetchall()
column_descriptions_catetwo = cursor_two.description
if data_hive_catetwo_netgmv:
    data_hive_catetwo_netgmv_df = pd.DataFrame(data_hive_catetwo_netgmv)
    data_hive_catetwo_netgmv_df.columns = [c[0] for c in column_descriptions_catetwo]
else:
    data_hive_catetwo_netgmv_df = pd.DataFrame()
print(data_hive_catetwo_netgmv_df.head(10))

# hive 三级类目net-gmv数据处理
cursor_three = con_hive.cursor()
cursor_three.execute(sql_hive_catethree_netgmv)
data_hive_catethree_netgmv = cursor_three.fetchall()
column_descriptions_catethree = cursor_three.description
if data_hive_catethree_netgmv:
    data_hive_catethree_netgmv_df = pd.DataFrame(data_hive_catethree_netgmv)
    data_hive_catethree_netgmv_df.columns = [c[0] for c in column_descriptions_catethree]
else:
    data_hive_catethree_netgmv_df = pd.DataFrame()
print(data_hive_catethree_netgmv_df.head(10))
# hive 一级类目 查看列名及列名处理
data_hive_cateone_netgmv_df.columns.values
data_hive_cateone_netgmv_df.rename(columns={'front_cate_one': '一级类目', 'avg_rating': '历史综合评分',
                                            'sum_rating_num': '历史综合评分数量', 'hei_num': '黑名单数量',
                                            'bai_num': '白名单数量', 'hui_num': '灰名单数量',
                                            'baihui_num': '偏白灰名单数量', 'net_gmv': 'Net-Gmv', 'total': '总GMV',
                                            'refund_total': '退货金额'},
                                   inplace=True)
print(data_hive_cateone_netgmv_df.head(10))

# hive 二级类目 查看列名及列名处理
data_hive_catetwo_netgmv_df.columns.values
data_hive_catetwo_netgmv_df.rename(columns={'front_cate_one': '一级类目', 'front_cate_two': '二级类目',
                                            'avg_rating': '历史综合评分', 'sum_rating_num': '历史综合评分数量',
                                            'hei_num': '黑名单数量', 'bai_num': '白名单数量', 'hui_num': '灰名单数量',
                                            'baihui_num': '偏白灰名单数量', 'net_gmv': 'Net-Gmv', 'total': '总GMV',
                                            'refund_total': '退货金额'}, inplace=True)
print(data_hive_catetwo_netgmv_df.head(10))

# hive 三级类目 查看列名及列名处理
data_hive_catethree_netgmv_df.columns.values
data_hive_catethree_netgmv_df.rename(columns={'front_cate_one': '一级类目', 'front_cate_two': '二级类目',
                                              'front_cate_three': '三级类目', 'avg_rating': '历史综合评分',
                                              'sum_rating_num': '历史综合评分数量', 'hei_num': '黑名单数量',
                                              'bai_num': '白名单数量', 'hui_num': '灰名单数量',
                                              'baihui_num': '偏白灰名单数量', 'net_gmv': 'Net-Gmv', 'total': '总GMV',
                                              'refund_total': '退货金额'}, inplace=True)
print(data_hive_catethree_netgmv_df.head(10))

# ms数据库-一级类目曝光访客等基础数据
data_ms_cateone_pvuv = pd.read_sql(sql_mssql_cateone_rating, con_mssql)
print(data_ms_cateone_pvuv.head(10))

# ms数据库-二级类目曝光访客等基础数据
data_ms_catetwo_pvuv = pd.read_sql(sql_mssql_catetwo_rating, con_mssql)
print(data_ms_catetwo_pvuv.head(10))

# ms数据库-三级类目曝光访客等基础数据
data_ms_catethree_pvuv = pd.read_sql(sql_mssql_catethree_rating, con_mssql)
print(data_ms_catethree_pvuv.head(10))

# 一级类目数据合并
data_cateone_total = pd.merge(data_ms_cateone_pvuv, data_hive_cateone_netgmv_df,
                              on=['一级类目', 'seller_type'], how='inner')
print(data_cateone_total.head(10))

# 二级类目数据合并
data_catetwo_total = pd.merge(data_ms_catetwo_pvuv, data_hive_catetwo_netgmv_df,
                              on=['一级类目', '二级类目', 'seller_type'], how='inner')
print(data_catetwo_total.head(10))

# 三级类目数据合并
data_catethree_total = pd.merge(data_ms_catethree_pvuv, data_hive_catethree_netgmv_df,
                                on=['一级类目', '二级类目', '三级类目', 'seller_type'], how='inner')
print(data_catethree_total.head(10))

# 写入Excel操作
writer = pd.ExcelWriter('品类健康' + '.xlsx')
data_cateone_total.to_excel(writer, sheet_name='一级类目', index=False)
data_catetwo_total.to_excel(writer, sheet_name='二级类目', index=False)
data_catethree_total.to_excel(writer, sheet_name='三级类目', index=False)

os.chdir(r'/Users/apache/Downloads/A-python')
writer.save()
