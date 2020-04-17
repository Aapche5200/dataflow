from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("mssql+pymssql://sa:yssshushan2008@192.168.128.5:1433/CFflows?charset=utf8")

sql_pcapache = ('''
select 数据日期,货号,上架时间,状态,一级类目,二级类目,三级类目,前台一级类目,前台二级,前台三级,售价,商品等级,商品标签,
上货来源,商家名称,供应链标签,黑名单原因,曝光量,点击数,加购数,收藏数,访客数,点击率,收藏率,加购率,下单买家数,下单量,下单件数,
下单转化率,支付买家数,男买家数,女买家数,支付订单量,支付件数,支付金额,
历史评论数,历史评分
from CFgoodsday.dbo.商品数据
where 数据日期 between '2020-02-26' and '2020-02-29'
''')

data_pcapache = pd.read_sql(sql_pcapache, engine)
print(data_pcapache.head(10))


engine_shusheng = create_engine("mssql+pymssql://sa:yssshushan2008@172.16.92.2:1433/CFgoodsday?charset=utf8")
data_pcapache.to_sql('商品数据', con=engine_shusheng, if_exists='append', index=False)
