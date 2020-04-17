import pandas as pd
import psycopg2
import os
import time
import pymssql

def get_mssql_test_conn():
    conn = pymssql.connect(
        user='sa',
        password='yssshushan2008',
        host='172.16.92.6',
        port=1433,
        database='CFflows',
        charset='utf8')
    # return conn


def get_data(conn):
    sql1 = '''

        (select 日期 as 数据日期,货号,上架时间,在架状态 as 状态,后台一级类目 as 一级类目,后台二级类目 as 二级类目,后台三级类目 as 三级类目,前台一级类目,前台二级类目 as 前台二级,前台三级类目 as 前台三级,系统定价 as 售价,商品等级,商品标签,
            上货来源,商家名称,供应链标签,黑名单原因,曝光 as 曝光量,点击 as 点击数,加购 as 加购数,收藏 as 收藏数,商详访客 as 访客数,点击率,收藏率,加购率,下单买家数,下单量,下单件数,下单转化率,支付买家数,男买家数,女买家数,支付订单量,支付件数,支付金额,
            支付转化率,近7天支付件数 as 近7天销量,'null' as 退货件数,'null' as 退货金额,历史评分数量 as 历史评论数,历史评分,30日复购率,'null' as mazon_in最低价,'null' as flipkart最低价,'null' as snapdeal最低价,'null' as 最大价差,'null' as 上周评分,
                'null' as 上周评论数,'null' as 缺货率,'null' as name,'null' as 操作状态,'null' as 商品名称,折扣,'null' as 前台三级类目	,'null' as 前台二级类目,'null' as 日期
                    from  OPENROWSET('Microsoft.Ace.OLEDB.12.0','Excel 12.0;
                    DATABASE=\\vmware-host\Shared Folders\下载\导入模板\2019-11-17商品日报.xlsx' , 'Select * from [Sheet1$]'))
        UNION all 
        (select 日期 as 数据日期,货号,上架时间,在架状态 as 状态,后台一级类目 as 一级类目,后台二级类目 as 二级类目,后台三级类目 as 三级类目,前台一级类目,前台二级类目 as 前台二级,前台三级类目 as 前台三级,系统定价 as 售价,商品等级,商品标签,
            上货来源,商家名称,供应链标签,黑名单原因,曝光 as 曝光量,点击 as 点击数,加购 as 加购数,收藏 as 收藏数,商详访客 as 访客数,点击率,收藏率,加购率,下单买家数,下单量,下单件数,下单转化率,支付买家数,男买家数,女买家数,支付订单量,支付件数,支付金额,
            支付转化率,近7天支付件数 as 近7天销量,'null' as 退货件数,'null' as 退货金额,历史评分数量 as 历史评论数,历史评分,30日复购率,'null' as mazon_in最低价,'null' as flipkart最低价,'null' as snapdeal最低价,'null' as 最大价差,'null' as 上周评分,
                'null' as 上周评论数,'null' as 缺货率,'null' as name,'null' as 操作状态,'null' as 商品名称,折扣,'null' as 前台三级类目	,'null' as 前台二级类目,'null' as 日期
                    from  OPENROWSET('Microsoft.Ace.OLEDB.12.0','Excel 12.0;
                    DATABASE=\\vmware-host\Shared Folders\下载\导入模板\2019-11-16商品日报.xlsx' , 'Select * from [Sheet1$]'))
        UNION all
        (select 日期 as 数据日期,货号,上架时间,在架状态 as 状态,后台一级类目 as 一级类目,后台二级类目 as 二级类目,后台三级类目 as 三级类目,前台一级类目,前台二级类目 as 前台二级,前台三级类目 as 前台三级,系统定价 as 售价,商品等级,商品标签,
            上货来源,商家名称,供应链标签,黑名单原因,曝光 as 曝光量,点击 as 点击数,加购 as 加购数,收藏 as 收藏数,商详访客 as 访客数,点击率,收藏率,加购率,下单买家数,下单量,下单件数,下单转化率,支付买家数,男买家数,女买家数,支付订单量,支付件数,支付金额,
            支付转化率,近7天支付件数 as 近7天销量,'null' as 退货件数,'null' as 退货金额,历史评分数量 as 历史评论数,历史评分,30日复购率,'null' as mazon_in最低价,'null' as flipkart最低价,'null' as snapdeal最低价,'null' as 最大价差,'null' as 上周评分,
                'null' as 上周评论数,'null' as 缺货率,'null' as name,'null' as 操作状态,'null' as 商品名称,折扣,'null' as 前台三级类目	,'null' as 前台二级类目,'null' as 日期
                    from  OPENROWSET('Microsoft.Ace.OLEDB.12.0','Excel 12.0;
                    DATABASE=\\vmware-host\Shared Folders\下载\导入模板\2019-11-15商品日报.xlsx' , 'Select * from [Sheet1$]'))

 '''

    data1 = pd.read_sql(sql1, conn)

    return data1


if __name__ == '__main__':
    con = get_mssql_test_conn()
    data1 = get_data(con)
    print(data1)

# ————————————写入Excel 操作——————————————————
    # writer = pd.ExcelWriter('类目推广' + time.strftime('%Y-%m-%d', time.localtime(time.time())) + '.xlsx')
    # data1.to_excel(writer, sheet_name='FB数据', index=False)

    # os.chdir(r'/Users/apache/Downloads/A-python')
    # writer.save()



