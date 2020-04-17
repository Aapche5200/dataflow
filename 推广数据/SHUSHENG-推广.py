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
    return conn


def get_data(conn):
    sql1 = '''

        SELECT a.日期,a.商品等级,a.前台一级类目,b.日期 as 季度,a.商品数量,a.Q4目标值 ,b.商品扶持,b.商品扶持当前数量,b.Q4目标扶持数量
            from (
                    SELECT a.日期,a.商品等级,a.前台一级类目,b.日期 as 季度,a.商品数量,b.商品数量 as Q4目标值
                        from  CFcategory.dbo.商品等级数据_前台 as a
                        join CFcategory.dbo.[商品等级目标表] as b on a.商品等级=b.商品等级 and a.[前台一级类目]=b.[前台一级类目]
                            ) as a
                    left JOIN 
                    (SELECT a.日期 ,a.商品扶持,a.前台一级类目 ,a.商品数量 as 商品扶持当前数量, b.商品数量 as Q4目标扶持数量 
                        from CFcategory.dbo.商品扶持当前数据 as a
                        join CFcategory.dbo.商品扶持目标表 as b on  a.前台一级类目=b.前台一级类目) as b  on a.日期=b.日期 and a.前台一级类目=b.前台一级类目



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



