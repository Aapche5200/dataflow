import pandas as pd
import psycopg2
import os
import time


def get_redshift_test_conn():
    conn = psycopg2.connect(
        user='yinshushan',
        password='Yinshushan123',
        host='jiayundatatest.cls0csjdlwvj.us-west-2.redshift.amazonaws.com',
        port=5439,
        database='jiayundata')
    return conn


def get_data(conn):
    sql1 = '''
    
select 
                case when a.product_level=5 then '头部商品' 
                when a.product_level=4 then '腰部商品'
                when a.product_level=3 then '长尾商品'
                when a.product_level in (1,2,6) then '测试商品'
                else '其他' end product_level,
           front_cate_one,
           count(distinct case when product_active=1 then product_no else null end) as item_num
from public.product_info_history as a 
join dwd.product_info as b on b.item_no=a.product_no
where log_date = trunc(getdate()) -1
group by 1,2
order by 1,2

 '''

    data1 = pd.read_sql(sql1, conn)

    return data1


if __name__ == '__main__':
    con = get_redshift_test_conn()
    data1 = get_data(con)

    writer = pd.ExcelWriter('头部腰部在架商品' + time.strftime('%Y-%m-%d', time.localtime(time.time())) + '.xlsx')
    data1.to_excel(writer, sheet_name='头部腰部商品数量', index=False)

    # print(data1)

    os.chdir("F:/Python")
    writer.save()



