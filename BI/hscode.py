import pandas as pd
import psycopg2
import os
import time


def get_NANO_cms_conn():
    shp = sshProxy()
    ssh= shp.proxy("hangzhou", "rm-bp1kw60k05dv03xbujo.mysql.rds.aliyuncs.com",3306)
    try:
        conn = pymysql.connect(
            host="127.0.0.1",
            port=ssh.local_bind_port,
            user='readonly',
            passwd='Lt4H*R#17K8B',
            charset='utf8')
        conn.select_db('cms')
        return conn
    except pymysql.Error as e:
        print (e)
        return None


def get_data(conn):
    sql1 = '''


select outer_product_no,hscode,top_category_id,second_category_id,third_category_id,title,product_detail_url

from hs_code_task
where `status`=1



 '''





    data1 = pd.read_sql(sql1, conn)



    return data1


if __name__ == '__main__':
    con = get_NANO_cms_conn()
    data1 = get_data(con)

    writer = pd.ExcelWriter('Category_re' + time.strftime('%Y-%m-%d', time.localtime(time.time())) + '.xlsx')
    data1.to_excel(writer, sheet_name='1级icon', index=False)

    print(data1)

    # os.chdir("F:/Python")
    # writer.save()



