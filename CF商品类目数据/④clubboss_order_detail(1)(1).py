
'''
@author: kongxw
@contact: happykxw@163.com
@file: clubboss_order_detail.py
@time: 2019/3/15 14:08
@desc:
'''

# 本地一
import pandas as pd
import datetime
import db_help
import os

# 获取昨天日期的字符串格式的函数
def getYesterday():
    # 获取今天的日期
    today = datetime.date.today()
    # 获取一天的日期格式数据
    oneday = datetime.timedelta(days=1)
    # 获取昨天的格式化字符串
    yesterday = today - oneday
    yesterdaystr = yesterday.strftime('%y-%m-%d')

    print('yesterdaystr: ' + yesterdaystr)

    return yesterdaystr

# 获取数据库连接-本地二
def get_test_con():
    # 本地二
    con = db_help.get_redshift_bireport_conn()
    print(con)
    con_hiboss = db_help.get_boss_conn()
    print(con_hiboss)

    return con,con_hiboss

def get_basic_user_01(con_hiboss,start_day,end_day):
    sql1 = '''
            SELECT t2.inviter_uid,t.uid,t2.level,t.order_name 
              FROM cluboss.orders t
        inner join cluboss.users t2
                on t.uid = t2.id
             WHERE t.uid not in ('0','131000694','131012777','131059349')
               and DATE(t.created_at+ interval 8 hour) >= '{start_day}'
               and DATE(t.created_at+ interval 8 hour) <= '{end_day}'
               and t.payment_date is not null
               -- and t.order_name in ('SO96057302','SO96106880')
          GROUP BY 1,2,3,4
    '''.format(start_day=start_day,end_day=end_day)

    data_basic_01 = pd.read_sql(sql1,con_hiboss)
    data_order_ist = data_basic_01['order_name'].drop_duplicates()

    return data_basic_01,data_order_ist

def get_basic_user_order_detail_03(con,data_order_ist):
    sql3 = '''
         SELECT  t2.user_id,t.order_name,
                 t.create_at + interval '8 hour' as create_at,
                 t.order_at + interval '8 hour' as order_at,
                 t2.channel,t2.currency,t2.rate,t2.state,t2.shipping_country,
                 t2.shipping_phone,t2.shipping_state,t2.shipping_city,t2.shipping_street2,t2.shipping_email,
                 t.price_real * t.origin_qty sku_origin_amount, t.price_real * t.product_qty sku_send_amount,
                 t2.amount_total,t2.delivery_way,t2.tracking_no,
                 date(send_time) send_time ,
                 t2.status,t.item_no,t.sku_id,t.product_name,
                 t.origin_qty,t.product_qty, t.price_origin,t.price_unit,t2.warehouse,t.category_one_en,
                 t.category_two_en,t.category_three_en
           from  jiayundw_dm.sale_order_line_df t 
     inner join jiayundw_dm.sale_order_info_df t2
             on  t.order_name = t2.order_name 
          where  t.order_name in ({order_name})   
            and  t.sku_id not in ('717664','3701368')
    '''.format(order_name=",".join("'" + str(b) + "'" for b in data_order_ist))

    data_order_detail = pd.read_sql(sql3,con)

    return data_order_detail

def main():
    yesterdaystr = getYesterday()

    start_day = '2019-03-01'
    end_day = (datetime.datetime.now() + datetime.timedelta(-1)).strftime("%Y-%m-%d")

    print('1、获取数据库链接')
    con, con_hiboss = get_test_con()
    print('2、获取查订单基本信息')
    data_basic_01, data_order_ist = get_basic_user_01(con_hiboss, start_day, end_day)
    print('3、获取订单详情')
    data_order_detail = get_basic_user_order_detail_03(con,data_order_ist)
    print('4、合并')
    data_all = pd.merge(data_basic_01,data_order_detail,left_on='order_name', right_on='order_name', how='left')
    del data_all['user_id']

    data_all.columns = ['分享用户ID','用户ID','等级','订单号','支付时间','下单时间','支付方式','货币','汇率','支付状态','收件国家',
                        '收件人手机','州','城市','街道','邮件','sku下单金额','sku发货金额','订单金额','物流公司','运单号','发货时间',
                        '物流状态','货号','sku_ID','商品名称','下单数量','发货数量','price_origin','price_real','发货仓','一级类目','二级类目',
                        '三级类目']
    print('5、输出')
    os.chdir(r'E:\下载\取数文件\取数结果')
    writer = pd.ExcelWriter('Hiboss_订单详情_' + yesterdaystr + '.xlsx')
    data_all.to_excel(writer, sheet_name='订单详情', index=False)
    writer.save()

    print('6、all finish')

if __name__ == '__main__':
    main()    