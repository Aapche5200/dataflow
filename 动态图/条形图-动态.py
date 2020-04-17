# ----导入包----
import numpy as np
import os
import time
import pymssql
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.animation as animation
from IPython.display import HTML
# 建立数据库连接
conn = pymssql.connect("172.16.92.6", "sa", "yssshushan2008", "CFflows", charset="utf8")
print("连接成功")
# 读取数据库表数据
data = pd.read_sql("SELECT datepart(year,日期)*100+datepart(month,日期) as yearmonth,一级类目 as category_one,sum(支付金额) as gmv,"
                   "AVG(在售商品数) as goods_num,(sum(支付金额)/sum(买家数)) as pay_price from CFcategory.dbo.category_123_day "
                   "WHERE 业务类型='total' and (二级类目 = ' ' or 二级类目 is null ) and 一级类目 !='全类目'"
                   "GROUP BY datepart(year,日期)*100+datepart(month,日期) ,一级类目 "
                   "HAVING sum(支付金额)  >=150000 "
                   "ORDER BY datepart(year,日期)*100+datepart(month,日期) ,一级类目", con=conn)
print(data.head(10))
# 映射函数方式来构造字典分配颜色
colors = dict(zip(['is_null', 'other', 'Office & Books', 'Automobiles', 'Home Appliances',
                   'Men\'s Bags', 'Sports & Fitness', 'Women\'s Bags', 'Beauty & Health','Kids',
                   'Women\'s Shoes', 'Home', 'Men\'s Clothing', 'Women\'s Clothing', 'Men\'s Shoes',
                   'Mobiles & Accessories', 'Jewelry & Accessories', 'Watches', 'Electronics'],
                  ['#DDDDFF', '#7D7DFF', '#0000C6', '#000079', '#CEFFCE', '#28FF28', '#007500',
                   '#FFFF93', '#8C8C00', '#FFB5B5', '#FF0000', '#CE0000', '#750000', '#ffb3ff',
                   '#90d595', '#e48381', '#aafbff', '#f7bb5f', '#eafb50']))
fig, ax = plt.subplots(figsize=(15, 8))


def draw_barchart(month):
    dff = data[data['yearmonth'].eq(month)].sort_values(by='gmv', ascending=True).tail(10)
    ax.clear()
    ax.barh(dff['category_one'], dff['gmv'], color=[colors[x] for x in dff['category_one']])
    dx = dff['gmv'].max() / 200
    # for循环设置标签显示
    for i, (value, name) in enumerate(zip(dff['gmv'], dff['category_one'])):
        ax.text(value-dx, i, name, size=9, ha='right', va='center')  # 设置标签category显示 weight=60设置粗细
        ax.text(value+dx, i, f'{value:,.0f}', size=9, ha='left', va='center')  # 设置GMV显示
    ax.text(1, 0.4, month, transform=ax.transAxes, color='#777777', size=46, ha='right', weight=800)  # 设置显示日期变化位置及颜色等
    ax.text(0, 1.06, 'SUM (GMV)/month=(price/day)*(sales/day)', transform=ax.transAxes, size=12, color='#777777')  # 设置副标题
    ax.xaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))
    ax.xaxis.set_ticks_position('top')  # x 轴移动到顶端
    ax.tick_params(axis='x', color='#777777', labelsize=12)
    ax.set_yticks([])
    ax.margins(0, 0.01)
    ax.grid(which='major', axis='x', linestyle='-')  # 设置X轴网格线
    ax.set_axisbelow(True)
    ax.text(0, 1.1, 'Changes in monthly sales of various items in 2019',
            transform=ax.transAxes, size=24, weight=600, ha='left')  # 设置标题
    plt.box(False)


import matplotlib.animation as animation
from IPython.display import HTML
fig, ax = plt.subplots(figsize=(15, 8))
animator = animation.FuncAnimation(fig, draw_barchart, frames=range(201901, 201913), interval=1000, save_count=50)
os.chdir(r'/Users/apache/Downloads/导入模板')
#animator.save('test.gif', writer='imagemagick', fps=0.5, dpi=80)
#HTML(animator.to_jshtml())
plt.show()

# 对于保存动画的持续时间将是frames * (1/fps)（以秒计）
# 对于显示动画的持续时间将是frames * interval/1000（以秒为）
# transform=axs.transAxes就是轴坐标，大概意思就是左边距离横坐标轴长的1倍，下面距离纵坐标轴的0.4倍，如果不写的话默认就是data坐标<br>，即1代表横轴的1个单位，即坐标点
