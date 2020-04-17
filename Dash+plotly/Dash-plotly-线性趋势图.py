import pymssql
import pandas as pd  # 数据处理例如：读入，插入需要用的包
import numpy as np  # 平均值中位数需要用的包
import os  # 设置路径需要用的包
import psycopg2
from datetime import datetime, timedelta  # 设置当前时间及时间间隔计算需要用的包

con_mssql = pymssql.connect("172.16.92.2", "sa", "yssshushan2008", "CFflows", charset="utf8")

sql_category = ('''
SELECT DISTINCT 日期 as log_date,业务类型 as laiyuan, 一级类目 as category_one ,支付金额 as gmv ,独立访客 as uv ,支付转化率 as cvr
from CFcategory.dbo.category_123_day 
WHERE (二级类目 = ' ' or 二级类目 is null) and ( 三级类目=' ' or 三级类目 is null)  and 日期 is not null
ORDER BY 日期                
''')

sql_product_level = ('''SELECT a.log_date,a.product_level,a.front_cate_one,b.日期 as Quarter_Four,a.item_num,b.商品数量 as mubiao_num,a.item_num/b.商品数量 as done_lv
from  CFcategory.dbo.商品等级数据_前台 as a
join CFcategory.dbo.[商品等级目标表] as b on a.product_level=b.商品等级 and a.front_cate_one=b.[前台一级类目]''')

sql_level_change = ('''select log_date,
case when 上货来源=5 then 'seller' else 'CF' end laiyuan,
case when count(distinct product_no)=0 then 0 else count(distinct case when product_level=3 then product_no else null end)::float/count(distinct product_no)::float end rate1_3,
case when count(distinct product_no)=0 then 0 else count(distinct case when product_level=-2 then product_no else null end)::float/count(distinct product_no)::float end rate2_2
from
(select  DISTINCT h.log_date,h.product_no,
        case when i.event_date is not null and i.product_no is not null then '广告商品' else '普通商品' end pno_type,
				h.product_level,
				g.product_level as status_1d,
				f.product_level as status_2d,
				e.product_level as status_3d,
				d.product_level as status_4d,
				c.product_level as status_5d,
				b.product_level as status_6d,
				a.product_level as status_7d,
				write_uid as 上货来源
from  (select DISTINCT log_date,product_no,product_level from public.product_info_history where log_date BETWEEN TRUNC(getdate())-180 and TRUNC(getdate())-1 ) as a 
left join (select DISTINCT log_date,product_no,product_level from public.product_info_history where log_date BETWEEN TRUNC(getdate())-180 and TRUNC(getdate())-1 ) as b on a.product_no=b.product_no and date(to_date(a.log_date,'YYYY MM DD')+ interval '1 D')=b.log_date
left join (select DISTINCT log_date,product_no,product_level from public.product_info_history where log_date BETWEEN TRUNC(getdate())-180 and TRUNC(getdate())-1 ) as c on a.product_no=c.product_no and date(to_date(a.log_date,'YYYY MM DD')+ interval '2 D')=c.log_date
left join (select DISTINCT log_date,product_no,product_level from public.product_info_history where log_date BETWEEN TRUNC(getdate())-180 and TRUNC(getdate())-1 ) as d on a.product_no=d.product_no and date(to_date(a.log_date,'YYYY MM DD')+ interval '3 D')=d.log_date
left join (select DISTINCT log_date,product_no,product_level from public.product_info_history where log_date BETWEEN TRUNC(getdate())-180 and TRUNC(getdate())-1 ) as e on a.product_no=e.product_no and date(to_date(a.log_date,'YYYY MM DD')+ interval '4 D')=e.log_date
left join (select DISTINCT log_date,product_no,product_level from public.product_info_history where log_date BETWEEN TRUNC(getdate())-180 and TRUNC(getdate())-1 ) as f on a.product_no=f.product_no and date(to_date(a.log_date,'YYYY MM DD')+ interval '5 D')=f.log_date
left join (select DISTINCT log_date,product_no,product_level from public.product_info_history where log_date BETWEEN TRUNC(getdate())-180 and TRUNC(getdate())-1 ) as g on a.product_no=g.product_no and date(to_date(a.log_date,'YYYY MM DD')+ interval '6 D')=g.log_date
left join (select DISTINCT log_date,product_no,product_level from public.product_info_history where log_date BETWEEN TRUNC(getdate())-180 and TRUNC(getdate())-1 ) as h on a.product_no=h.product_no and date(to_date(a.log_date,'YYYY MM DD')+ interval '7 D')=h.log_date
left join analysts.roi_pid_detail as i on i.event_date=a.log_date and i.product_no=a.product_no
left join jiayundw_dim.product_basic_info_df as  x on x.item_no=h.product_no
where h.log_date is not null)
where status_1d in (4,5) 
and status_2d in (4,5)
and status_3d in (4,5)
and status_4d in (4,5)
and status_5d in (4,5)
and status_6d in (4,5)
and status_7d in (4,5)
group by 1,2
ORDER BY 1''')

# 类目销售支付相关数据及处理
data_category = pd.read_sql(sql_category, con_mssql)
print(data_category.head(10))
# data_category['log_date'] = pd.to_datetime(data_category.log_date,infer_datetime_format=True)
data_category_cf = data_category[data_category.laiyuan.eq('cf')]
data_category_seller = data_category[data_category.laiyuan.eq('seller')]
data_category_total = data_category[data_category.laiyuan.eq('total')]

# 商品等级KPI数据及处理
data_product_level = pd.read_sql(sql_product_level, con_mssql)
data_product_level['mubiao_lv'] = 1.5 - data_product_level.done_lv
print(data_product_level.head(10))

# 画图加载包
import plotly.graph_objects as go
import plotly
import dash  # dash的核心后端
import dash_core_components as dcc  # 交互式组件
import dash_html_components as html  # 代码转html
from dash.dependencies import Input, Output  # 回调
from flask import Flask, render_template
import dash_daq as daq
from plotly.subplots import make_subplots

# 创建dash实例
# html.Title=("商品维度数据看板"),
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(external_stylesheets=external_stylesheets)
app.layout = html.Title(title=("test"))
app.layout = html.Div(children=[
    html.Div([html.H3("商品维度类目销售趋势", style={'textAlign': 'center'}),  # 类目销售趋势DIV
              dcc.Dropdown(id='my-dropdown-ca',  # 功能性组件， 设定id值作为标签关联callback函数中的标签
                           options=[{'label': 'Women\'s Shoes', 'value': 'Women\'s Shoes'},
                                    {'label': 'Women\'s Clothing', 'value': 'Women\'s Clothing'},
                                    {'label': 'Women\'s Bags', 'value': 'Women\'s Bags'},
                                    {'label': 'Watches', 'value': 'Watches'},
                                    {'label': 'Men\'s Shoes', 'value': 'Men\'s Shoes'},
                                    {'label': 'Men\'s Clothing', 'value': 'Men\'s Clothing'},
                                    {'label': 'Men\'s Bags', 'value': 'Men\'s Bags'},
                                    {'label': 'Jewelry & Accessories', 'value': 'Jewelry & Accessories'},
                                    {'label': 'Home Appliances', 'value': 'Home Appliances'},
                                    {'label': '全类目', 'value': '全类目'}],
                           multi=True,
                           value=['全类目'],
                           style={"display": "block", "margin-left": "auto", "margin-right": "auto", "width": "60%"}),
              dcc.Graph(id='my-graph-ca')],
             style={'text-align': 'center'}
             )
],
    className="container",  # 使用class 可以直接加入调用css功能
    style={'columnCount': 1})


# 对callback函数进行设置,与上面的对应,将数据return回对应id的Graph
@app.callback(Output('my-graph-ca', 'figure'),
              [Input('my-dropdown-ca', 'value'), ], )
def update_graph(selected_dropdown_value_ca):  # 类目销售趋势回调数据
    dropdown_ca = {"全类目": "全类目", "Women\'s Shoes": "Women\'s Shoes", "Women\'s Clothing": "Women\'s Clothing",
                   "Women\'s Bags": "Women\'s Bags", "Watches": "Watches", "Men\'s Shoes": "Men\'s Shoes",
                   "Men\'s Clothing": "Men\'s Clothing", "Men\'s Bags": "Men\'s Bags",
                   "Jewelry & Accessories": "Jewelry & Accessories", "Home Appliances": "Home Appliances", }
    trace1_ca = []
    trace2_ca = []
    trace3_ca = []
    for leimu in selected_dropdown_value_ca:
        # 设置total图例及数据
        trace1_ca.append(go.Scatter(x=data_category_total[data_category_total["category_one"] == leimu]["log_date"],
                                    y=data_category_total[data_category_total["category_one"] == leimu]["gmv"],
                                    mode='lines', opacity=0.7, name=f'Total {dropdown_ca[leimu]}',
                                    line=dict(width=3),
                                    textposition='bottom center'))
        # 设置cf图例及数据
        trace2_ca.append(go.Scatter(x=data_category_cf[data_category_cf["category_one"] == leimu]["log_date"],
                                    y=data_category_cf[data_category_cf["category_one"] == leimu]["gmv"],
                                    mode='lines', opacity=0.7, name=f'Cf {dropdown_ca[leimu]}',
                                    line=dict(width=3),
                                    textposition='bottom center'))
        # 设置seller图例及数据
        trace3_ca.append(go.Scatter(x=data_category_seller[data_category_seller["category_one"] == leimu]["log_date"],
                                    y=data_category_seller[data_category_seller["category_one"] == leimu]["gmv"],
                                    mode='lines', opacity=0.7, name=f'Seller {dropdown_ca[leimu]}',
                                    line=dict(width=3),
                                    textposition='bottom center'))
    # 画等级变化趋势
    traces_ca = [trace1_ca, trace2_ca, trace3_ca]
    data_ca = [val_ca for sublist in traces_ca for val_ca in sublist]
    figure_ca = {'data': data_ca,
                 'layout': go.Layout(colorway=['rgb(235, 12, 25)', 'rgb(234, 143, 116)', 'rgb(122, 37, 15)',
                                               'rgb(0, 81, 108)', 'rgb(93,145,167)', 'rgb(0,164,220)',
                                               'rgb(107,207,246)', 'rgb(0,137,130)', 'rgb(109,187,191)',
                                               'rgb(205,221,230)', 'rgb(184,207,220)', '#C49C94', '#E377C2', '#F7B6D2',
                                               '#7F7F7F', '#C7C7C7', '#BCBD22', '#BCBD22', '#DBDB8D', '#17BECF',
                                               '#9EDAE5', '#729ECE', '#FF9E4A', '#67BF5C', '#ED665D', '#AD8BC9',
                                               '#A8786E', '#ED97CA', '#A2A2A2', '#CDCC5D'],
                                     height=600,
                                     legend=dict(font=dict(family='Arial', size=11)),
                                     title=dict(
                                         text=f"GMV for {', '.join(str(dropdown_ca[i]) for i in selected_dropdown_value_ca)}",
                                         font=dict(family='Arial', size=16)),
                                     xaxis={'title': {'text': '日期 (可筛选)', 'font': {'family': 'Arial', 'size': 11}},
                                            'tickfont': {'family': 'Arial', 'size': 11},
                                            'rangeselector': {'buttons': list([{'count': 1, 'label': '1M',
                                                                                'step': 'month', 'stepmode': 'backward'
                                                                                },
                                                                               {'count': 6, 'label': '6M',
                                                                                'step': 'month', 'stepmode': 'backward'
                                                                                },
                                                                               {'step': 'all'}])},
                                            'rangeslider': {'visible': True}, 'type': 'date'},
                                     yaxis={'title': {'text': '支付金额 (单位美元)',  # X轴类型为日期，否则报错
                                                      'font': {'family': 'Arial', 'size': 11}},
                                            'tickfont': {'family': 'Arial', 'size': 11}})}
    return figure_ca


# 主函数运行服务器操作
if __name__ == '__main__':
    app.server.run()
# '192.168.129.46',8050
