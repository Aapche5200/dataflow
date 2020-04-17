import pymssql
import pandas as pd  # 数据处理例如：读入，插入需要用的包
import numpy as np  # 平均值中位数需要用的包
import os  # 设置路径需要用的包
import psycopg2
from datetime import datetime, timedelta  # 设置当前时间及时间间隔计算需要用的包

con_mssql = pymssql.connect("172.16.92.2", "sa", "yssshushan2008", "CFflows", charset="utf8")
con_redshift = psycopg2.connect(user='operation', password='Operation123',
                                host='jiayundatapro.cls0csjdlwvj.us-west-2.redshift.amazonaws.com',
                                port=5439, database='jiayundata')

sql_category = ('''
SELECT DISTINCT 日期 as log_date,业务类型 as laiyuan, 一级类目 as category_one ,支付金额 as gmv ,独立访客 as uv ,支付转化率 as cvr
from CFcategory.dbo.category_123_day 
WHERE 二级类目 is null and  三级类目 is null  and 日期 is not null
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
# 商品等级变化数据及处理
data_level_change = pd.read_sql(sql_level_change, con_redshift)
print(data_level_change.head(10))
data_level_change.rate1_3 = data_level_change.rate1_3.apply(lambda x: format(x, '.2%'))
data_level_change.rate2_2 = data_level_change.rate2_2.apply(lambda x: format(x, '.2%'))
data_level_change_cf = data_level_change[data_level_change.laiyuan.eq('CF')]
data_level_change_seller = data_level_change[data_level_change.laiyuan.eq('seller')]

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

# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash()

# app.index_string = '''
# <!DOCTYPE html>
# <html>
#     <head>
#         <title>test</title>
#     </head>
# </html>
# '''

app.layout = html.Div([
    html.Div([
        dcc.DatePickerRange(
            id='date-picker-range-level-kpi',
            start_date=(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
            end_date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
            start_date_placeholder_text="Start Date",
            end_date_placeholder_text="End Date",
            calendar_orientation='vertical'
        )],
        className='four columns',
        style={'flex': ['0', '0', '20%'], 'max-width': '20%', 'border': '3px', 'border-radius': '5px', 'height': '90px',
               'margin': '5px', 'margin-left': '80px'}),
    html.Div([
        dcc.Graph(id='my-graph-kpi-toubu', style={"height": "80%", "width": "80%"})],  # 设置画布长宽
        className='four columns',
        style={'flex': ['0', '0', '40%'], 'max-width': '40%', 'border': '3px', 'border-radius': '5px',
               'height': '550px',
               'margin': '5px', 'text-align': 'center'}),
    html.Div([
        dcc.Graph(id='my-graph-kpi-yaobu', style={"height": "80%", "width": "80%"})],
        className='four columns',
        style={'flex': ['0', '0', '40%'], 'max-width': '40%', 'border': '3px', 'border-radius': '5px',
               'height': '550px',
               'margin': '5px', 'text-align': 'center'})],
    style={'margin': 'auto', 'display': 'flex', 'flex-wrap': 'wrap',
           'margin-right': '15px', 'margin-left': '15px'})


@app.callback([Output('my-graph-kpi-toubu', 'figure'), Output('my-graph-kpi-yaobu', 'figure')],
              [Input('date-picker-range-level-kpi', 'start_date'), Input('date-picker-range-level-kpi', 'end_date')], )
def update_level_kpi_toubu(start_date, end_date):
    # 画图进图条图 数据处理
    data_product_level.log_date = pd.to_datetime(data_product_level.log_date, format='%Y-%m-%d')
    filter_data_product_level_total = data_product_level[(data_product_level.log_date >= start_date) &
                                                         (data_product_level.log_date <= end_date)]
    # 求平局值-分组求平均as_index=False否则格式为索引格式
    data_product_level_total = filter_data_product_level_total.groupby([
        'product_level', 'front_cate_one'], as_index=False).mean()
    data_product_level_total.rename(columns={'item_num': 'item_num_mean', 'mubiao_num': 'mubiao_num_mean',
                                             'done_lv': 'done_lv_mean', 'mubiao_lv': 'mubiao_lv_mean'}, inplace=True)
    print(data_product_level_total.head(20))
    # 排序 使得做出来的图降序排序
    filter_data_product_level_total_toubu = \
        data_product_level_total[data_product_level_total.product_level.eq('头部商品')]. \
            sort_values(by='item_num_mean').tail(15)
    print(filter_data_product_level_total_toubu.head(20))

    filter_data_product_level_total_yaobu = \
        data_product_level_total[data_product_level_total.product_level.eq('腰部商品')]. \
            sort_values(by='item_num_mean').tail(15)
    print(filter_data_product_level_total_yaobu.head(20))

    # 头部进度条图
    fig_kpi_toubu = go.Figure(go.Bar(y=filter_data_product_level_total_toubu.front_cate_one,  # 横向条形图Y为类目值
                                     x=filter_data_product_level_total_toubu.done_lv_mean,  # X为数值
                                     width=0.4,  # 设置条形图宽度
                                     name="当前完成情况",  # 设置图例标题
                                     orientation='h',  # 设置条形图横向
                                     marker=dict(
                                         color='rgb(0, 81, 108)',  # 设置条形图填充色
                                         line=dict(color='rgb(0, 81, 108)', width=1)  # 设置条形图边框颜色及宽度
                                     ),  # text是在图形内设置标签
                                     # text=np.round(filter_data_product_level_total_toubu.done_lv_mean, decimals=2),
                                     textposition='auto'
                                     ))

    fig_kpi_toubu.add_trace(go.Bar(y=filter_data_product_level_total_toubu.front_cate_one,
                                   x=filter_data_product_level_total_toubu.mubiao_lv_mean,
                                   width=0.35,
                                   name="目标进度",
                                   orientation='h',
                                   marker=dict(
                                       color='rgb(235, 12, 25)',
                                       line=dict(color='rgb(235, 12, 25)', width=1)
                                   )
                                   ))

    # annotations是添加文本注释的意思，也可以用来添加标签
    annotations_done_lv = [dict(x=xi + 0.1, y=yi, text=str(int(np.round(xi, decimals=2) * 100)) + '%', xanchor='auto',
                                yanchor='middle',
                                font=dict(family='Arial', size=10, color='rgb(255, 255, 255)'),
                                showarrow=False) for xi,
                                                     yi in zip(filter_data_product_level_total_toubu.done_lv_mean,
                                                               filter_data_product_level_total_toubu.front_cate_one)
                           ]

    annotations_item_num = [dict(x=-0.1, y=yi, text=str(int(np.round(zi, decimals=0))), xanchor='auto',
                                 yanchor='middle',
                                 font=dict(family='Arial', size=10, color='rgb(0, 81, 108)'),
                                 showarrow=False) for xi, yi,
                                                      zi in zip(filter_data_product_level_total_toubu.done_lv_mean,
                                                                filter_data_product_level_total_toubu.front_cate_one,
                                                                filter_data_product_level_total_toubu.item_num_mean)
                            ]
    # 头部布局
    fig_kpi_toubu.update_layout(title=dict(text="头部等级KPI", font=dict(family='Arial', size=15), xanchor='left',
                                           yanchor='top', xref='container', yref='container', x=0.45, y=0.98),
                                barmode='stack',
                                annotations=annotations_item_num + annotations_done_lv,
                                paper_bgcolor='rgb(255,255,255)',  # 设置绘图区背景色
                                plot_bgcolor='rgb(255,255,255)',  # 设置画布背景颜色
                                xaxis=dict({'categoryorder': 'total descending'}, side='top',  # 设置X轴刻度值置顶
                                           showgrid=False, showline=False, showticklabels=False),
                                yaxis=dict(tickangle=0, tickfont=dict(family='Arial', size=11)),  # 设置轴标签旋转角度
                                showlegend=False,  # 设置是否显示图例
                                margin=dict(l=5, r=5, t=35, b=5))

    # 添加设置腰部数据标签
    annotations_done_lv_yao = [
        dict(x=xi + 0.3, y=yi, text=str(int(np.round(xi, decimals=2) * 100)) + '%', xanchor='auto',
             yanchor='middle',
             font=dict(family='Arial', size=10, color='rgb(255, 255, 255)'),
             showarrow=False) for xi,
                                  yi in zip(filter_data_product_level_total_yaobu.done_lv_mean,
                                            filter_data_product_level_total_yaobu.front_cate_one)
        ]
    # xanchor 设置相对x轴位置对其方式
    annotations_item_num_yao = [dict(x=-0.1, y=yi, text=str(int(np.round(zi, decimals=0))), xanchor='auto',
                                     yanchor='middle',
                                     font=dict(family='Arial', size=10, color='rgb(0, 81, 108)'),
                                     showarrow=False) for xi,
                                                          yi,
                                                          zi in zip(filter_data_product_level_total_yaobu.done_lv_mean,
                                                                    filter_data_product_level_total_yaobu.front_cate_one,
                                                                    filter_data_product_level_total_yaobu.item_num_mean
                                                                    )
                                ]

    # 腰部进度条
    fig_kpi_yaobu = go.Figure(go.Bar(y=filter_data_product_level_total_yaobu.front_cate_one,
                                     x=filter_data_product_level_total_yaobu.done_lv_mean,
                                     width=0.4,
                                     name="当前完成情况",
                                     orientation='h',
                                     marker=dict(
                                         color='rgb(0, 81, 108)',
                                         line=dict(color='rgb(0, 81, 108)', width=1)
                                     )
                                     ))

    fig_kpi_yaobu.add_trace(go.Bar(y=filter_data_product_level_total_yaobu.front_cate_one,
                                   x=filter_data_product_level_total_yaobu.mubiao_lv_mean,
                                   width=0.35,
                                   name="目标进度",
                                   orientation='h',
                                   marker=dict(
                                       color='rgb(235, 12, 25)',
                                       line=dict(color='rgb(235, 12, 25)', width=1)
                                   )
                                   ))
    # 腰部布局
    fig_kpi_yaobu.update_layout(title=dict(text="腰部等级KPI", font=dict(family='Arial', size=15), xanchor='left',
                                           yanchor='top', xref='container', yref='container', x=0.45, y=0.98),
                                barmode='stack',
                                annotations=annotations_item_num_yao + annotations_done_lv_yao,
                                paper_bgcolor='rgb(255,255,255)',
                                plot_bgcolor='rgb(255,255,255)',
                                xaxis=dict({'categoryorder': 'total descending'}, side='top', showgrid=False,
                                           showline=False, showticklabels=False),
                                yaxis=dict(tickangle=0, tickfont=dict(family='Arial', size=11)),
                                showlegend=False,
                                margin=dict(l=5, r=5, t=35, b=5))

    return fig_kpi_toubu, fig_kpi_yaobu


if __name__ == '__main__':
    app.server.run()
