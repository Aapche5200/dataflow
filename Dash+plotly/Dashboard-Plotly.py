import pymssql
import pandas as pd  # 数据处理例如：读入，插入需要用的包
import numpy as np  # 平均值中位数需要用的包
import os  # 设置路径需要用的包
import psycopg2
from datetime import datetime, timedelta  # 设置当前时间及时间间隔计算需要用的包
# 画图加载包
import plotly.graph_objects as go
import plotly
import dash  # dash的核心后端
import dash_core_components as dcc  # 交互式组件
import dash_html_components as html  # 代码转html
from dash.dependencies import Input, Output  # 回调
from flask import Flask, render_template
import dash_daq as daq
import dash_auth
from plotly.subplots import make_subplots  # 画子图加载包

con_mssql = pymssql.connect("172.16.92.2", "sa", "yssshushan2008", "CFflows", charset="utf8")
con_redshift = psycopg2.connect(user='operation', password='Operation123',
                                host='jiayundatapro.cls0csjdlwvj.us-west-2.redshift.amazonaws.com',
                                port=5439, database='jiayundata')

sql_category = ('''
SELECT DISTINCT 日期 as log_date,业务类型 as laiyuan, 一级类目 as category_one ,支付金额 as gmv ,独立访客 as uv ,支付转化率 as cvr
from CFcategory.dbo.category_123_day 
WHERE (二级类目 = ' ' or 二级类目 is null) and ( 三级类目=' ' or 三级类目 is null)  and 日期 is not null
ORDER BY 日期                
''')

sql_product_level = ('''SELECT a.log_date,a.product_level,a.front_cate_one,b.日期 as Quarter_Four,a.item_num,
b.商品数量 as mubiao_num,a.item_num/b.商品数量 as done_lv
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

# 业务线数据-做占比图
sql_yewuxian = ('''SELECT 日期 as log_date,业务类型 as yewuxian,一级类目 as front_cate_one,sum(支付金额) as gmv ,SUM(独立访客) as uv
from CFcategory.dbo.category_123_day
WHERE 一级类目 ='全类目' and (二级类目 = ' ' or 二级类目 is null) and ( 三级类目=' ' or 三级类目 is null)
and 日期 >= '2019-07-01'
GROUP BY 日期,业务类型,一级类目
ORDER BY 日期,业务类型,一级类目''')

# 各个类目占比-取数
sql_cate_zhanbi = ('''
SELECT 日期 as log_date,一级类目 as front_cate_one,SUM(独立访客) as uv,sum(支付金额) as gmv,SUM(在售商品数) as item_num
from CFcategory.dbo.category_123_day
WHERE 业务类型='total'  and (三级类目 = ' ' or 三级类目 is null) and (二级类目 = ' ' or 二级类目 is null) and 日期  >= '2019-01-01'
GROUP BY 日期 ,一级类目
ORDER BY 日期 ,一级类目
''')

# 类目数据占比数据处理
data_cate_zhanbi = pd.read_sql(sql_cate_zhanbi, con_mssql)
print(data_cate_zhanbi.head(10))

# 业务线数据处理
data_yewuxian = pd.read_sql(sql_yewuxian, con_mssql)
print(data_yewuxian.head(10))

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

# 以下为画图部分
VALID_USERNAME_PASSWORD_PAIRS = {
    '1': '1'
}

app = dash.Dash()

auth = dash_auth.BasicAuth(
    app,
    VALID_USERNAME_PASSWORD_PAIRS

)

app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>CF Category DashBoard</title>
        {%favicon%}
        {%css%}
    </head>
    <body>
        <div></div>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
        <div></div>
    </body>
</html>
'''

app.layout = \
    html.Div([
        html.Div([
            html.Div([
                dcc.DatePickerRange(
                    id='date-picker-range-level-kpi',
                    start_date=(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
                    end_date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                    start_date_placeholder_text="Start Date",
                    end_date_placeholder_text="End Date",
                    calendar_orientation='vertical',
                    style={"height": "100%", "width": "100%"}
                )
            ],
                style={'flex': ['0', '0', '10%'], 'max-width': '10%', 'height': '300px', 'margin': '5px',
                       'margin-left': '10px'
                       }
            ),
            html.Div([
                dcc.Graph(
                    id='my-graph-kpi-toubu',
                    config={"displayModeBar": False},
                    style={"height": "90%", "width": "80%"
                           }
                )],
                style={'flex': ['0', '0', '45%'], 'max-width': '45%', 'height': '550px', 'margin': 'auto'
                       }
            ),
            html.Div([
                dcc.Graph(
                    id='my-graph-kpi-yaobu',
                    config={"displayModeBar": False},
                    style={"height": "90%", "width": "80%"
                           }
                )],
                style={'flex': ['0', '0', '45%'], 'max-width': '45%', 'height': '550px', 'margin': 'auto'
                       }
            )
        ],
            style={'display': 'flex', 'flex-wrap': 'wrap', 'margin': 'auto', 'margin-right': '10px', 'float': 'none',
                   'margin-left': '10px'
                   }
        ),
        html.Div([
            html.Div([" "],
                     style={'flex': ['0', '0', '10%'], 'max-width': '10%', 'height': '90px', 'margin': '5px',
                            'margin-left': '10px'
                            }
                     ),
            html.Div([
                dcc.Graph(
                    id='my-graph-yewuxian',
                    config={"displayModeBar": False},
                    style={"height": "90%", "width": "80%"
                           }
                )],
                style={'flex': ['0', '0', '45%'], 'max-width': '45%', 'height': '550px', 'margin': 'auto',
                       'margin-left': '180px'
                       }
            ),
            html.Div([
                dcc.Graph(
                    id='my-graph-catezhanbi',
                    config={"displayModeBar": False},
                    style={"height": "90%", "width": "80%"
                           }
                )],
                style={'flex': ['0', '0', '45%'], 'max-width': '45%', 'height': '550px', 'margin': 'auto'
                       }
            )
        ],
            style={'display': 'flex', 'flex-wrap': 'wrap', 'margin': 'auto', 'margin-right': '10px', 'float': 'none',
                   'margin-left': '10px'
                   }
        ),
        html.Div([
            html.Div([
                html.Label("商品维度类目销售趋势",
                           style={'textAlign': 'left', 'width': '100%', 'position': 'relative',
                                  'font': {'family': 'Microsoft YaHei', 'size': 15}}
                           ),
                dcc.Dropdown(
                    id='my-dropdown-ca',
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
                    style={"height": "100%", "width": "150px"}
                )
            ],
                style={'flex': ['0', '0', '20%'], 'max-width': '20%', 'height': '90px', 'margin': '5px',
                       'margin-left': '10px'
                       }
            ),
            html.Div([
                dcc.Graph(
                    id='my-graph-ca',
                    config={"displayModeBar": False},
                    style={"height": "90%", "width": "100%"}
                )],
                style={'flex': ['0', '0', '80%'], 'max-width': '80%', 'height': '550px', 'margin': 'auto',
                       'margin-right': '120px'
                       }
            )
        ],
            style={'display': 'flex', 'flex-wrap': 'wrap', 'margin': 'auto', 'float': 'none',
                   'margin-left': '10px'
                   }
        ),
        html.Div([
            html.Div([
                html.Label("商品等级流转趋势",
                           style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                  'font': {'family': 'Microsoft YaHei', 'size': 15}}),
                dcc.Dropdown(
                    id='my-dropdown-level-change',
                    options=[{'label': 'Cf', 'value': 'CF'},
                             {'label': 'Seller', 'value': 'seller'}],
                    multi=True,
                    value=['seller'],
                    style={"height": "100%", "width": "150px"}
                )
            ],
                style={'flex': ['0', '0', '20%'], 'max-width': '20%', 'height': '90px', 'margin': '5px',
                       'margin-left': '10px'
                       }
            ),
            html.Div([
                dcc.Graph(
                    id='my-graph-level-change',
                    config={"displayModeBar": False},
                    style={"height": "90%", "width": "100%"}
                )
            ],
                style={'flex': ['0', '0', '80%'], 'max-width': '80%', 'height': '550px', 'margin': 'auto',
                       'margin-right': '120px'
                       }
            )
        ],
            style={'display': 'flex', 'flex-wrap': 'wrap', 'margin': 'auto', 'float': 'none',
                   'margin-left': '10px'
                   }
        )
    ],
        style={'display': 'block', 'flex-wrap': 'wrap', 'margin': 'auto',
               'margin-right': '30px', 'margin-left': '10px', 'width': '1200px'
               }
    )


@app.callback([Output('my-graph-kpi-toubu', 'figure'), Output('my-graph-kpi-yaobu', 'figure'),
               Output('my-graph-yewuxian', 'figure'), Output('my-graph-catezhanbi', 'figure')],
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
    print("头部腰部汇总数据-平均值")
    # 排序 使得做出来的图降序排序
    filter_data_product_level_total_toubu = \
        data_product_level_total[data_product_level_total.product_level.eq('头部商品')]. \
            sort_values(by='item_num_mean').tail(15)
    print("头部进度条数据-排序版")

    filter_data_product_level_total_yaobu = \
        data_product_level_total[data_product_level_total.product_level.eq('腰部商品')]. \
            sort_values(by='item_num_mean').tail(15)
    print("腰部进度条数据-排序版")

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
                                font=dict(family='Microsoft YaHei', size=9, color='rgb(255, 255, 255)'),
                                showarrow=False) for xi,
                                                     yi in zip(filter_data_product_level_total_toubu.done_lv_mean,
                                                               filter_data_product_level_total_toubu.front_cate_one)
                           ]

    annotations_item_num = [dict(x=-0.1, y=yi, text=str(int(np.round(zi, decimals=0))), xanchor='auto',
                                 yanchor='middle',
                                 font=dict(family='Microsoft YaHei', size=9, color='rgb(0, 81, 108)'),
                                 showarrow=False) for xi, yi,
                                                      zi in zip(filter_data_product_level_total_toubu.done_lv_mean,
                                                                filter_data_product_level_total_toubu.front_cate_one,
                                                                filter_data_product_level_total_toubu.item_num_mean)
                            ]
    # 头部布局
    fig_kpi_toubu.update_layout(title=dict(text="头部等级KPI", font=dict(family='Microsoft YaHei', size=15),
                                           xanchor='left', yanchor='top', xref='container', yref='container',
                                           x=0.45, y=0.98),
                                barmode='stack',
                                # width=500,
                                annotations=annotations_item_num + annotations_done_lv,
                                paper_bgcolor='rgb(255,255,255)',  # 设置绘图区背景色
                                plot_bgcolor='rgb(255,255,255)',  # 设置画布背景颜色
                                xaxis=dict({'categoryorder': 'total descending'}, side='top',  # 设置X轴刻度值置顶
                                           showgrid=False, showline=False, showticklabels=False),
                                yaxis=dict(tickangle=0, tickfont=dict(family='Microsoft YaHei', size=10)),  # 设置轴标签旋转角度
                                showlegend=False,  # 设置是否显示图例
                                margin=dict(l=35, r=5, t=35, b=35),
                                autosize=False
                                )

    # 添加设置腰部数据标签
    annotations_done_lv_yao = [
        dict(x=xi + 0.3, y=yi, text=str(int(np.round(xi, decimals=2) * 100)) + '%', xanchor='auto',
             yanchor='middle',
             font=dict(family='Microsoft YaHei', size=9, color='rgb(255, 255, 255)'),
             showarrow=False) for xi,
                                  yi in zip(filter_data_product_level_total_yaobu.done_lv_mean,
                                            filter_data_product_level_total_yaobu.front_cate_one)
    ]
    # xanchor 设置相对x轴位置对其方式
    annotations_item_num_yao = [dict(x=-0.1, y=yi, text=str(int(np.round(zi, decimals=0))), xanchor='auto',
                                     yanchor='middle',
                                     font=dict(family='Microsoft YaHei', size=9, color='rgb(0, 81, 108)'),
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
    fig_kpi_yaobu.update_layout(title=dict(text="腰部等级KPI", font=dict(family='Microsoft YaHei', size=15),
                                           xanchor='left',
                                           yanchor='top', xref='container', yref='container', x=0.45, y=0.98),
                                barmode='stack',
                                # width=500,
                                annotations=annotations_item_num_yao + annotations_done_lv_yao,
                                paper_bgcolor='rgb(255,255,255)',
                                plot_bgcolor='rgb(255,255,255)',
                                xaxis=dict({'categoryorder': 'total descending'}, side='top', showgrid=False,
                                           showline=False, showticklabels=False),
                                yaxis=dict(tickangle=0, tickfont=dict(family='Microsoft YaHei', size=10)),
                                showlegend=False,
                                margin=dict(l=35, r=5, t=35, b=35),
                                autosize=False
                                )

    # 饼图数据联动-图形渲染
    data_yewuxian.log_date = pd.to_datetime(data_yewuxian.log_date, format='%Y-%m-%d')
    filter_data_yewuxian = data_yewuxian[(data_yewuxian.log_date >= start_date) & (data_yewuxian.log_date <= end_date)]

    filter_data_yewuxian_sum = filter_data_yewuxian.groupby(['yewuxian', 'front_cate_one'], as_index=False).sum()
    filter_data_yewuxian_sum_total = filter_data_yewuxian_sum[filter_data_yewuxian_sum.yewuxian != 'total']

    print("业务线占比图-饼图")

    # 子图设置方式 注意doman是根据列和行来设置的，一行一个中括号
    fig_yewuxian = make_subplots(rows=2, cols=1, specs=[[{'type': 'domain'}], [{'type': 'domain'}]])

    fig_yewuxian.add_trace(go.Pie(labels=filter_data_yewuxian_sum_total.yewuxian,
                                  values=filter_data_yewuxian_sum_total.gmv,
                                  name='GMV占比'), 1, 1)
    fig_yewuxian.add_trace(go.Pie(labels=filter_data_yewuxian_sum_total.yewuxian,
                                  values=filter_data_yewuxian_sum_total.uv,
                                  name='UV占比'), 2, 1)

    # hoverinfo 设置鼠标悬停是显示的方式
    fig_yewuxian.update_traces(hole=.4, hoverinfo="label+percent+value",  # 设置环形饼图空白内径的半径参数是与半径比值
                               textfont=dict(family='Microsoft YaHei', size=9),
                               marker=dict(colors=['rgb(235, 12, 25)', 'rgb(0, 81, 108)'],
                                           line=dict(color=['rgb(235, 12, 25)', 'rgb(0, 81, 108)'], width=2)))

    annotations_yewuxian = [dict(x=0.5, y=0.2, text='GMV占比', showarrow=False,
                                 font=dict(family='Microsoft YaHei', size=10)),
                            dict(x=0.5, y=0.8, text='UV占比', showarrow=False,
                                 font=dict(family='Microsoft YaHei', size=10))]

    fig_yewuxian.update_layout(title=dict(text='业务线GMV&UV占比', font=dict(family='Microsoft YaHei', size=15),
                                          xanchor='left', yanchor='top', xref='container', yref='container',
                                          x=0.35, y=0.98),
                               # width=500,
                               annotations=annotations_yewuxian,
                               legend=dict(xanchor='left', yanchor='top', x=0.56, y=0.57,  # 设置图例位置
                                           font=dict(family='Microsoft YaHei', size=10)),
                               margin=dict(l=35, r=5, t=35, b=35),
                               autosize=False
                               )

    # 梯形图数据联动-图形渲染
    data_cate_zhanbi.log_date = pd.to_datetime(data_cate_zhanbi.log_date, format='%Y-%m-%d')
    filter_data_cate_zhanbi = data_cate_zhanbi[(data_cate_zhanbi.log_date >= start_date) &
                                               (data_cate_zhanbi.log_date <= end_date)]

    filter_data_cate_zhanbi_sum = filter_data_cate_zhanbi.groupby(['front_cate_one'], as_index=False).sum()

    filter_data_cate_zhanbi_sum_df = filter_data_cate_zhanbi_sum[filter_data_cate_zhanbi_sum.front_cate_one != '全类目']

    filter_data_cate_zhanbi_sum['uv_zhanbi'] = \
        filter_data_cate_zhanbi_sum.uv / \
        filter_data_cate_zhanbi_sum_df.uv.sum()

    filter_data_cate_zhanbi_sum['GMV_zhanbi'] = \
        filter_data_cate_zhanbi_sum.gmv / \
        filter_data_cate_zhanbi_sum_df.gmv.sum()

    filter_data_cate_zhanbi_sum['item_num_zhanbi'] = \
        filter_data_cate_zhanbi_sum.item_num / \
        filter_data_cate_zhanbi_sum_df.item_num.sum()

    filter_data_cate_zhanbi_sum['total_zhanbi'] = filter_data_cate_zhanbi_sum['uv_zhanbi'] + \
                                                  filter_data_cate_zhanbi_sum['GMV_zhanbi'] + \
                                                  filter_data_cate_zhanbi_sum['item_num_zhanbi']

    filter_data_cate_zhanbi_sum_total_1 = \
        filter_data_cate_zhanbi_sum.sort_values(by='total_zhanbi', ascending=False).head(11)

    filter_data_cate_zhanbi_sum_total_1.uv_zhanbi = \
        filter_data_cate_zhanbi_sum_total_1.uv_zhanbi.apply(lambda x: format(x, '.2%'))

    filter_data_cate_zhanbi_sum_total_1.GMV_zhanbi = \
        filter_data_cate_zhanbi_sum_total_1.GMV_zhanbi.apply(lambda x: format(x, '.2%'))

    filter_data_cate_zhanbi_sum_total_1.item_num_zhanbi = \
        filter_data_cate_zhanbi_sum_total_1.item_num_zhanbi.apply(lambda x: format(x, '.2%'))

    filter_data_cate_zhanbi_sum_total = filter_data_cate_zhanbi_sum_total_1[
        filter_data_cate_zhanbi_sum_total_1['front_cate_one'] != '全类目']

    print("各类目各指标占比漏斗图")

    # 开始画梯形图
    fig_cate_zhanbi = go.Figure()

    fig_cate_zhanbi.add_trace(go.Funnel(name='GMV-%', y=filter_data_cate_zhanbi_sum_total.front_cate_one,
                                        x=filter_data_cate_zhanbi_sum_total.GMV_zhanbi,
                                        width=0.5,
                                        marker=dict(color='rgb(235, 12, 25)'),
                                        textposition='auto',
                                        textinfo='value',  # 设置显示标签方式
                                        textfont={"family": "Microsoft YaHei", "size": 9, "color": "black"}, ))

    fig_cate_zhanbi.add_trace(go.Funnel(name='UV-%', y=filter_data_cate_zhanbi_sum_total.front_cate_one,
                                        x=filter_data_cate_zhanbi_sum_total.uv_zhanbi,
                                        width=0.5,
                                        marker=dict(color='rgb(234, 143, 116)'),
                                        orientation='h',
                                        textposition='auto',
                                        textinfo='value',
                                        textfont={"family": "Microsoft YaHei", "size": 9, "color": "black"}, ))

    fig_cate_zhanbi.add_trace(go.Funnel(name='在售商品数-%', y=filter_data_cate_zhanbi_sum_total.front_cate_one,
                                        x=filter_data_cate_zhanbi_sum_total.item_num_zhanbi,
                                        width=0.5,
                                        marker=dict(color='rgb(0, 81, 108)'),
                                        orientation='h',
                                        textposition='auto',
                                        textinfo='value',
                                        textfont={"family": "Microsoft YaHei", "size": 9, "color": "black"}, ))

    fig_cate_zhanbi.update_traces(hoverinfo='none')

    fig_cate_zhanbi.update_layout(title=dict(text='类目GMV&UV&在售商品数占比',
                                             font=dict(family='Microsoft YaHei', size=15),
                                             xanchor='left', yanchor='top', xref='container', yref='container',
                                             x=0.25, y=0.98),
                                  # width=500,
                                  funnelmode='stack',
                                  paper_bgcolor='rgb(255, 255, 255)', plot_bgcolor='rgb(255, 255, 255)',
                                  yaxis=dict(tickfont=dict(family='Microsoft YaHei', size=10)),
                                  legend=dict(font=dict(family='Microsoft YaHei', size=10), x=0.7, y=0),
                                  margin=dict(l=35, r=5, t=35, b=35),
                                  autosize=False
                                  )

    return fig_kpi_toubu, fig_kpi_yaobu, fig_yewuxian, fig_cate_zhanbi


@app.callback(Output('my-graph-ca', 'figure'),
              [Input('my-dropdown-ca', 'value')], )
def update_graph_ca(selected_dropdown_value_ca):  # 类目销售趋势回调数据
    dropdown_ca = {"全类目": "全类目", "Women\'s Shoes": "Women\'s Shoes", "Women\'s Clothing": "Women\'s Clothing",
                   "Women\'s Bags": "Women\'s Bags", "Watches": "Watches", "Men\'s Shoes": "Men\'s Shoes",
                   "Men\'s Clothing": "Men\'s Clothing", "Men\'s Bags": "Men\'s Bags",
                   "Jewelry & Accessories": "Jewelry & Accessories", "Home Appliances": "Home Appliances", }
    trace1_ca = []
    trace2_ca = []
    trace3_ca = []
    for leimu in selected_dropdown_value_ca:
        # 设置total图例及数据
        trace1_ca.append(
            go.Scatter(x=data_category_total[data_category_total["category_one"] == leimu]["log_date"],
                       y=data_category_total[data_category_total["category_one"] == leimu]["gmv"],
                       mode='lines', opacity=1, name=f'Total {dropdown_ca[leimu]}',
                       line=dict(width=3),
                       textposition='bottom center'))
        # 设置cf图例及数据
        trace2_ca.append(
            go.Scatter(x=data_category_cf[data_category_cf["category_one"] == leimu]["log_date"],
                       y=data_category_cf[data_category_cf["category_one"] == leimu]["gmv"],
                       mode='lines', opacity=1, name=f'Cf {dropdown_ca[leimu]}',
                       line=dict(width=3),
                       textposition='bottom center'))
        # 设置seller图例及数据
        trace3_ca.append(
            go.Scatter(x=data_category_seller[data_category_seller["category_one"] == leimu]["log_date"],
                       y=data_category_seller[data_category_seller["category_one"] == leimu]["gmv"],
                       mode='lines', opacity=1, name=f'Seller {dropdown_ca[leimu]}',
                       line=dict(width=3),
                       textposition='bottom center'))
        # 画GMV变化趋势
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
                                     # height=600,
                                     width=1000,
                                     legend=dict(font=dict(family='Microsoft YaHei', size=10), x=0, y=1),
                                     margin=dict(l=35, r=5, t=35, b=35),
                                     title=dict(
                                         text=f"GMV for {', '.join(str(dropdown_ca[i]) for i in selected_dropdown_value_ca)}",
                                         font=dict(family='Microsoft YaHei', size=15), xanchor='left', yanchor='top',
                                         xref='container', yref='container', x=0.4, y=0.98),
                                     xaxis={'title': {'text': '日期 (可筛选)',
                                                      'font': {'family': 'Microsoft YaHei', 'size': 10}},
                                            'tickfont': {'family': 'Microsoft YaHei', 'size': 9},
                                            'rangeselector': {'buttons': list([{'count': 1, 'label': '1M',
                                                                                'step': 'month', 'stepmode': 'backward'
                                                                                },
                                                                               {'count': 6, 'label': '6M',
                                                                                'step': 'month', 'stepmode': 'backward'
                                                                                },
                                                                               {'step': 'all'}])},
                                            'showgrid': False, 'showline': False,
                                            'rangeslider': {'visible': True}, 'type': 'date'},
                                     yaxis={'title': {'text': '支付金额 (单位美元)',  # X轴类型为日期，否则报错
                                                      'font': {'family': 'Microsoft YaHei', 'size': 10}},
                                            'showgrid': False, 'showline': False,
                                            'tickfont': {'family': 'Microsoft YaHei', 'size': 9}},
                                     autosize=False
                                     )

                 }
    return figure_ca


@app.callback(Output('my-graph-level-change', 'figure'),
              [Input('my-dropdown-level-change', 'value')], )
def update_graph_level(selected_dropdown_value):
    dropdown_level_change = {"CF": "Cf", "seller": "Seller", }  # 等级流转回调数据
    trace1_level_change = []
    trace2_level_change = []
    for yewuxian in selected_dropdown_value:
        trace1_level_change.append(
            go.Scatter(x=data_level_change[data_level_change["laiyuan"] == yewuxian]["log_date"],
                       y=data_level_change[data_level_change["laiyuan"] == yewuxian]["rate1_3"],
                       mode='lines', opacity=1, name=f'{dropdown_level_change[yewuxian]} 转变成等级3',
                       line=dict(width=3),
                       textposition='bottom center'))
        trace2_level_change.append(
            go.Scatter(x=data_level_change[data_level_change["laiyuan"] == yewuxian]["log_date"],
                       y=data_level_change[data_level_change["laiyuan"] == yewuxian]["rate2_2"],
                       mode='lines', opacity=1, name=f'{dropdown_level_change[yewuxian]} 转变成等级-2',
                       line=dict(width=3),
                       textposition='bottom center'))
        traces_level_change = [trace1_level_change, trace2_level_change]
        df_level_change = [val_level for sublist_level in traces_level_change for val_level in sublist_level]
        figure_levle_change = \
            {'data': df_level_change,
             'layout': go.Layout(colorway=['rgb(235, 12, 25)', 'rgb(234, 143, 116)',
                                           'rgb(122, 37, 15)', 'rgb(0, 81, 108)'],
                                 # height=600,
                                 width=1000,
                                 hovermode="closest",  # 设置鼠标悬停时候，不显示对比
                                 title=dict(
                                     text=
                                     f"{','.join(str(dropdown_level_change[j]) for j in selected_dropdown_value)} "
                                     f"等级变化趋势",
                                     font=dict(family='Microsoft YaHei', size=15), xanchor='left', yanchor='top',
                                     xref='container', yref='container', x=0.4, y=0.98),
                                 xaxis={'title': {'text': '日期 (可筛选)',
                                                  'font': {'family': 'Microsoft YaHei', 'size': 10}},
                                        'rangeslider': {'visible': True}, 'type': 'date', 'showgrid': False,
                                        'showline': False,
                                        'tickfont': {'family': 'Microsoft YaHei', 'size': 9}},
                                 yaxis={'title': {'text': '占比 %',
                                                  'font': {'family': 'Microsoft YaHei', 'size': 10}},
                                        'showgrid': False, 'showline': False,
                                        'tickfont': {'family': 'Microsoft YaHei', 'size': 9}},
                                 margin=dict(l=35, r=5, t=35, b=35),
                                 legend=dict(font=dict(family='Microsoft YaHei', size=10),
                                             x=0, y=1
                                             # , traceorder='normal'
                                             ),
                                 autosize=False
                                 )
             }
    return figure_levle_change


if __name__ == '__main__':
    app.server.run()
# '192.168.130.49', 8050
