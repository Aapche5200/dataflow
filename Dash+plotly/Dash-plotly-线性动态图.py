import pymssql
import pandas as pd  # 数据处理例如：读入，插入需要用的包
import numpy as np  # 平均值中位数需要用的包
import os  # 设置路径需要用的包
import psycopg2
import dash
from datetime import datetime, timedelta  # 设置当前时间及时间间隔计算需要用的包
import plotly.graph_objects as go
import plotly
import dash_core_components as dcc  # 交互式组件
import dash_html_components as html  # 代码转html
from dash.dependencies import Input, Output  # 回调
from flask import Flask, render_template
import dash_daq as daq
import plotly.express as px
from plotly.subplots import make_subplots  # 画子图加载包
import dash_auth
import sys
import urllib

sys.path.append('/Users/apache/PycharmProjects/shushan-CF/Dash+plotly/apps/projectone')
# from appshudashboard import app

con_mssql = pymssql.connect("172.16.92.2", "sa", "yssshushan2008", "CFflows", charset="utf8")

# 可视化table取数
sql_animation_ca = ('''
SELECT MONTH(日期) as yearmonth,业务类型,一级类目 as category_one,sum(支付金额) as 支付金额,
        AVG(在售商品数) as 均在售商品数,(sum(支付金额)/sum(买家数)) as 客单价 from CFcategory.dbo.category_123_day
        WHERE (二级类目 = ' ' or 二级类目 is null ) and 一级类目 !='全类目' and 日期 BETWEEN '2019-01-01' and '2019-12-31'
        GROUP BY MONTH(日期),业务类型,一级类目 
        HAVING sum(支付金额)  >=150000
        ORDER BY MONTH(日期) ,业务类型,一级类目
''')

# 可视化table数据处理
data_animation_ca = pd.read_sql(sql_animation_ca, con_mssql)
print("打印datadashanimation-类目数据看板")


# VALID_USERNAME_PASSWORD_PAIRS = {
#     '1': '1'
# }
#
# auth = dash_auth.BasicAuth(
#     app,
#     VALID_USERNAME_PASSWORD_PAIRS
#
# )
app = dash.Dash()

fig_animation_ca = []
app.layout = \
    html.Div([
        # html.Div([header(app)], ),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("业务类型：",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        dcc.Dropdown(
                            id='my-dropdown-animation-ca',
                            options=[{'label': 'Cf', 'value': 'cf'},
                                     {'label': 'Seller', 'value': 'seller'},
                                     {'label': 'Total', 'value': 'total'}],
                            multi=False,
                            value='total',
                            style={'width': '150px', 'font-family': 'Microsoft YaHei',
                                   'font-size': 9, 'textAlign': 'left'}
                        )
                    ],
                        className="padding-top-bot"
                    ),
                ],
                    className="bg-white user-control"
                )
            ],
                className="two columns card"
            ),
            html.Div([
                html.Div([
                    html.Div([
                        dcc.Graph(
                            id='my-graph-animation-ca',
                            config={"displayModeBar": False},
                        ),
                        dcc.Interval(
                            id='animation-interval',
                            interval=1000,
                            n_intervals=0,
                            disabled=True
                        )
                    ]
                    ),
                    html.Div([
                        html.Button(
                            'Play',
                            id='button-animation-start',
                            n_clicks=0,
                            style={'font-family': 'Microsoft YaHei', 'font-size': 9, }
                        ),
                        html.Button(
                            'Stop',
                            id='button-animation-stop',
                            n_clicks=0,
                            style={'font-family': 'Microsoft YaHei', 'font-size': 9, }
                        )
                    ], style={"width": "40%", "margin-left": "auto",
                              "margin-right": "auto", "display": "block"
                              }
                    )
                ],
                    className="bg-white"
                )
            ],
                className="ten columns card-left"
            )
        ], className="row app-body"
        )
    ],
    )


@app.callback([
    Output('my-graph-animation-ca', 'figure'),
    Output('animation-interval', 'disabled')
],
    [
        Input('my-dropdown-animation-ca', 'value'),
        Input('animation-interval', 'n_intervals'),
        Input('button-animation-start', 'n_clicks'),
        Input('button-animation-stop', 'n_clicks')
    ], )
def update_table_ca(selected_dropdown_value_animation_ca,
                    interval, animation_play,
                    animation_stop):
    global fig_animation_ca

    data_animation_ca_filter = data_animation_ca[data_animation_ca['业务类型'] ==
                                                 selected_dropdown_value_animation_ca]

    animation_ctx = dash.callback_context

    WMC_trace = go.Scatter(
        x=data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                   "Women\'s Clothing"]['yearmonth'],
        y=data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                   "Women\'s Clothing"]['支付金额'][:interval],
        mode='lines',
        name="女装",
        line=dict(width=3),
        marker={"color": "rgb(235, 12, 25)"})

    MC_trace = go.Scatter(
        x=data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                   "Men\'s Clothing"]['yearmonth'],
        y=data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                   "Men\'s Clothing"]['支付金额'][:interval],
        mode='lines',
        name="男装",
        line=dict(width=3),
        marker={"color": "rgb(234, 143, 116)"})

    MAC_trace = go.Scatter(
        x=data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                   "Mobiles & Accessories"]['yearmonth'],
        y=data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                   "Mobiles & Accessories"]['支付金额'][:interval],
        mode='lines',
        name="手机配件",
        line=dict(width=3),
        marker={"color": "rgb(122, 37, 15)"})

    WSE_trace = go.Scatter(
        x=data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                   "Women\'s Shoes"]['yearmonth'],
        y=data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                   "Women\'s Shoes"]['支付金额'][:interval],
        mode='lines',
        name="女鞋",
        line=dict(width=3),
        marker={"color": "rgb(0, 81, 108)"})

    HAP_trace = go.Scatter(
        x=data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                   "Home Appliances"]['yearmonth'],
        y=data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                   "Home Appliances"]['支付金额'][:interval],
        mode='lines',
        name="小家电",
        line=dict(width=3),
        marker={"color": "rgb(93,145,167)"})

    JAC_trace = go.Scatter(
        x=data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                   "Jewelry & Accessories"]['yearmonth'],
        y=data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                   "Jewelry & Accessories"]['支付金额'][:interval],
        mode='lines',
        name="配饰",
        line=dict(width=3),
        marker={"color": "rgb(0,164,220)"})

    WBG_trace = go.Scatter(
        x=data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                   "Women\'s Bags"]['yearmonth'],
        y=data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                   "Women\'s Bags"]['支付金额'][:interval],
        mode='lines',
        name="女包",
        line=dict(width=3),
        marker={"color": "rgb(107,207,246)"})

    animation_data = [WMC_trace, MC_trace, MAC_trace,
                      WSE_trace, HAP_trace, JAC_trace,
                      WBG_trace
                      ]

    # for label, y_trace in zip(data_animation_ca_filter.category_one, data_animation_ca_filter['支付金额']):
    #     animation_annotations = [dict(xref='paper', x=0.95, y=y_trace,
    #                                   xanchor='right', yanchor='middle',
    #                                   text=label,
    #                                   font=dict(family='Microsoft YaHei', size=9),
    #                                   showarrow=False)
    #                              ]

    fig_animation_ca = {"data": animation_data,
                        "layout": go.Layout(title={"text": "类目月动态变化趋势",
                                                   'font': {'family': 'Microsoft YaHei', 'size': 13}
                                                   },
                                            # height=600,
                                            showlegend=False,
                                            xaxis={"title": {"text": "Date（月）",
                                                             'font': {'family': 'Microsoft YaHei', 'size': 10}},
                                                   "tickangle": 0,
                                                   "range": [0, 12],
                                                   "showline": False,
                                                   "showgrid": False,
                                                   'tickfont': {'family': 'Microsoft YaHei', 'size': 9}
                                                   },
                                            yaxis={"title": {"text": "销售额 $",
                                                             'font': {'family': 'Microsoft YaHei', 'size': 10}},
                                                   "range": [0, 20000000],
                                                   "showline": False,
                                                   "showgrid": True,
                                                   'tickfont': {'family': 'Microsoft YaHei', 'size': 9}
                                                   },
                                            hovermode="closest", ),
                        # "annotations": animation_annotations
                        }

    if animation_ctx.triggered[0]['prop_id'].split('.')[0] == \
            'button-animation-start' and animation_ctx.triggered[0]['prop_id'].split('.')[0] == \
            'animation-interval':
        animation_disable = False
    elif animation_ctx.triggered[0]['prop_id'].split('.')[0] == \
            'button-animation-stop' or interval >= 25:
        animation_disable = True
    elif animation_play == 0 and animation_stop == 0:
        animation_disable = True
    else:
        animation_disable = False

    return fig_animation_ca, animation_disable


if __name__ == '__main__':
    app.server.run('127.0.0.1', port=5001)
