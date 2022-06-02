import pymssql
import pymysql
import pandas as pd  # 数据处理例如：读入，插入需要用的包
import numpy as np  # 平均值中位数需要用的包
import os  # 设置路径需要用的包
import psycopg2
import time
from datetime import datetime, timedelta  # 设置当前时间及时间间隔计算需要用的包
import plotly.graph_objects as go
import plotly
import dash_core_components as dcc  # 交互式组件
import dash_html_components as html  # 代码转html
from dash.dependencies import Input, Output  # 回调
from flask import Flask, render_template
import dash_daq as daq
import dash_table
from plotly.subplots import make_subplots  # 画子图加载包
import dash_auth
import sys
import urllib
import dash
from textwrap import dedent

sys.path.append('/Users/apache/PycharmProjects/shushan-CF/Dash+plotly/apps/projectone')
from appshudashboard import app
from pages import ReturnNetGmv
from layoutpage import header
from ConfigTag import buld_modal_info_overlay

print("Return-类目退回率")

con_ms = pymysql.connect("127.0.0.1",
                         "root",
                         "yssshushan2008",
                         "CFcategory",
                         charset="utf8")

sql_ms = ('''
SELECT week, old_cate_one,yewu_type,tuihuolv
from CFcategory.CategoryReturn
where week BETWEEN DATENAME(week, curdate())-16 and DATENAME(week, curdate())-4
ORDER BY week
''')

data_return_rate = pd.read_sql(sql_ms, con_ms)

return_front_cate_one = [
    '全类目', 'Women\'s Shoes', 'Women\'s Clothing', 'Women\'s Bags',
    'Watches', 'Men\'s Shoes', 'Home',
    'Men\'s Clothing', 'Men\'s Bags', 'Jewelry & Accessories', 'Home Appliances',
    'Mobiles & Accessories', 'Electronics']

fig_return = []

layout = \
    html.Div([
        html.Div([header(app)], ),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("**类目退货率**",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 15}),
                    ],
                        className="padding-top-bot"
                    ),
                    html.Div([
                        html.Label("业务类型：",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        dcc.Dropdown(
                            id='my-dropdown-ReturnRate',
                            options=[{'label': 'Cf', 'value': 'cf'},
                                     {'label': 'Seller', 'value': 'seller'},
                                     {'label': 'Total', 'value': 'total'}],
                            multi=True,
                            value=['total'],
                            style={'width': '150px', 'font-family': 'Microsoft YaHei',
                                   'font-size': 8, 'textAlign': 'left'}
                        )
                    ],
                        className="padding-top-bot"
                    ),
                    html.Div([
                        html.Label("一级类目：",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        dcc.Dropdown(
                            id='front-cate-one-ReturnRate',
                            placeholder="Select CategoryOne",
                            value=['全类目'],
                            options=[{'label': v, 'value': v} for v in return_front_cate_one],
                            # persistence=True,
                            multi=True,
                            style={'width': '150px', 'font-family': 'Microsoft YaHei',
                                   'font-size': 8, 'textAlign': 'left'}
                        )
                    ],
                        className="padding-top-bot"
                    ),
                ],
                    className="bg-white user-control add_yingying"
                )
            ],
                className="two columns card-top"
            ),
            html.Div(
                buld_modal_info_overlay('ReturnRate', 'bottom', dedent(f"""
                展示近6-8周退货数据\n
                因退货有延迟，所以时间维度都是前推\n
                退货率==退货件数/发货件数\n
                """)
                                        )
            ),
            html.Div([
                html.Div([
                    html.Div([
                        html.Label(
                            id='show-ReturnRate-modal',
                            children='�',
                            n_clicks=0,
                            className='info-icon',
                            style={"font-size": "13px", 'color': 'rgb(240, 240, 240)'}
                        )
                    ], className="container_title"
                    ),
                    html.Div(
                        [
                            dcc.Graph(
                                id='my-graph-ReturnRate',
                                config={"displayModeBar": False},
                                style={"width": "97%"}
                            )
                        ]
                    )
                ],
                    className="bg-white add_yingying"
                )
            ],
                className="ten columns card-left-top",
                id="ReturnRate-div"
            )
        ], className="row app-body"
        ),
        html.Div(ReturnNetGmv.layout)
    ],
    )

for id in ['ReturnRate']:
    @app.callback([Output(f"{id}-modal", 'style'), Output(f"{id}-div", 'style')],
                  [Input(f'show-{id}-modal', 'n_clicks'),
                   Input(f'close-{id}-modal', 'n_clicks')])
    def toggle_modal(n_show, n_close):
        ctx = dash.callback_context
        if ctx.triggered and ctx.triggered[0]['prop_id'].startswith('show-'):
            return {"display": "block"}, {'zIndex': 1003}
        else:
            return {"display": "none"}, {'zIndex': 0}


@app.callback(Output('my-graph-ReturnRate', 'figure'),
              [
                  Input('my-dropdown-ReturnRate', 'value'),
                  Input('front-cate-one-ReturnRate', 'value'),
              ], )
def update_table_ca(laiyuan_type, cate_one_return, ):
    global fig_return

    trace_goods = []
    for laiyuan_id in laiyuan_type:
        for cate_id in cate_one_return:
            trace_goods.append(go.Scatter(
                x=data_return_rate[(data_return_rate["yewu_type"] == laiyuan_id) &
                                   (data_return_rate["old_cate_one"] == cate_id)]["week"],
                y=data_return_rate[(data_return_rate["yewu_type"] == laiyuan_id) &
                                   (data_return_rate["old_cate_one"] == cate_id)]["tuihuolv"],
                mode='lines', opacity=1,
                name=f'{laiyuan_id}, {cate_id}',
                line=dict(width=3),
                # line_dash='dot',
                textposition='bottom center')
            )

    fig_return = {
        'data': trace_goods,
        'layout': go.Layout(colorway=['rgb(235, 12, 25)', 'rgb(234, 143, 116)', 'rgb(122, 37, 15)',
                                      'rgb(0, 81, 108)', 'rgb(93,145,167)', 'rgb(0,164,220)',
                                      'rgb(107,207,246)', 'rgb(0,137,130)', 'rgb(109,187,191)',
                                      'rgb(205,221,230)', 'rgb(184,207,220)', '#C49C94', '#E377C2', '#F7B6D2',
                                      '#7F7F7F', '#C7C7C7', '#BCBD22', '#BCBD22', '#DBDB8D', '#17BECF',
                                      '#9EDAE5', '#729ECE', '#FF9E4A', '#67BF5C', '#ED665D', '#AD8BC9',
                                      '#A8786E', '#ED97CA', '#A2A2A2', '#CDCC5D'],
                            # height=600,
                            legend=dict(font=dict(family='Microsoft YaHei', size=8), x=0, y=1),
                            margin=dict(l=35, r=5, t=35, b=35),
                            title=dict(
                                text=
                                f"各业务线一级类目退货率趋势",
                                font=dict(family='Microsoft YaHei', size=13), xanchor='left', yanchor='top',
                                xref='container', yref='container', x=0.4, y=0.98),
                            xaxis={'title': {'text': '周', 'font': {'family': 'Microsoft YaHei', 'size': 10}},
                                   'tickfont': {'family': 'Microsoft YaHei', 'size': 9},
                                   'showgrid': False, 'showline': False,
                                   },
                            yaxis={'title': {'text': '退货率',
                                             'font': {'family': 'Microsoft YaHei', 'size': 10}},
                                   'tickformat': '.2%',
                                   'tickfont': {'family': 'Microsoft YaHei', 'size': 9},
                                   'showgrid': False, 'showline': False},
                            autosize=False
                            )

    }

    return fig_return
