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
from layoutpage import header
from ConfigTag import buld_modal_info_overlay
from pages import WholeeDAU
from pages import WholeePrimeNum

print("DAU-整站DAU数据")

con_ms = pymysql.connect("127.0.0.1",
                         "root",
                         "yssshushan2008",
                         "CFcategory",
                         charset="utf8")

sql_ms = ('''
SELECT event_date,dau from CFflows.CfDau
where event_date BETWEEN date_sub(curdate(),interval 90 day) and date_sub(curdate(),interval 1 day)
ORDER BY event_date
''')

data_dau = pd.read_sql(sql_ms, con_ms)

fig_dau = []

trace_goods = []

trace_goods.append(go.Scatter(
    x=data_dau["event_date"],
    y=data_dau["dau"],
    mode='lines', opacity=1,
    line=dict(width=3),
    # line_dash='dot',
    textposition='bottom center')
)

fig_dau = {
    'data': trace_goods,
    'layout': go.Layout(colorway=['rgb(235, 12, 25)', 'rgb(234, 143, 116)', 'rgb(122, 37, 15)',
                                  'rgb(0, 81, 108)', 'rgb(93,145,167)', 'rgb(0,164,220)',
                                  'rgb(107,207,246)', 'rgb(0,137,130)', 'rgb(109,187,191)',
                                  'rgb(205,221,230)', 'rgb(184,207,220)', '#C49C94', '#E377C2', '#F7B6D2',
                                  '#7F7F7F', '#C7C7C7', '#BCBD22', '#BCBD22', '#DBDB8D', '#17BECF',
                                  '#9EDAE5', '#729ECE', '#FF9E4A', '#67BF5C', '#ED665D', '#AD8BC9',
                                  '#A8786E', '#ED97CA', '#A2A2A2', '#CDCC5D'],
                        # height=600,
                        # width=990,
                        legend=dict(font=dict(family='Microsoft YaHei', size=8), x=0, y=1),
                        margin=dict(l=35, r=5, t=35, b=35),
                        title=dict(
                            text=
                            f"每日DAU趋势",
                            font=dict(family='Microsoft YaHei', size=13), xanchor='left', yanchor='top',
                            xref='container', yref='container', x=0.4, y=0.98),
                        xaxis={'title': {'text': 'Day', 'font': {'family': 'Microsoft YaHei', 'size': 10}},
                               'tickfont': {'family': 'Microsoft YaHei', 'size': 9},
                               'showgrid': False, 'showline': False,
                               'rangeselector': {'buttons': list([{'count': 1, 'label': '1M',
                                                                   'step': 'month', 'stepmode': 'backward'
                                                                   },
                                                                  # {'count': 2, 'label': '2M',
                                                                  #  'step': 'month', 'stepmode': 'backward'
                                                                  #  },
                                                                  {'step': 'all'}])},
                               'rangeslider': {'visible': True}, 'type': 'date'
                               },
                        yaxis={'title': {'text': 'Dau',
                                         'font': {'family': 'Microsoft YaHei', 'size': 10}},
                               'tickfont': {'family': 'Microsoft YaHei', 'size': 9},
                               'showgrid': False, 'showline': False},
                        autosize=True,
                        )

}


layout = \
    html.Div([
        # html.Div([header(app)], ),
        html.Div(WholeeDAU.layout),
        html.Div(WholeePrimeNum.layout),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("**DAU**",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 15}),
                    ],
                        className="padding-top-bot"
                    ),
                ],
                    className="bg-white user-control add_yingying"
                )
            ],
                className="two columns card"
            ),
            html.Div(
                buld_modal_info_overlay('Dau', 'bottom', dedent(f"""
                展示近90天数据\n
                数据13点更新，及时关注刷新\n
                推广获客的dau（整站数据）\n
                """)
                                        )
            ),
            html.Div([
                html.Div([
                    html.Div([
                        html.Label(
                            id='show-Dau-modal',
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
                                id='my-graph-dau',
                                figure=fig_dau,
                                config={"displayModeBar": False},
                                style={"width": "97%"}
                            )
                        ]
                    )
                ],
                    className="bg-white add_yingying"
                )
            ],
                className="ten columns card-left",
                id="Dau-div"
            )
        ], className="row app-body"
        ),
    ],
    )

for id in ['Dau']:
    @app.callback([Output(f"{id}-modal", 'style'), Output(f"{id}-div", 'style')],
                  [Input(f'show-{id}-modal', 'n_clicks'),
                   Input(f'close-{id}-modal', 'n_clicks')])
    def toggle_modal(n_show, n_close):
        ctx = dash.callback_context
        if ctx.triggered and ctx.triggered[0]['prop_id'].startswith('show-'):
            return {"display": "block"}, {'zIndex': 1003}
        else:
            return {"display": "none"}, {'zIndex': 0}

