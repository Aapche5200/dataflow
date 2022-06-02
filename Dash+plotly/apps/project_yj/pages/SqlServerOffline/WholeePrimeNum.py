import pymssql
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

con_ms = pymssql.connect("172.16.92.2", "sa", "yssshushan2008", "CFflows", charset="utf8")

sql_ms = ('''
select 日期,国家,会员数 from CFflows.dbo.UserPrime
where 日期 BETWEEN getdate()-90 and getdate()-1 
order by 日期
''')

sql_PrimeALL = ('''
select 国家,sum(会员数) as "会员数" from CFflows.dbo.UserPrime
where 日期 BETWEEN getdate()-90 and getdate()-1 
GROUP BY 国家
''')

data_WholeePrime = pd.read_sql(sql_ms, con_ms)
data_PrimeAll = pd.read_sql(sql_PrimeALL, con_ms)

print("Wholee每日新增会员数")

fig_WholeePrime = []

trace_WholeePrime_uk = []
trace_WholeePrime_gb = []
trace_WholeePrime_all = []

trace_WholeePrime_uk.append(go.Scatter(
    x=data_WholeePrime[data_WholeePrime["国家"] == "英国"]["日期"],
    y=data_WholeePrime[data_WholeePrime["国家"] == "英国"]["会员数"],
    mode='lines', opacity=1,
    name=f'英国新增会员数',
    line=dict(width=3),
    # line_dash='dot',
    textposition='bottom center')
)
trace_WholeePrime_gb.append(go.Scatter(
    x=data_WholeePrime[data_WholeePrime["国家"] == "美国"]["日期"],
    y=data_WholeePrime[data_WholeePrime["国家"] == "美国"]["会员数"],
    mode='lines', opacity=1,
    name=f'美国新增会员数',
    line=dict(width=3),
    # line_dash='dot',
    textposition='bottom center')
)
trace_WholeePrime_all.append(go.Scatter(
    x=data_WholeePrime[data_WholeePrime["国家"] == "整站"]["日期"],
    y=data_WholeePrime[data_WholeePrime["国家"] == "整站"]["会员数"],
    mode='lines', opacity=1,
    name=f'整站新增会员数',
    line=dict(width=3),
    # line_dash='dot',
    textposition='bottom center')
)
annotations_uk = []
annotations_gb = []
annotations_all = []

annotations_uk = [dict(xref='paper', x=0.99,
                       y=(data_WholeePrime[data_WholeePrime["国家"] == "英国"]).values[-1][2],  # 取最后一行一列数值
                       xanchor='left',
                       yanchor='middle',
                       text="总数" + str(int(data_PrimeAll[data_PrimeAll["国家"] == "英国"]["会员数"])),
                       font=dict(family='Microsoft YaHei', size=9, color='rgb(0, 81, 108)'),
                       showarrow=False
                       ), ]

annotations_gb = [dict(xref='paper', x=0.99,
                       y=(data_WholeePrime[data_WholeePrime["国家"] == "美国"]).values[-1][2],
                       xanchor='left',
                       yanchor='middle',
                       text="总数" + str(int(data_PrimeAll[data_PrimeAll["国家"] == "美国"]["会员数"])),
                       font=dict(family='Microsoft YaHei', size=9, color='rgb(0, 81, 108)'),
                       showarrow=False
                       ), ]

annotations_all = [dict(xref='paper', x=0.99,
                        y=(data_WholeePrime[data_WholeePrime["国家"] == "整站"]).values[-1][2],
                        xanchor='left',
                        yanchor='middle',
                        text="总数" + str(int(data_PrimeAll[data_PrimeAll["国家"] == "整站"]["会员数"])),
                        font=dict(family='Microsoft YaHei', size=9, color='rgb(0, 81, 108)'),
                        showarrow=False
                        ), ]

trace_WholeePrime = [trace_WholeePrime_uk, trace_WholeePrime_gb, trace_WholeePrime_all]
data_trace_WholeePrime = \
    [val_WholeePrime for sublist_WholeePrime in trace_WholeePrime for val_WholeePrime in sublist_WholeePrime]

fig_WholeePrime = {
    'data': data_trace_WholeePrime,
    'layout': go.Layout(colorway=['rgb(235, 12, 25)', 'rgb(234, 143, 116)', 'rgb(122, 37, 15)',
                                  'rgb(0, 81, 108)', 'rgb(93,145,167)', 'rgb(0,164,220)',
                                  'rgb(107,207,246)', 'rgb(0,137,130)', 'rgb(109,187,191)',
                                  'rgb(205,221,230)', 'rgb(184,207,220)', '#C49C94', '#E377C2', '#F7B6D2',
                                  '#7F7F7F', '#C7C7C7', '#BCBD22', '#BCBD22', '#DBDB8D', '#17BECF',
                                  '#9EDAE5', '#729ECE', '#FF9E4A', '#67BF5C', '#ED665D', '#AD8BC9',
                                  '#A8786E', '#ED97CA', '#A2A2A2', '#CDCC5D'],
                        # height=600,
                        legend=dict(font=dict(family='Microsoft YaHei', size=8), x=0, y=1),
                        margin=dict(l=35, r=35, t=35, b=35),
                        annotations=annotations_uk + annotations_gb + annotations_all,
                        title=dict(
                            text=
                            f"Wholee每日新增会员数趋势",
                            font=dict(family='Microsoft YaHei', size=13), xanchor='left', yanchor='top',
                            xref='container', yref='container', x=0.4, y=0.98),
                        xaxis={'title': {'text': '日期', 'font': {'family': 'Microsoft YaHei', 'size': 10}},
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
                        yaxis={'title': {'text': 'NetGmv',
                                         'font': {'family': 'Microsoft YaHei', 'size': 10}},
                               'tickfont': {'family': 'Microsoft YaHei', 'size': 9},
                               'showgrid': False, 'showline': False},
                        autosize=True
                        )
}

layout = \
    html.Div([
        # html.Div([header(app)], ),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("**Wholee新增会员数**",
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
                buld_modal_info_overlay('WholeePrime', 'bottom', dedent(f"""
                展示近90天数据\n
                Wholee每日新增会员数--区分国家\n
                右侧为此国家截止当前累积会员总数量\n
                """)
                                        )
            ),
            html.Div([
                html.Div([
                    html.Div([
                        html.Label(
                            id='show-WholeePrime-modal',
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
                                id='my-graph-WholeePrime',
                                figure=fig_WholeePrime,
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
                id="WholeePrime-div"
            )
        ], className="row app-body"
        )
    ],
    )

for id in ['WholeePrime']:
    @app.callback([Output(f"{id}-modal", 'style'), Output(f"{id}-div", 'style')],
                  [Input(f'show-{id}-modal', 'n_clicks'),
                   Input(f'close-{id}-modal', 'n_clicks')])
    def toggle_modal(n_show, n_close):
        ctx = dash.callback_context
        if ctx.triggered and ctx.triggered[0]['prop_id'].startswith('show-'):
            return {"display": "block"}, {'zIndex': 1003}
        else:
            return {"display": "none"}, {'zIndex': 0}
