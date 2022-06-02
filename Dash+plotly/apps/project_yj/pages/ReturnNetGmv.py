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

con_ms = pymysql.connect("127.0.0.1",
                         "root",
                         "yssshushan2008",
                         "CFcategory",
                         charset="utf8")

sql_ms = ('''
SELECT create_at, front_cate_one,seller_type,sum(gmv) gmv,SUM(refund_total) refund_total,(sum(gmv)-SUM(refund_total)) NetGmv,((sum(gmv)-SUM(refund_total))/sum(gmv)) NetRate
from CFcategory.NetGmvData
WHERE create_at BETWEEN date_sub(curdate(),interval 90 day) and date_sub(curdate(),interval 1 day)
GROUP BY create_at, front_cate_one,seller_type
UNION ALL
SELECT create_at,front_cate_one,'total' seller_type,sum(gmv) gmv,SUM(refund_total) refund_total,(sum(gmv)-SUM(refund_total)) NetGmv,((sum(gmv)-SUM(refund_total))/sum(gmv)) NetRate
from CFcategory.NetGmvData
WHERE create_at BETWEEN date_sub(curdate(),interval 90 day) and date_sub(curdate(),interval 1 day)
GROUP BY create_at,front_cate_one
UNION ALL
SELECT create_at,'全类目' front_cate_one,seller_type,sum(gmv) gmv,SUM(refund_total) refund_total,(sum(gmv)-SUM(refund_total)) NetGmv,((sum(gmv)-SUM(refund_total))/sum(gmv)) NetRate
from CFcategory.NetGmvData
WHERE create_at BETWEEN date_sub(curdate(),interval 90 day) and date_sub(curdate(),interval 1 day)
GROUP BY create_at,seller_type
UNION ALL
SELECT create_at,'全类目' front_cate_one,'total' seller_type,sum(gmv) gmv,SUM(refund_total) refund_total,(sum(gmv)-SUM(refund_total)) NetGmv,((sum(gmv)-SUM(refund_total))/sum(gmv)) NetRate
from CFcategory.NetGmvData
WHERE create_at BETWEEN date_sub(curdate(),interval 90 day) and date_sub(curdate(),interval 1 day)
GROUP BY create_at
ORDER BY create_at, front_cate_one,seller_type DESC
''')

data_Net_rate = pd.read_sql(sql_ms, con_ms)

print("ReturnNetGmv-NetGmv及占比")

Net_front_cate_one = [
    '全类目', 'Women\'s Shoes', 'Women\'s Clothing', 'Women\'s Bags',
    'Watches', 'Men\'s Shoes', 'Home',
    'Men\'s Clothing', 'Men\'s Bags', 'Jewelry & Accessories', 'Home Appliances',
    'Mobiles & Accessories', 'Electronics']

fig_Net = []

layout = \
    html.Div([
        # html.Div([header(app)], ),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("**NetGmv**",
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
                            id='laiyuan-dropdown-Netgmv',
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
                            id='front-cate-one-Netgmv',
                            placeholder="Select CategoryOne",
                            value=['全类目'],
                            options=[{'label': v, 'value': v} for v in Net_front_cate_one],
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
                className="two columns card"
            ),
            html.Div(
                buld_modal_info_overlay('NetGMV', 'bottom', dedent(f"""
                展示近90天数据\n
                取订单创建后40天Net数据\n
                """)
                                        )
            ),
            html.Div([
                html.Div([
                    html.Div([
                        html.Label(
                            id='show-NetGMV-modal',
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
                                id='my-graph-Netgmv',
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
                id="NetGMV-div"
            )
        ], className="row app-body"
        )
    ],
    )

for id in ['NetGMV']:
    @app.callback([Output(f"{id}-modal", 'style'), Output(f"{id}-div", 'style')],
                  [Input(f'show-{id}-modal', 'n_clicks'),
                   Input(f'close-{id}-modal', 'n_clicks')])
    def toggle_modal(n_show, n_close):
        ctx = dash.callback_context
        if ctx.triggered and ctx.triggered[0]['prop_id'].startswith('show-'):
            return {"display": "block"}, {'zIndex': 1003}
        else:
            return {"display": "none"}, {'zIndex': 0}


@app.callback(Output('my-graph-Netgmv', 'figure'),
              [
                  Input('laiyuan-dropdown-Netgmv', 'value'),
                  Input('front-cate-one-Netgmv', 'value'),
              ], )
def update_table_ca(laiyuan_type, cate_one_netgmv, ):
    global fig_Net

    trace_net_gmv = []
    trace_net_rate = []

    for laiyuan_id in laiyuan_type:
        for cate_id in cate_one_netgmv:
            trace_net_gmv.append(go.Scatter(
                x=data_Net_rate[(data_Net_rate["seller_type"] == laiyuan_id) &
                                (data_Net_rate["front_cate_one"] == cate_id)]["create_at"],
                y=data_Net_rate[(data_Net_rate["seller_type"] == laiyuan_id) &
                                (data_Net_rate["front_cate_one"] == cate_id)]["NetGmv"],
                mode='lines', opacity=1,
                name=f'{laiyuan_id} {cate_id} NetGmv',
                line=dict(width=3),
                # line_dash='dot',
                textposition='bottom center')
            )
            trace_net_rate.append(go.Scatter(
                x=data_Net_rate[(data_Net_rate["seller_type"] == laiyuan_id) &
                                (data_Net_rate["front_cate_one"] == cate_id)]["create_at"],
                y=data_Net_rate[(data_Net_rate["seller_type"] == laiyuan_id) &
                                (data_Net_rate["front_cate_one"] == cate_id)]["NetRate"],
                mode='lines', opacity=1,
                name=f'{laiyuan_id} {cate_id} Net占比',
                line=dict(width=3),
                yaxis='y2',
                # line_dash='dot',
                textposition='bottom center')
            )

    trace_net = [trace_net_gmv, trace_net_rate]
    data_trace_net = [val_net for sublist_netgmv in trace_net for val_net in sublist_netgmv]
    fig_Net = {
        'data': data_trace_net,
        'layout': go.Layout(colorway=['rgb(235, 12, 25)', 'rgb(234, 143, 116)', 'rgb(122, 37, 15)',
                                      'rgb(0, 81, 108)', 'rgb(93,145,167)', 'rgb(0,164,220)',
                                      'rgb(107,207,246)', 'rgb(0,137,130)', 'rgb(109,187,191)',
                                      'rgb(205,221,230)', 'rgb(184,207,220)', '#C49C94', '#E377C2', '#F7B6D2',
                                      '#7F7F7F', '#C7C7C7', '#BCBD22', '#BCBD22', '#DBDB8D', '#17BECF',
                                      '#9EDAE5', '#729ECE', '#FF9E4A', '#67BF5C', '#ED665D', '#AD8BC9',
                                      '#A8786E', '#ED97CA', '#A2A2A2', '#CDCC5D'],
                            # height=600,
                            legend=dict(font=dict(family='Microsoft YaHei', size=8), x=0, y=1),
                            margin=dict(l=40, r=30, t=35, b=35),
                            title=dict(
                                text=
                                f"各业务线一级类目NetGmv及占比",
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
                            yaxis2={'title': {'text': 'Net占比',
                                              'font': {'family': 'Microsoft YaHei', 'size': 10}},
                                    'tickformat': '%',
                                    'side': 'right',
                                    'overlaying': 'y',
                                    'anchor': "x",
                                    # 'position': 0.95,
                                    'tickfont': {'family': 'Microsoft YaHei', 'size': 9},
                                    'showgrid': False, 'showline': False},
                            autosize=False
                            )
    }

    return fig_Net

