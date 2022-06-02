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
import plotly.figure_factory as ff
import geocoder
import json
import pandas as pd
from urllib.request import urlopen
from pandas.io.json import json_normalize

sys.path.append('/Users/apache/PycharmProjects/shushan-CF/Dash+plotly/apps/projectone')
from appshudashboard import app
from pages import ReturnNetGmv
from layoutpage import header
from ConfigTag import buld_modal_info_overlay

print("MapPay-类目退回率")

con_ms = pymssql.connect("172.16.92.2", "sa", "yssshushan2008", "CFflows", charset="utf8")

sql_ms = ('''
SELECT shipping_country,shipping_state,sum(origin_amount) AS gmv
from CFcategory.dbo.MapPay
WHERE log_date between (GETDATE()-30) and (GETDATE()-1) and shipping_country='India' 
and shipping_state not LIKE '%?%' and shipping_state !=''
GROUP BY shipping_country,shipping_state
''')

data_map = pd.read_sql(sql_ms, con_ms)

with urlopen('https://raw.githubusercontent.com/geohacker/india/master/state/india_state.geojson') as f:
    d = json.load(f)

df = json_normalize(d['features'])

figure = plotly.graph_objects.Choropleth(
    geojson=d,
    featureidkey="properties.id",
    # locations=[1, 2, 3, 4, 5],
    # locationmode="geojson-id"
)

app = dash.Dash()

app.layout = \
    html.Div([
        html.Div([header(app)], ),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("**MAP地区占比**",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 15}),
                    ],
                        className="padding-top-bot"
                    ),
                ],
                    className="bg-white user-control"
                )
            ],
                className="two columns card-top add_yingying"
            ),
            html.Div(
                buld_modal_info_overlay('MapPay', 'bottom', dedent(f"""
                近30天数据\n
                因退货有延迟，所以时间维度都是前推\n
                退货率==退货件数/发货件数\n
                """)
                                        )
            ),
            html.Div([
                html.Div([
                    html.Div([
                        html.Label(
                            id='show-MapPay-modal',
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
                                figure=figure,
                                id='my-graph-MapPay',
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
                id="MapPay-div"
            )
        ], className="row app-body"
        )
    ],
    )

for id in ['MapPay']:
    @app.callback([Output(f"{id}-modal", 'style'), Output(f"{id}-div", 'style')],
                  [Input(f'show-{id}-modal', 'n_clicks'),
                   Input(f'close-{id}-modal', 'n_clicks')])
    def toggle_modal(n_show, n_close):
        ctx = dash.callback_context
        if ctx.triggered and ctx.triggered[0]['prop_id'].startswith('show-'):
            return {"display": "block"}, {'zIndex': 1003}
        else:
            return {"display": "none"}, {'zIndex': 0}

if __name__ == '__main__':
    app.server.run(port=5002)
