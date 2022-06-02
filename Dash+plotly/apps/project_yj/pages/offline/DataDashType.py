import pymssql
import pandas as pd  # 数据处理例如：读入，插入需要用的包
import numpy as np  # 平均值中位数需要用的包
import os  # 设置路径需要用的包
import psycopg2
import time
from datetime import datetime, timedelta  # 设置当前时间及时间间隔计算需要用的包
import plotly.graph_objects as go
import plotly.express as px
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

print("YewuType-业务类型占比数据")

df = px.data.tips()

con_ms = pymssql.connect("172.16.92.2", "sa", "yssshushan2008", "CFflows", charset="utf8")

sql_ms = ('''
SELECT 业务类型,sum(支付金额) as gmv
from CFcategory.dbo.category_123_day
WHERE (三级类目 = ' ' or 三级类目 is null) and (二级类目 = ' ' or 二级类目 is null) and 日期 BETWEEN getdate()-2 and getdate()-1 and 一级类目 = '全类目' and 业务类型!='total'
GROUP BY 业务类型
ORDER BY 业务类型
''')

data_type = pd.read_sql(sql_ms, con_ms)

data_type['total'] = 'total'
print(data_type)

filter_data_type = \
    data_type.sort_values(by='gmv', ascending=False).head(10)

fig_type = []

trace_goods = []

fig_type = px.parallel_categories(filter_data_type, dimensions=['total', '业务类型'],
                                  color=filter_data_type.gmv,
                                  color_continuous_scale=px.colors.sequential.Inferno,
                                  # size=filter_data_type.gmv,
                                  labels={'total': '整体', '业务类型': '业务线',},
                                  )

fig_type.update_layout(showlegend=False,)

app = dash.Dash()

app.layout = \
    html.Div([
        html.Div([
            html.Div(
                buld_modal_info_overlay('YewuType', 'bottom', dedent(f"""
                类目GMV概况\n
                """)
                                        )
            ),
            html.Div([
                html.Div([
                    html.Div([
                        html.Label(
                            id='show-YewuType-modal',
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
                                figure=fig_type,
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
                id="YewuType-div"
            )
        ], className="row app-body"
        ),
    ],
    )

for id in ['YewuType']:
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
