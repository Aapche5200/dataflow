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

con_ms = pymssql.connect("172.16.92.2", "sa", "yssshushan2008", "CFflows", charset="utf8")

sql_ms = ('''
SELECT 一级类目,业务类型,SUM(独立访客) as 商详访客,sum(支付金额) as gmv,
sum(曝光数量) as 曝光量,sum(支付商品件数) as 销量,avg(支付转化率 ) as 日均转化率,AVG(在售商品数) as 日均在售商品数,
(sum(支付金额)/sum(支付商品件数)) as 出单均价,(avg(支付商品数)/avg(在售商品数)) as  平均动销率
from CFcategory.dbo.category_123_day
WHERE (三级类目 = ' ' or 三级类目 is null) and (二级类目 = ' ' or 二级类目 is null) and 日期 BETWEEN getdate()-2 and getdate()-1 and 业务类型 = 'total' and 一级类目 != '全类目'
GROUP BY 一级类目,业务类型
ORDER BY 一级类目,业务类型
''')

data_dau = pd.read_sql(sql_ms, con_ms)

filter_data_dau = \
    data_dau.sort_values(by='gmv', ascending=False).head(10)

fig_dau = []

trace_goods = []

fig_dau = go.Figure(go.Funnelarea(
    textfont=dict(family='Microsoft YaHei', size=9, color='rgb(255, 255, 255)'),
    text=filter_data_dau.一级类目,
    values=filter_data_dau.gmv))

fig_dau.update_layout(colorway=['rgb(235, 12, 25)', 'rgb(234, 143, 116)', 'rgb(122, 37, 15)',
                                'rgb(0, 81, 108)', 'rgb(93,145,167)', 'rgb(0,164,220)',
                                'rgb(107,207,246)', 'rgb(0,137,130)', 'rgb(109,187,191)',
                                'rgb(205,221,230)', 'rgb(184,207,220)', '#C49C94', '#E377C2', '#F7B6D2',
                                '#7F7F7F', '#C7C7C7', '#BCBD22', '#BCBD22', '#DBDB8D', '#17BECF',
                                '#9EDAE5', '#729ECE', '#FF9E4A', '#67BF5C', '#ED665D', '#AD8BC9',
                                '#A8786E', '#ED97CA', '#A2A2A2', '#CDCC5D'],
                      # height=600,
                      # width=990,
                      # legend=dict(font=dict(family='Microsoft YaHei', size=8), x=0, y=1),
                      showlegend=False,
                      margin=dict(l=35, r=5, t=35, b=35),
                      title=dict(
                          text=
                          f"类目销售排行榜",
                          font=dict(family='Microsoft YaHei', size=13), xanchor='left', yanchor='top',
                          xref='container', yref='container', x=0.48, y=0.98),
                      autosize=True,
                      )

app = dash.Dash()

app.layout = \
    html.Div([
        html.Div([
            html.Div(
                buld_modal_info_overlay('YewuType', 'bottom', dedent(f"""
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
