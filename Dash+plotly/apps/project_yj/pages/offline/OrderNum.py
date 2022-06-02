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
import prestodb
import urllib
import dash
from textwrap import dedent
from pyhive import hive

sys.path.append('/Users/apache/PycharmProjects/shushan-CF/Dash+plotly/apps/projectone')
from appshudashboard import app
from layoutpage import header
from ConfigTag import buld_modal_info_overlay

con_hive = prestodb.dbapi.connect(
    host='ec2-54-68-88-224.us-west-2.compute.amazonaws.com',
    port=80,
    user='hadoop',
    catalog='hive',
    schema='default',
)

sql_hive = ('''
select 
       date(so.create_at) as log_date,
       case when a.write_uid=5 then 'seller' else 'cf' end seller_type,
       count(distinct so.order_name)  as pay_order_num,
	   sum(sol.origin_qty)  as origin_qty,
	   count(distinct sol.item_no) as dongtai_item
from jiayundw_dm.sale_order_info_df as so 
join dw_dwd.sale_order_so_essential_df as tt on tt.order_name=so.order_name
join jiayundw_dm.sale_order_line_df as sol on sol.order_name=so.order_name
join jiayundw_dim.product_basic_info_df as a on sol.item_no=a.item_no
where sol.is_delivery=0 and date(so.create_at) between date('2020-04-01') and date('2020-05-08') 
and date_id=('2020-05-08') and tt.is_essential =1
group by date(so.create_at),case when a.write_uid=5 then 'seller' else 'cf' end
order by date(so.create_at)
''')

cursor = con_hive.cursor()
cursor.execute(sql_hive)
data = cursor.fetchall()
column_descriptions = cursor.description
if data:
    data_OrderNum = pd.DataFrame(data)
    data_OrderNum.columns = [c[0] for c in column_descriptions]
else:
    data_OrderNum = pd.DataFrame()

print("OrderNum")

fig_OrderNum = []

app = dash.Dash()

app.layout = \
    html.Div([
        # html.Div([header(app)], ),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("**Essential-Order**",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 15}),
                    ],
                        className="padding-top-bot"
                    ),
                    html.Div([
                        html.Label("指标选择：",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        dcc.RadioItems(
                            id="type-OrderNum",
                            options=[
                                {"label": "订单量", "value": "pay_order_num"},
                                {"label": "销量", "value": "origin_qty",
                                 "disabled": False},
                                {"label": "动销商品数", "value": "dongtai_item",
                                 "disabled": False},
                            ],
                            value="pay_order_num",
                            labelStyle={'font-family': 'Microsoft YaHei', 'font-size': 9,
                                        'display': 'inline-block'}
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
                buld_modal_info_overlay('OrderNum', 'bottom', dedent(f"""
                essential订单逻辑\n
                检索逻辑：检索订单中商品是否essential，全是则订单是essential，否则非essential订单\n
                UTC时间\n
                """)
                                        )
            ),
            html.Div([
                html.Div([
                    html.Div([
                        html.Label(
                            id='show-OrderNum-modal',
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
                                id='my-graph-OrderNum',
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
                id="OrderNum-div"
            )
        ], className="row app-body"
        )
    ],
    )

for id in ['OrderNum']:
    @app.callback([Output(f"{id}-modal", 'style'), Output(f"{id}-div", 'style')],
                  [Input(f'show-{id}-modal', 'n_clicks'),
                   Input(f'close-{id}-modal', 'n_clicks')])
    def toggle_modal(n_show, n_close):
        ctx = dash.callback_context
        if ctx.triggered and ctx.triggered[0]['prop_id'].startswith('show-'):
            return {"display": "block"}, {'zIndex': 1003}
        else:
            return {"display": "none"}, {'zIndex': 0}


@app.callback(Output('my-graph-OrderNum', 'figure'),
              [
                  Input('type-OrderNum', 'value'),
              ], )
def update_table_ca(zhibiao_type, ):
    global fig_OrderNum

    trace_net_seller = []
    trace_net_cf = []

    trace_net_seller.append(go.Scatter(
        x=data_OrderNum[(data_OrderNum["seller_type"] == "seller")]["log_date"],
        y=data_OrderNum[(data_OrderNum["seller_type"] == "seller")][f"{zhibiao_type}"],
        mode='lines', opacity=1,
        name=f'seller {zhibiao_type}',
        line=dict(width=3),
        # line_dash='dot',
        textposition='bottom center')
    )
    trace_net_cf.append(go.Scatter(
        x=data_OrderNum[(data_OrderNum["seller_type"] == "cf")]["log_date"],
        y=data_OrderNum[(data_OrderNum["seller_type"] == "cf")][f"{zhibiao_type}"],
        mode='lines', opacity=1,
        name=f'cf {zhibiao_type}',
        line=dict(width=3),
        # line_dash='dot',
        textposition='bottom center')
    )

    trace_OrderNum = [trace_net_seller, trace_net_cf]
    data_trace_trace_OrderNum = [val_net for sublist_netgmv in trace_OrderNum for val_net in sublist_netgmv]
    fig_OrderNum = {
        'data': data_trace_trace_OrderNum,
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
                                f"essential订单-{zhibiao_type}-趋势图",
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
                            yaxis={'title': {'text': f'{zhibiao_type}',
                                             'font': {'family': 'Microsoft YaHei', 'size': 10}},
                                   'tickfont': {'family': 'Microsoft YaHei', 'size': 9},
                                   'showgrid': False, 'showline': False},
                            autosize=True
                            )
    }

    return fig_OrderNum


if __name__ == '__main__':
    app.server.run(port=5002)
