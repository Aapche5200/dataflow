import pymysql
import pandas as pd  # 数据处理例如：读入，插入需要用的包
import numpy as np  # 平均值中位数需要用的包
import os  # 设置路径需要用的包
import psycopg2
from datetime import datetime, timedelta  # 设置当前时间及时间间隔计算需要用的包
import plotly.graph_objects as go
import plotly
import dash_core_components as dcc  # 交互式组件
import dash_html_components as html  # 代码转html
from dash.dependencies import Input, Output  # 回调
from flask import Flask, render_template
import dash_daq as daq
from plotly.subplots import make_subplots  # 画子图加载包
import dash_auth
import dash_table
import dash
import sys
from textwrap import dedent
import urllib
import dash_table.FormatTemplate as FormatTemplate
from dash_table.Format import Format, Scheme, Sign, Symbol

sys.path.append('/Users/apache/PycharmProjects/shushan-CF/Dash+plotly/apps/projectone')
from appshudashboard import app
from ConfigTag import buld_modal_info_overlay

con_mssql = pymysql.connect("127.0.0.1",
                            "root",
                            "yssshushan2008",
                            "CFcategory",
                            charset="utf8")

# 可视化table取数
sql_table_ca = ('''
SELECT a.log_date,a.front_cate_one,a.yewu_type,a.gmv,a.order_num,a.qty,a.pay_user_nums,(a.pay_user_nums/uv) as cvr,(a.gmv/a.pay_user_nums) as 客单价,pv
from (
SELECT 日期 as log_date,一级类目 front_cate_one,业务类型 yewu_type,SUM(独立访客) as uv,sum(支付金额) as gmv,sum(支付订单量) as order_num,
sum(曝光数量) as pv,sum(支付商品件数) as qty,AVG(在售商品数) as item_num,sum(买家数) as pay_user_nums
from CFcategory.category_123_day
WHERE (三级类目 = ' ' or 三级类目 is null) and (二级类目 = ' ' or 二级类目 is null)
GROUP BY 日期 ,一级类目,业务类型
) as a
where a.log_date BETWEEN date_sub(curdate(),interval 9 day) and date_sub(curdate(),interval 1 day) and a.front_cate_one='全类目' and a.yewu_type='total'
ORDER BY a.log_date,a.front_cate_one,a.yewu_type
''')

sql_ms_return = ('''
SELECT week, old_cate_one,yewu_type,tuihuolv
from CFcategory.CategoryReturn
where week BETWEEN DATENAME(week, date_sub(curdate(),interval 8 day)  and DATENAME(week, date_sub(curdate(),interval 5 day)  and old_cate_one='全类目' and yewu_type='total'
ORDER BY week DESC
''')

sql_ms_dau = ('''
SELECT event_date,dau from CFflows.CfDau
where event_date BETWEEN date_sub(curdate(),interval 9 day) and date_sub(curdate(),interval 1 day)
''')

# 可视化table数据处理
data_table_ca = pd.read_sql(sql_table_ca, con_mssql)
data_table_re = pd.read_sql(sql_ms_return, con_mssql)
data_table_dau = pd.read_sql(sql_ms_dau, con_mssql)
print("打印datadashboard-类目数据看板")

data_table_dau_now = data_table_dau[
    (data_table_dau.event_date == ((datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')))]
data_table_dau_now.reset_index(drop=True, inplace=True)
data_table_dau_past = data_table_dau[
    (data_table_dau.event_date == ((datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')))]
data_table_dau_past.reset_index(drop=True, inplace=True)

data_table_dau_now['dau_huanbi'] = ((data_table_dau_now.dau - data_table_dau_past.dau) / data_table_dau_past.dau)
data_table_dau_now['dau_huanbi'] = data_table_dau_now['dau_huanbi'].apply(lambda x: format(x, '.2%'))
data_table_dau_now['dau_huanbi_tag'] = ['▲' + str(i) if i.find('-') else "▼" + str(i)
                                        for i in data_table_dau_now['dau_huanbi']]

data_table_re_now = data_table_re.head(1)
data_table_re_now['huanbi'] = \
    (data_table_re.loc[0, 'tuihuolv'] - data_table_re.loc[1, 'tuihuolv']) / data_table_re.loc[1, 'tuihuolv']

data_table_re_now['tuihuolv'] = data_table_re_now['tuihuolv'].apply(lambda x: format(x, '.2%'))
data_table_re_now['huanbi'] = data_table_re_now['huanbi'].apply(lambda x: format(x, '.2%'))

data_table_re_now['huanbi_tag'] = ['▲' + str(i) if i.find('-') else "▼" + str(i)
                                   for i in data_table_re_now['huanbi']]

data_table_ca.log_date = pd.to_datetime(data_table_ca.log_date, format='%Y-%m-%d')
data_table_ca_now = data_table_ca[
    (data_table_ca.log_date == ((datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')))]
data_table_ca_now.reset_index(drop=True, inplace=True)
data_table_ca_past = data_table_ca[
    (data_table_ca.log_date == ((datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')))]
data_table_ca_past.reset_index(drop=True, inplace=True)

data_table_ca_now['gmv_huanbi'] = \
    ((data_table_ca_now.gmv - data_table_ca_past.gmv) / data_table_ca_past.gmv)
data_table_ca_now['order_num_huanbi'] = \
    ((data_table_ca_now.order_num - data_table_ca_past.order_num) / data_table_ca_past.order_num)
data_table_ca_now['qty_huanbi'] = \
    ((data_table_ca_now.qty - data_table_ca_past.qty) / data_table_ca_past.qty)
data_table_ca_now['cvr_huanbi'] = \
    ((data_table_ca_now.cvr - data_table_ca_past.cvr) / data_table_ca_past.cvr)
data_table_ca_now['客单价_huanbi'] = \
    ((data_table_ca_now.客单价 - data_table_ca_past.客单价) / data_table_ca_past.客单价)
data_table_ca_now['pv_huanbi'] = \
    ((data_table_ca_now.pv - data_table_ca_past.pv) / data_table_ca_past.pv)

data_table_ca_now['cvr'] = data_table_ca_now['cvr'].apply(lambda x: format(x, '.2%'))
data_table_ca_now['gmv_huanbi'] = data_table_ca_now['gmv_huanbi'].apply(lambda x: format(x, '.2%'))
data_table_ca_now['order_num_huanbi'] = data_table_ca_now['order_num_huanbi'].apply(lambda x: format(x, '.2%'))
data_table_ca_now['qty_huanbi'] = data_table_ca_now['qty_huanbi'].apply(lambda x: format(x, '.2%'))
data_table_ca_now['cvr_huanbi'] = data_table_ca_now['cvr_huanbi'].apply(lambda x: format(x, '.2%'))
data_table_ca_now['客单价_huanbi'] = data_table_ca_now['客单价_huanbi'].apply(lambda x: format(x, '.2%'))
data_table_ca_now['pv_huanbi'] = data_table_ca_now['pv_huanbi'].apply(lambda x: format(x, '.2%'))

data_table_ca_now['gmv_huanbi_tag'] = \
    ['▲' + str(i) if i.find('-') else "▼" + str(i)
     for i in data_table_ca_now['gmv_huanbi']]
data_table_ca_now['order_num_huanbi_tag'] = \
    ['▲' + str(i) if i.find('-') else "▼" + str(i)
     for i in data_table_ca_now['order_num_huanbi']]
data_table_ca_now['qty_huanbi_tag'] = \
    ['▲' + str(i) if i.find('-') else "▼" + str(i)
     for i in data_table_ca_now['qty_huanbi']]
data_table_ca_now['cvr_huanbi_tag'] = \
    ['▲' + str(i) if i.find('-') else "▼" + str(i)
     for i in data_table_ca_now['cvr_huanbi']]
data_table_ca_now['客单价_huanbi_tag'] = \
    ['▲' + str(i) if i.find('-') else "▼" + str(i)
     for i in data_table_ca_now['客单价_huanbi']]
data_table_ca_now['pv_huanbi_tag'] = \
    ['▲' + str(i) if i.find('-') else "▼" + str(i)
     for i in data_table_ca_now['pv_huanbi']]

heh = 'red' if data_table_ca_now.loc[0, 'gmv_huanbi_tag'].find('▲') else 'green'

fig_table_ca = []

# app = dash.Dash()

layout = \
    html.Div([
        html.Div([
            html.Div(
                buld_modal_info_overlay('banerboard', 'bottom', dedent(f"""
                支付数据:T-1天数据 - 退货数据:W-5数据 - \n
                数据来源:前台类目交易日报\n
                曝光和访客均为：商详页曝光和访客\n
                环比=(本周-上周)/上周 - 支付转化=支付买家数/访客 - 客单价=销售/支付买家数\n
                """)
                                        )
            ),
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("APP支付金额",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': '15px'}),
                        html.Br(),
                        html.Br(),
                        html.Label(f"{data_table_ca_now.iloc[0, 3].round(0)}",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': '13px'}),
                        html.Label("     "),
                        html.Label(f"{data_table_ca_now.loc[0, 'gmv_huanbi_tag']}",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': '13px',
                                          'color':
                                              f"{'red' if data_table_ca_now.loc[0, 'gmv_huanbi_tag'].find('▲') else 'green'}"
                                          })
                    ],
                    ),
                ],
                    style={'height': '100px'},
                    className="bg-white user-control add_yingying"
                ),
            ],
                className="one-six column card-banner"
            ),
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("APP订单量",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': '15px'}),
                        html.Br(),
                        html.Br(),
                        html.Label(f"{data_table_ca_now.iloc[0, 4].round(0)}",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': '13px'}),
                        html.Label("     "),
                        html.Label(f"{data_table_ca_now.loc[0, 'order_num_huanbi_tag']}",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': '13px',
                                          'color':
                                              f"{'red' if data_table_ca_now.loc[0, 'order_num_huanbi_tag'].find('▲') else 'green'}"
                                          })
                    ],
                    ),
                ],
                    style={'height': '100px'},
                    className="bg-white add_yingying"
                ),
            ],
                className="one-six column card-banner"
            ),
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("APP DAU",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': '15px'}),
                        html.Br(),
                        html.Br(),
                        html.Label(f"{data_table_dau_now.loc[0, 'dau'].round(0)}",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': '13px'}),
                        html.Label("     "),
                        html.Label(f"{data_table_dau_now.loc[0, 'dau_huanbi_tag']}",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': '13px',
                                          'color':
                                              f"{'red' if data_table_dau_now.loc[0, 'dau_huanbi_tag'].find('▲') else 'green'}"
                                          })
                    ],
                    ),
                ],
                    style={'height': '100px'},
                    className="bg-white add_yingying"
                ),
            ],
                className="one-six column card-banner"
            ),
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("APP支付转化",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': '15px'}),
                        html.Br(),
                        html.Br(),
                        html.Label(f"{data_table_ca_now.iloc[0, 7]}",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': '13px'}),
                        html.Label("     "),
                        html.Label(f"{data_table_ca_now.loc[0, 'cvr_huanbi_tag']}",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': '13px',
                                          'color':
                                              f"{'red' if data_table_ca_now.loc[0, 'cvr_huanbi_tag'].find('▲') else 'green'}"
                                          })
                    ],
                    ),
                ],
                    style={'height': '100px'},
                    className="bg-white add_yingying"
                ),
            ],
                className="one-six column card-banner"
            ),
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("APP客单价",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': '15px'}),
                        html.Br(),
                        html.Br(),
                        html.Label(f"{data_table_ca_now.iloc[0, 8].round(2)}",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': '13px'}),
                        html.Label("     "),
                        html.Label(f"{data_table_ca_now.loc[0, '客单价_huanbi_tag']}",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': '13px',
                                          'color':
                                              f"{'red' if data_table_ca_now.loc[0, '客单价_huanbi_tag'].find('▲') else 'green'}"
                                          })
                    ],
                    ),
                ],
                    style={'height': '100px'},
                    className="bg-white add_yingying"
                ),
            ],
                className="one-six column card-banner"
            ),
            html.Div([
                html.Div(
                    [
                        html.Div([
                            html.Label(
                                id='show-banerboard-modal',
                                children='�',
                                n_clicks=0,
                                className='info-icon',
                                style={"font-size": "13px", 'color': 'rgb(240, 240, 240)'}
                            )
                        ], className="container_title"
                        ),
                        html.Div(
                            [
                                html.Label("APP退货率",
                                           style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                                  'font-family': 'Microsoft YaHei', 'font-size': '15px'}),
                                html.Br(),
                                html.Br(),
                                html.Label(f"{data_table_re_now.loc[0, 'tuihuolv']}",
                                           style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                                  'font-family': 'Microsoft YaHei', 'font-size': '13px'}),
                                html.Label("     "),
                                html.Label(f"{data_table_re_now.loc[0, 'huanbi_tag']}",
                                           style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                                  'font-family': 'Microsoft YaHei', 'font-size': '13px',
                                                  'color':
                                                      f"{'green' if data_table_re_now.loc[0, 'huanbi_tag'].find('▲') else 'red'}"
                                                  })
                            ],
                        ),
                    ],
                    style={'height': '100px'},
                    className="bg-white add_yingying"
                )
            ],
                className="one-six column card-left-top",
                id="banerboard-div"
            )
        ], className="row app-body"
        ),
    ],
    )

for id in ['banerboard']:
    @app.callback([Output(f"{id}-modal", 'style'), Output(f"{id}-div", 'style')],
                  [Input(f'show-{id}-modal', 'n_clicks'),
                   Input(f'close-{id}-modal', 'n_clicks')])
    def toggle_modal(n_show, n_close):
        ctx = dash.callback_context
        if ctx.triggered and ctx.triggered[0]['prop_id'].startswith('show-'):
            return {"display": "block"}, {'zIndex': 1003}
        else:
            return {"display": "none"}, {'zIndex': 0}
