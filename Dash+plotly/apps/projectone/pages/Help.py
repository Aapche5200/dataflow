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

print("Help")


layout = \
    html.Div([
        html.Div([header(app)], ),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([
                        html.Div([
                            html.Label("**Wholee-数据看板指导**",
                                       style={'textAlign': 'center', "width": "100%", 'position': 'relative',
                                              'font-family': 'Microsoft YaHei', 'font-size': 15, }),
                        ],
                            className="padding-top-bot"
                        ),
                        html.Div([
                            html.Label("//带有Wholee标题的均为Wholee站数据，否则为ClubFactory整站数据（已经停止更新）//",
                                       style={'textAlign': 'center', "width": "100%", 'position': 'relative',
                                              'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        ],
                            className="padding-top-bot"
                        ),
                        html.Div([
                            html.Label("逻辑及口径相关：及时关注每个模块右侧问号内容",
                                       style={'textAlign': 'center', "width": "100%", 'position': 'relative',
                                              'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        ],
                            className="padding-top-bot"
                        ),
                        html.Div([
                            html.Label("①首页--->包含整站订单、访客、销售及整体类目数据-已经停止更新",
                                       style={'textAlign': 'center', "width": "100%", 'position': 'relative',
                                              'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        ],
                            className="padding-top-bot"
                        ),
                        html.Div([
                            html.Label("②交易--->包含整站类目商品等级//类目趋势//等级变化//GMV占比",
                                       style={'textAlign': 'center', "width": "100%", 'position': 'relative',
                                              'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        ],
                            className="padding-top-bot"
                        ),
                        html.Div([
                            html.Label("③类目看板--->包含Wholee类目看板环比趋势//整站动态趋势图",
                                       style={'textAlign': 'center', "width": "100%", 'position': 'relative',
                                              'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        ],
                            className="padding-top-bot"
                        ),
                        html.Div([
                            html.Label("④店铺--->包含平台治理所有数据-已经停止更新",
                                       style={'textAlign': 'center', "width": "100%", 'position': 'relative',
                                              'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        ],
                            className="padding-top-bot"
                        ),
                        html.Div([
                            html.Label("⑤退货--->包含整站退货率//NetGMV-已停止更新",
                                       style={'textAlign': 'center', "width": "100%", 'position': 'relative',
                                              'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        ],
                            className="padding-top-bot"
                        ),
                        html.Div([
                            html.Label("⑥流量--->迭代中",
                                       style={'textAlign': 'center', "width": "100%", 'position': 'relative',
                                              'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        ],
                            className="padding-top-bot"
                        ),
                        html.Div([
                            html.Label("⑦用户--->包含WholeeDAU数据&Wholee会员数-更新中//整站DAU",
                                       style={'textAlign': 'center', "width": "100%", 'position': 'relative',
                                              'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        ],
                            className="padding-top-bot"
                        ),
                        html.Div([
                            html.Label("⑧品类--->包含整站商品维度监控",
                                       style={'textAlign': 'center', "width": "100%", 'position': 'relative',
                                              'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        ],
                            className="padding-top-bot"
                        ),
                        html.Div([
                            html.Label("⑨自助取数--->类目维度//商品维度 自定义取数工具--停止更新",
                                       style={'textAlign': 'center', "width": "100%", 'position': 'relative',
                                              'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        ],
                            className="padding-top-bot"
                        ),

                    ],
                    )

                ],
                    className="bg-white add_yingying"
                )
            ],
                className="twelve columns card-left-top"
            )
        ], className="row app-body"
        )
    ],
    )

