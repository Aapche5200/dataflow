import pymssql
import pandas as pd  # 数据处理例如：读入，插入需要用的包
import dash_table
import os  # 设置路径需要用的包
import psycopg2
import time
from datetime import datetime, timedelta  # 设置当前时间及时间间隔计算需要用的包
import plotly.graph_objects as go
import plotly
import dash_core_components as dcc  # 交互式组件
import dash_html_components as html  # 代码转html
from dash.dependencies import Input, Output, State  # 回调
from flask import Flask, render_template
import dash_daq as daq
import dash_table
from plotly.subplots import make_subplots  # 画子图加载包
import dash_auth
import dash
from pyhive import hive
import prestodb
from textwrap import dedent
import sys
import urllib
import dash
from textwrap import dedent

sys.path.append('/Users/apache/PycharmProjects/shushan-CF/Dash+plotly/apps/projectone')
from appshudashboard import app
from layoutpage import header
from ConfigTag import buld_modal_info_overlay

fig_auto_fetch_filter_goods = []

layout = \
    html.Div([
        html.Div([
            html.Div([header(app)], ),
            html.Div([
                html.Div([
                    html.Div([
                        html.Div([
                            html.Label("**Wholee-类目分场景数据看板**",
                                       style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                              'font-family': 'Microsoft YaHei', 'font-size': 15})
                        ],
                            # className="padding-top-bot"
                        ),
                    ],
                        style={'height': '43px'},
                        className="bg-white user-control add_yingying"
                    )
                ],
                    className="five columns card-top"
                ),
                html.Div([
                    html.Div([
                        html.Div([
                            dcc.DatePickerRange(
                                id='date-picker-range-cate-yewuxian-ott-Wholee',
                                min_date_allowed=(datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d'),
                                max_date_allowed=(datetime.now() - timedelta(days=0)).strftime('%Y-%m-%d'),
                                start_date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                                end_date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                                start_date_placeholder_text="Start Date",
                                end_date_placeholder_text="End Date",
                                calendar_orientation='vertical',
                                display_format="YY/M/D-Q-ωW-E",  # q ε
                                style={'font-family': 'Microsoft YaHei', 'font-size': '9px',
                                       "height": "100%", "border": "#ffffff"},
                                className="button_custom"
                            ),
                        ],
                            className="button_custom"
                        ),
                    ],
                        style={'height': '43px'},
                        className="bg-white add_yingying"
                    ),
                ],
                    className="five columns card-top"
                ),
                html.Div([
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.A(html.Button("Download Data", id='data_download_button_Wholee',
                                                       style={'font-family': 'Microsoft YaHei', 'font-size': 9,
                                                              "height": "100%", "width": "150px"},
                                                       className="button_custom"
                                                       ),
                                           id='download-link-ott-Wholee',
                                           href="",
                                           download="类目数据看板.csv",
                                           target="_blank",
                                           ),
                                ],
                                # className="padding-top-bot"
                            )
                        ],
                        style={'height': '43px'},
                        className="bg-white add_yingying"
                    )
                ],
                    className="two columns card-top",
                    id="AutoGoodsFilter-div"
                )
            ], className="row app-body"
            )
        ],
        ),
        html.Div([
            html.Div([
                html.Div(
                    buld_modal_info_overlay('WholeeCateFlowType', 'bottom', dedent("""
                    支付口径：宽口径-->统计时间内只要有点击过该场景的商品则计算其支付数据\n
                    访客逻辑：商品维度，按照类目聚合，计算商详页UV\n
                    访客、销售、销量、曝光=统计时间内日累计数据\n
                    环比=(本周-上周)/上周 - 支付转化=支付买家数/访客\n
                    日期可选范围：一级近90天，二级近60天，三级近30天\n
                    """)
                                            )
                ),
                html.Div([
                    html.Div([
                        html.Label(
                            id='show-WholeeCateFlowType-modal',
                            children='�',
                            n_clicks=0,
                            className='info-icon',
                            style={"font-size": "13px", 'color': 'rgb(240, 240, 240)'}
                        )
                    ], className="container_title"
                    ),
                    html.Div(
                        id='my-graph-table-ca-ott-Wholee',
                        className="bg-white add_yingying"
                    )
                ],
                    className="twelve-all columns card-left",
                    id="WholeeCateFlowType-div"
                )
            ], className="row app-body"
            ),
        ],
        )
    ])
