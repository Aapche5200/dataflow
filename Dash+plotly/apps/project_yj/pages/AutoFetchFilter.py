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

print("AutoFetchFilter-商品维度可筛选类目数据下载")

fig_auto_fetch_filter_goods = []

layout = \
    html.Div([
        # html.Div([header(app)], ),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("**货号维度下载**",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 15}),
                    ],
                        className="padding-top-bot"
                    ),
                    html.Div([
                        html.A(html.Button("Download Data", id='data_autofetch_download_button_filter',
                                           style={"width": '150px', 'font-family': 'Microsoft YaHei', 'font-size': 9}
                                           ),
                               id='autofetch-download-link-filter',
                               href="",
                               download="筛选货号数据.csv",
                               target="_blank",
                               ),
                    ],
                        className="padding-top-bot"
                    )
                ],
                    className="bg-white user-control add_yingying"
                )
            ],
                className="two columns card"
            ),
            html.Div(
                buld_modal_info_overlay('AutoGoodsFilter', 'bottom', dedent(f"""
                多个货号一定要以换行来区分\n
                最多可输入50w个货号\n
                货号前后不能有任何其他字符含空格\n
                """)
                                        )
            ),
            html.Div([
                html.Div([
                    html.Div([
                        dcc.Textarea(
                            id='item-no-filter',
                            loading_state='is_loading',
                            placeholder="**请输入货号**\n"
                                        " \n"
                                        "示例-单个货号：\n"
                                        "UPA004783030N\n"
                                        " \n"
                                        "示例-多个货号：\n"
                                        "HPC006973405N\n"
                                        "UPA004783030N\n"
                                        "JBR000453556N\n"
                                        " \n"
                                        "ps:\n所有符号都在英文状态下\n",
                            # value="'UPA004783030N'",
                            style={'width': '150px', 'font-family': 'Microsoft YaHei', 'height': '480px',
                                   'font-size': 8, 'textAlign': 'left',
                                   'border-style': 'solid', 'border-color': '#D1D1D1', 'border-width': '1px'
                                   }
                        )
                    ], className="padding-top-bot"
                    ),
                    html.Div([
                        html.Button('Click Submit', id='goods-filter-button',
                                    style={'font-family': 'Microsoft YaHei', 'font-size': 9, 'width': '150px'}
                                    )
                    ], className="padding-top-bot", style={'padding-top': '10px'}
                    ),
                ],
                    style={'height': '550px'},
                    className="bg-white add_yingying"
                ),
            ],
                className="two columns card-left"
            ),
            html.Div([
                html.Div(
                    [
                        html.Div([
                            html.Label(
                                id='show-AutoGoodsFilter-modal',
                                children='�',
                                n_clicks=0,
                                className='info-icon',
                                style={"font-size": "13px", 'color': 'rgb(240, 240, 240)'}
                            )
                        ], className="container_title"
                        ),
                        html.Div(
                            [
                                html.Label("指标选择-多选：",
                                           style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                                  'font-family': 'Microsoft YaHei', 'font-size': 13}),
                                dcc.Checklist(
                                    id='filter_autofetch_goods_checklist',
                                    options=[{"label": "前台一级类目", "value": "front_cate_one"},
                                             {"label": "前台二级类目", "value": "front_cate_two"},
                                             {"label": "前台三级类目", "value": "front_cate_three"},
                                             {"label": "后台一级类目", "value": "cate_one_cn"},
                                             {"label": "后台一级id", "value": "cate_one_id"},
                                             {"label": "后台二级类目", "value": "cate_two_cn"},
                                             {"label": "后台二级id", "value": "cate_two_id"},
                                             {"label": "后台三级类目", "value": "cate_three_cn"},
                                             {"label": "后台三级id", "value": "cate_three_id"},
                                             {"label": "商品等级", "value": "product_level"},
                                             {"label": "上货来源", "value": "write_uid"},
                                             {"label": "标签", "value": "illegal_tags"},
                                             {"label": "name", "value": "name"},
                                             {"label": "历史评分", "value": "rating"},
                                             ],
                                    value=["front_cate_one"],
                                    style={'font-family': 'Microsoft YaHei', 'font-size': 9},
                                    labelStyle={'display': 'inline-block'}
                                ),
                            ],
                            # className="padding-top-bot"
                        ),
                        html.Div(
                            [
                                dcc.Graph(
                                    id='my-graph-autofetch-goods-filter',
                                    config={"displayModeBar": False}
                                )
                            ],
                            # className="padding-top-bot"
                        )
                    ],
                    style={'height': '550px'},
                    className="bg-white add_yingying"
                )
            ],
                className="eight columns card-left",
                id="AutoGoodsFilter-div"
            )
        ], className="row app-body"
        )
    ],
    )

for id in ['AutoGoodsFilter']:
    @app.callback([Output(f"{id}-modal", 'style'), Output(f"{id}-div", 'style')],
                  [Input(f'show-{id}-modal', 'n_clicks'),
                   Input(f'close-{id}-modal', 'n_clicks')])
    def toggle_modal(n_show, n_close):
        ctx = dash.callback_context
        if ctx.triggered and ctx.triggered[0]['prop_id'].startswith('show-'):
            return {"display": "block"}, {'zIndex': 1003}
        else:
            return {"display": "none"}, {'zIndex': 0}


@app.callback([Output('my-graph-autofetch-goods-filter', 'figure'), Output('autofetch-download-link-filter', 'href')],
              [
                  Input('goods-filter-button', 'n_clicks')
              ],
              [
                  State('item-no-filter', 'value'),
                  State('filter_autofetch_goods_checklist', 'value')
              ])
def update_table_ca(n_clicks, item_no_filter, filter_goods_valuelist):
    print("打印autofetchfilter看板")
    global fig_auto_fetch_filter_goods

    con_red_atuofetch_goods_filter = \
        prestodb.dbapi.connect(
            host='ec2-54-68-88-224.us-west-2.compute.amazonaws.com',
            port=80,
            user='hadoop',
            catalog='hive',
            schema='default',
        )

    checklist = {"front_cate_one": 'front_cate_one',
                 "front_cate_two": 'front_cate_two',
                 "front_cate_three": 'front_cate_three',
                 "cate_one_cn": 'cate_one_cn',
                 "cate_one_id": 'cate_one_id',
                 "cate_two_cn": 'cate_two_cn',
                 "cate_two_id": 'cate_two_id',
                 "cate_three_cn": 'cate_three_cn',
                 "cate_three_id": 'cate_three_id',
                 "product_level": 'product_level',
                 "write_uid": 'write_uid',
                 "illegal_tags": 'illegal_tags',
                 "name": "name",
                 "rating": 'rating'
                 }

    item_no_re = item_no_filter.split()

    prefix = "'"
    suffix = "'\n"

    sql_autofetch_cateone_goods_filter = f"""
        SELECT item_no ,active,{', '.join((checklist[i]) for i in filter_goods_valuelist)}
        from jiayundw_dim.product_basic_info_df
        WHERE active=1 and item_no in ({','.join((prefix + str(i) + suffix) for i in item_no_re)})
        limit 500000
        """

    print(f"打印autofetchgoodsfilter-商品维度数据-开始下载中-共点击{n_clicks}次")

    cursor = con_red_atuofetch_goods_filter.cursor()
    cursor.execute(sql_autofetch_cateone_goods_filter)
    data = cursor.fetchall()
    column_descriptions = cursor.description
    if data:
        data_autofetch_goods_filter = pd.DataFrame(data)
        data_autofetch_goods_filter.columns = [c[0] for c in column_descriptions]
    else:
        data_autofetch_goods_filter = pd.DataFrame()

    csv_string_download_autofetch_filter = data_autofetch_goods_filter.to_csv(index=False, encoding='utf-8')
    csv_string_download_autofetch_filter = "data:text/csv;charset=utf-8," + urllib.parse.quote(
        csv_string_download_autofetch_filter)

    print(f'打印autofetchgoodsfilter-商品维度数据-下载完成-共点击{n_clicks}次')

    data_auto_fetch_goods_fig_filter = data_autofetch_goods_filter.head(10)

    fig_auto_fetch_filter_goods = \
        go.Figure(data=[
            go.Table(
                header=
                dict(values=list(data_auto_fetch_goods_fig_filter.columns),
                     fill_color='rgb(0, 81, 108)',
                     line=dict(color='rgb(0, 81, 108)', width=0.5),
                     align=['left'],
                     height=20,
                     font=dict(size=9, family='Microsoft YaHei', color='rgb(255, 255, 255)')),
                # columnwidth=[10, 10, 4],
                cells=
                dict(
                    values=[
                        data_auto_fetch_goods_fig_filter[k].tolist()
                        for k in data_auto_fetch_goods_fig_filter.columns[0:]
                    ],
                    # height=18.5,
                    fill_color='rgb(255, 255, 255)',
                    line=dict(color='rgb(0, 81, 108)', width=0.5),
                    align=['left'],
                    font=dict(size=8, family='Microsoft YaHei')
                )
            )
        ]
        )
    fig_auto_fetch_filter_goods.update_layout(
        autosize=False,
        # width=1000,
        # height=615,  # 设置高度
        title=
        dict(text=
             f"筛选货号-下载数据TOP预览",
             font=dict(family='Microsoft YaHei', size=13),
             x=0.5, y=0.88)
    )

    return fig_auto_fetch_filter_goods, csv_string_download_autofetch_filter

