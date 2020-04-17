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
from textwrap import dedent
import sys
import urllib
import dash
from textwrap import dedent

sys.path.append('/Users/apache/PycharmProjects/shushan-CF/Dash+plotly/apps/projectone')
from appshudashboard import app
from layoutpage import header
from ConfigTag import buld_modal_info_overlay
from pages import GoodsHGuan

print("GoodsSLN-商品360°")

fig_auto_fetch_filter_goods = []
fig_goods360 = []

layout = \
    html.Div([
        html.Div([header(app)], ),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("**商品360°**",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 15}),
                    ],
                        className="padding-top-bot"
                    ),
                    html.Div([
                        dcc.Textarea(
                            id='item-no-goods360',
                            loading_state='is_loading',
                            placeholder="**请输入货号**\n"
                                        " \n"
                                        "示例-单个货号：\n"
                                        "UPA004783030N\n"
                                        " \n"
                                        "示例-多个货号：\n"
                                        "HPC006973405N\n"
                                        "UPA004783030N\n"
                                        "JBR000453556N\n",
                            # value="**请输入货号**",
                            style={'width': '150px', 'font-family': 'Microsoft YaHei', 'height': '200px',
                                   'font-size': 8, 'textAlign': 'left',
                                   'border-style': 'solid', 'border-color': '#D1D1D1', 'border-width': '1px'
                                   }
                        )
                    ],
                        className="padding-top-bot"
                    ),
                    html.Div([
                        html.Button('Click Submit', id='goods360-button',
                                    style={'font-family': 'Microsoft YaHei', 'font-size': 9, 'width': '150px'}
                                    )
                    ], className="padding-top-bot", style={'padding-top': '10px'}
                    ),
                ],
                    className="bg-white user-control add_yingying"
                )
            ],
                className="two columns card-top"
            ),
            html.Div(
                buld_modal_info_overlay('GOODS360', 'bottom', dedent(f"""
                最多输入两个货号 - 单个货号只展示近14天数据\n
                指标可多选 - 同时可最多选4个指标查看\n
                数据来源:商品日报\n                              
                过滤条件：曝光500以下商品且销量为0商品\n
                商品维度数据加载很慢，需要耐心等待哦~\n
                """)
                                        )
            ),
            html.Div([
                html.Div(
                    [
                        html.Div([
                            html.Label(
                                id='show-GOODS360-modal',
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
                                    id='goods360_checklist',
                                    options=[
                                        {"label": "商品等级", "value": "商品等级"},
                                        {"label": "曝光量", "value": "曝光量"},
                                        {"label": "点击数", "value": "点击数"},
                                        {"label": "加购数", "value": "加购数"},
                                        {"label": "收藏数", "value": "收藏数"},
                                        {"label": "商详访客", "value": "访客数"},
                                        {"label": "下单买家数", "value": "下单买家数"},
                                        {"label": "销售", "value": "支付金额"},
                                        {"label": "买家数", "value": "支付买家数"},
                                        {"label": "订单量", "value": "支付订单量"},
                                        {"label": "销量", "value": "支付件数"},
                                        {"label": "评分", "value": "历史评分"},
                                        {"label": "支付转化", "value": "支付转化率"},
                                    ],
                                    value=['访客数', '支付金额', '支付件数', '历史评分'],
                                    style={'font-family': 'Microsoft YaHei', 'font-size': 9},
                                    labelStyle={'display': 'inline-block'}
                                ),
                            ],
                            # className="padding-top-bot"
                        ),
                        html.Div([
                            html.Label(f"商品监测")],
                            className="padding-top-bot",
                            style={'textAlign': 'center', "font-family": "Microsoft YaHei",
                                   "font-size": "15px", 'color': 'rgb(0, 81, 108)',
                                   'fontWeight': 'bold',
                                   # 'padding': '5px'
                                   }
                        ),
                        html.Div(
                            [
                                html.Div(
                                    id='my-graph-goods360',
                                    # config={"displayModeBar": False}
                                )
                            ],
                            className="padding-top-bot"
                        ),
                        html.Div(
                            [
                                dcc.Graph(
                                    id='graph-goods360',
                                    config={"displayModeBar": False},
                                    # style={"width": "97%"}
                                )
                            ],
                            className="padding-top-bot"
                        )

                    ],
                    # style={'height': '550px'},
                    className="bg-white add_yingying"
                )
            ],
                className="ten columns card-left-top",
                id="GOODS360-div"
            ),
        ], className="row app-body"
        ),
        html.Div(GoodsHGuan.layout),
    ],
    )

for id in ['GOODS360']:
    @app.callback([Output(f"{id}-modal", 'style'), Output(f"{id}-div", 'style')],
                  [Input(f'show-{id}-modal', 'n_clicks'),
                   Input(f'close-{id}-modal', 'n_clicks')])
    def toggle_modal(n_show, n_close):
        ctx = dash.callback_context
        if ctx.triggered and ctx.triggered[0]['prop_id'].startswith('show-'):
            return {"display": "block"}, {'zIndex': 1003}
        else:
            return {"display": "none"}, {'zIndex': 0}


@app.callback([Output('my-graph-goods360', 'children'),
               Output('graph-goods360', 'figure')],
              [
                  Input('goods360-button', 'n_clicks')
              ],
              [
                  State('item-no-goods360', 'value'),
                  State('goods360_checklist', 'value')
              ])
def update_table_ca(n_clicks, item_no_filter, filter_goods_valuelist):
    global fig_auto_fetch_filter_goods, fig_goods360

    con_ms_atuofetch_goods_filter = pymssql.connect("172.16.92.2", "sa", "yssshushan2008", "CFflows", charset="utf8")

    checklist = {
        "商品等级": '商品等级',
        "曝光量": '曝光量',
        "点击数": '点击数',
        "加购数": '加购数',
        "收藏数": '收藏数',
        "访客数": '访客数',
        "下单买家数": '下单买家数',
        "支付金额": '支付金额',
        "支付买家数": '支付买家数',
        "支付订单量": "支付订单量",
        "支付件数": '支付件数',
        "历史评分": "历史评分",
        "支付转化率": "支付转化率",
    }

    item_no_re = item_no_filter.split()

    prefix = "'"
    suffix = "'\n"

    sql_autofetch_cateone_goods_filter = f"""
        select top 28 数据日期, 货号, 前台一级类目,
        {', '.join((checklist[i]) for i in filter_goods_valuelist)}
        from CFgoodsday.dbo.商品数据
        WHERE 数据日期 BETWEEN GETDATE()-14 and GETDATE()-1 and
        货号 in ({','.join((prefix + str(i) + suffix) for i in item_no_re)})
        ORDER BY 数据日期 DESC
        """
    print("打印商品360°看板")

    data_autofetch_goods_filter = pd.read_sql(sql_autofetch_cateone_goods_filter, con_ms_atuofetch_goods_filter)

    for s in filter_goods_valuelist:
        if s == '历史评分':
            for i in range(0, len(data_autofetch_goods_filter['历史评分'])):
                if data_autofetch_goods_filter.loc[i, '历史评分'] is None:
                    data_autofetch_goods_filter.loc[i, '历史评分'] = 0
                else:
                    data_autofetch_goods_filter.loc[i, '历史评分'] = \
                        (data_autofetch_goods_filter.loc[i, '历史评分'] / 1).round(2)

    data_auto_fetch_goods_fig_filter = data_autofetch_goods_filter.head(28)

    fig_auto_fetch_filter_goods = html.Div(
        [
            html.Div([
                dash_table.DataTable(
                    data=data_auto_fetch_goods_fig_filter.to_dict('records'),
                    columns=[{
                        'id': c, 'name': c}
                                for c in ['数据日期', '货号', '前台一级类目']
                            ] +
                            [{
                                'id': c, 'name': c,
                            } for c in ['访客数', '支付金额', '支付件数', '历史评分']
                            ],
                    sort_action="native",
                    style_header={
                        "font-family": "Microsoft YaHei",
                        "font-size": '9px',
                        # "color": "rgb(255, 255, 255)",
                        'fontWeight': 'bold',
                        'textAlign': 'center',
                        'height': 'auto',
                        # 'whiteSpace': 'normal',
                        # 'backgroundColor': 'rgb(0, 81, 108)',
                        # 'border': '0px',
                        'padding': '0px',
                    },
                    style_as_list_view=True,
                    style_cell={
                        "font-family": "Microsoft YaHei",
                        "font-size": '8px',
                        'padding': '0px',
                        # 'border': '0px',
                        'textAlign': 'center',
                        # 'border-bottom': '1px dotted'
                    },
                    style_data={'whiteSpace': 'normal', 'height': 'auto',
                                'padding': '0px'},
                )
            ], style={'padding-bottom': '5px'}
            ),
        ]
    )

    trace1_goods = []
    for item_id in item_no_re:
        for zhibiao_list in filter_goods_valuelist:
            trace1_goods.append(go.Scatter(
                x=data_autofetch_goods_filter[data_autofetch_goods_filter["货号"] == item_id]["数据日期"],
                y=data_autofetch_goods_filter[data_autofetch_goods_filter["货号"] == item_id][zhibiao_list],
                mode='lines', opacity=1,
                name=f'{item_id}, {zhibiao_list}',
                line=dict(width=3),
                # line_dash='dot',
                textposition='bottom center')
            )

    fig_goods360 = {
        'data': trace1_goods,
        'layout': go.Layout(colorway=['rgb(235, 12, 25)', 'rgb(234, 143, 116)', 'rgb(122, 37, 15)',
                                      'rgb(0, 81, 108)', 'rgb(93,145,167)', 'rgb(0,164,220)',
                                      'rgb(107,207,246)', 'rgb(0,137,130)', 'rgb(109,187,191)',
                                      'rgb(205,221,230)', 'rgb(184,207,220)', '#C49C94', '#E377C2', '#F7B6D2',
                                      '#7F7F7F', '#C7C7C7', '#BCBD22', '#BCBD22', '#DBDB8D', '#17BECF',
                                      '#9EDAE5', '#729ECE', '#FF9E4A', '#67BF5C', '#ED665D', '#AD8BC9',
                                      '#A8786E', '#ED97CA', '#A2A2A2', '#CDCC5D'],
                            # height=600,
                            legend=dict(font=dict(family='Microsoft YaHei', size=8), x=0, y=1),
                            margin=dict(l=35, r=5, t=35, b=35),
                            # title=dict(
                            #     text=f"商品监测趋势",
                            #     font=dict(family='Microsoft YaHei', size=13)),
                            xaxis={'title': {'text': '日期', 'font': {'family': 'Microsoft YaHei', 'size': 10}},
                                   'tickfont': {'family': 'Microsoft YaHei', 'size': 9},
                                   'showgrid': False, 'showline': False,
                                   },
                            yaxis={'title': {'text': '指标',  # X轴类型为日期，否则报错
                                             'font': {'family': 'Microsoft YaHei', 'size': 10}},
                                   'tickfont': {'family': 'Microsoft YaHei', 'size': 9},
                                   'showgrid': False, 'showline': False},
                            autosize=False
                            )

    }

    return fig_auto_fetch_filter_goods, fig_goods360
