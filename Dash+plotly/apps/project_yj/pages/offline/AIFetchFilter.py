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

print("AIFetchFilter-商品维度可筛选类目数据下载")

fig_auto_fetch_filter_goods = []

layout = \
    html.Div([
        # html.Div([header(app)], ),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("**营销-货号维度下载**",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 15}),
                    ],
                        className="padding-top-bot"
                    ),
                    html.Div([
                        html.Label("日期：",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        html.Br(),
                        dcc.DatePickerSingle(
                            id='date-picker-single-cate-AIfilter',
                            min_date_allowed=(datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
                            max_date_allowed=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                            date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                            calendar_orientation='vertical',
                            display_format="YY/M/D-Q-ωW-E",  # q ε
                            style={"height": "100%", "width": "150px", 'font-size': '9px'}
                        )
                    ],
                        className="padding-top-bot"
                    ),
                    html.Div([
                        html.A(html.Button("Download Data", id='data_autofetch_download_button_filter',
                                           style={"width": '150px', 'font-family': 'Microsoft YaHei', 'font-size': 9}
                                           ),
                               id='AIfetch-download-link-filter',
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
                buld_modal_info_overlay('AIGoodsFilter', 'bottom', dedent(f"""
                多个货号一定要以换行来区分\n
                最多可输入500个货号\n
                货号前后不能有任何其他字符含空格\n
                """)
                                        )
            ),
            html.Div([
                html.Div([
                    html.Div([
                        dcc.Textarea(
                            id='item-no-AIfilter',
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
                            style={'width': '150px', 'font-family': 'Microsoft YaHei', 'height': '530px',
                                   'font-size': 8, 'textAlign': 'left',
                                   'border-style': 'solid', 'border-color': '#D1D1D1', 'border-width': '1px'
                                   }
                        )
                    ], className="padding-top-bot"
                    ),
                    html.Div([
                        html.Button('Click Submit', id='goods-AIfilter-button',
                                    style={'font-family': 'Microsoft YaHei', 'font-size': 9, 'width': '150px'}
                                    )
                    ], className="padding-top-bot", style={'padding-top': '10px'}
                    ),
                ],
                    style={'height': '600px'},
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
                                id='show-AIGoodsFilter-modal',
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
                                    id='filter_AIfetch_goods_checklist',
                                    options=[{"label": "后台一级", "value": "cat1_cn", },
                                             {"label": "后台二级", "value": "cat2_cn", },
                                             {"label": "后台三级", "value": "cat3_cn", },
                                             {"label": "商品等级", "value": "item_level", },
                                             {"label": "黑白名单", "value": "black_white_list", },
                                             {"label": "最小活动价格", "value": "activity_price_min", },
                                             {"label": "商品整体曝光pv", "value": "imp", },
                                             {"label": "商品整体曝光uv", "value": "imp_uv", },
                                             {"label": "商品整体点击pv", "value": "click", },
                                             {"label": "商品整体点击uv", "value": "click_uv", },
                                             {"label": "商品15天曝光pv", "value": "imp_15d", },
                                             {"label": "商品15天曝光uv", "value": "imp_uv_15d", },
                                             {"label": "商详15天曝光pv", "value": "imp_page_pv_15d", },
                                             {"label": "商详15天曝光uv", "value": "imp_page_uv_15d", },
                                             {"label": "近15天点击率", "value": "ctr_15d", },
                                             {"label": "近15天加购率", "value": "acr_15d", },
                                             {"label": "近15天收藏率", "value": "wr_uv_15d", },
                                             {"label": "15天加购人数", "value": "add_uv_15d", },
                                             {"label": "15天收藏人数", "value": "wishlist_uv_15d", },
                                             {"label": "近15天销量", "value": "confirm_sales_15d", },
                                             {"label": "近15天gmv", "value": "confirm_gmv_15d", },
                                             {"label": "近15天订单数", "value": "confirm_orders_15d", },
                                             {"label": "近15天买家数", "value": "confirm_buyers_15d", },
                                             {"label": "近15天女性买家数", "value": "confirm_buyer_female_15d", },
                                             {"label": "近15天男性买家数", "value": "confirm_buyer_male_15d", },
                                             {"label": "近15天中性买家数", "value": "confirm_buyer_neutral_15d", },
                                             {"label": "历史均分", "value": "score", },
                                             {"label": "历史评分数", "value": "score_cnt", },
                                             {"label": "近15天取消量", "value": "cancel_15d", },
                                             {"label": "近15天退货量", "value": "return_15d", },
                                             ],
                                    value=["item_level"],
                                    style={'font-family': 'Microsoft YaHei', 'font-size': 9},
                                    labelStyle={'display': 'inline-block'}
                                ),
                            ],
                            # className="padding-top-bot"
                        ),
                        html.Div(
                            [
                                dcc.Graph(
                                    id='my-graph-AIfetch-goods-filter',
                                    config={"displayModeBar": False}
                                )
                            ],
                            # className="padding-top-bot"
                        )
                    ],
                    style={'height': '600px'},
                    className="bg-white add_yingying"
                )
            ],
                className="eight columns card-left",
                id="AIGoodsFilter-div"
            )
        ], className="row app-body"
        )
    ],
    )

for id in ['AIGoodsFilter']:
    @app.callback([Output(f"{id}-modal", 'style'), Output(f"{id}-div", 'style')],
                  [Input(f'show-{id}-modal', 'n_clicks'),
                   Input(f'close-{id}-modal', 'n_clicks')])
    def toggle_modal(n_show, n_close):
        ctx = dash.callback_context
        if ctx.triggered and ctx.triggered[0]['prop_id'].startswith('show-'):
            return {"display": "block"}, {'zIndex': 1003}
        else:
            return {"display": "none"}, {'zIndex': 0}


@app.callback([Output('my-graph-AIfetch-goods-filter', 'figure'), Output('AIfetch-download-link-filter', 'href')],
              [
                  Input('goods-AIfilter-button', 'n_clicks')
              ],
              [
                  State('date-picker-single-cate-AIfilter', 'date'),
                  State('item-no-AIfilter', 'value'),
                  State('filter_AIfetch_goods_checklist', 'value')
              ])
def update_table_ca(n_clicks, event_day, item_no_filter, filter_goods_valuelist):
    print("打印AIfetchfilter看板")
    global fig_auto_fetch_filter_goods

    con_red_atuofetch_goods_filter = \
        prestodb.dbapi.connect(
            host='ec2-54-68-88-224.us-west-2.compute.amazonaws.com',
            port=80,
            user='hadoop',
            catalog='hive',
            schema='default',
        )

    checklist = {"cat1_cn": 'cat1_cn',
                 "cat2_cn": 'cat2_cn',
                 "cat3_cn": 'cat3_cn',
                 "item_level": 'item_level',
                 "black_white_list": 'black_white_list',
                 "activity_price_min": 'activity_price_min',
                 "imp": 'imp',
                 "imp_uv": 'imp_uv',
                 "click": 'click',
                 "click_uv": 'click_uv',
                 "imp_15d": "imp_15d",
                 "imp_uv_15d": 'imp_uv_15d',
                 "imp_page_pv_15d": "imp_page_pv_15d",
                 "imp_page_uv_15d": "imp_page_uv_15d",
                 "ctr_15d": "ctr_15d",
                 "acr_15d": "acr_15d",
                 "wr_uv_15d": "wr_uv_15d",
                 "add_uv_15d": "add_uv_15d",
                 "wishlist_uv_15d": "wishlist_uv_15d",
                 "confirm_sales_15d": "confirm_sales_15d",
                 "confirm_gmv_15d": "confirm_gmv_15d",
                 "confirm_orders_15d": "confirm_orders_15d",
                 "confirm_buyers_15d": "confirm_buyers_15d",
                 "confirm_buyer_female_15d": "confirm_buyer_female_15d",
                 "confirm_buyer_male_15d": "confirm_buyer_male_15d",
                 "confirm_buyer_neutral_15d": "confirm_buyer_neutral_15d",
                 "score": "score",
                 "score_cnt": "score_cnt",
                 "cancel_15d": "cancel_15d",
                 "return_15d": "return_15d",
                 }

    print(item_no_filter)
    item_no_re = item_no_filter.split()

    prefix = "'"
    suffix = "'\n"

    sql_autofetch_cateone_goods_filter = f"""
        SELECT 
                pno ,front_cat1,front_cat2,front_cat3,seller_type,
                {', '.join((checklist[i]) for i in filter_goods_valuelist)}
        from jiayundw_dm.product_profile_df
        WHERE is_online=1 and pno in ({','.join((prefix + str(i) + suffix) for i in item_no_re)})
            and date_id ='{event_day}'
        limit 500000
        """

    print(f"打印AIfetchgoodsfilter-商品维度数据-开始下载中-共点击{n_clicks}次")

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

    print(f'打印AIfetchgoodsfilter-商品维度数据-下载完成-共点击{n_clicks}次')

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

