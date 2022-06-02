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
from dash.dependencies import Input, Output, State  # 回调
from flask import Flask, render_template
import dash_daq as daq
import dash_table
from plotly.subplots import make_subplots  # 画子图加载包
import dash_auth
from pyhive import hive
import prestodb
import sys
import urllib
import dash
from textwrap import dedent

sys.path.append('/Users/apache/PycharmProjects/shushan-CF/Dash+plotly/apps/projectone')
from appshudashboard import app
from layoutpage import header
from ConfigTag import buld_modal_info_overlay

df = pd.read_excel('/Users/apache/Downloads/PythonDa/category_relation_new.xlsx', sheet_name='Sheet1')
print("打印AIfetchgoods")

WomenShoes = list(set(df[df.old_cate_one.eq('Women\'s Shoes')].old_cate_two))
WomenShoes.append('总')
WomenClothing = list(set(df[df.old_cate_one.eq('Women\'s Clothing')].old_cate_two))
WomenClothing.append('总')
WomenBags = list(set(df[df.old_cate_one.eq('Women\'s Bags')].old_cate_two))
WomenBags.append('总')
Watches = list(set(df[df.old_cate_one.eq('Watches')].old_cate_two))
Watches.append('总')
MenShoes = list(set(df[df.old_cate_one.eq('Men\'s Shoes')].old_cate_two))
MenShoes.append('总')
MenClothing = list(set(df[df.old_cate_one.eq('Men\'s Clothing')].old_cate_two))
MenClothing.append('总')
MenBags = list(set(df[df.old_cate_one.eq('Men\'s Bags')].old_cate_two))
MenBags.append('总')
JewelryAccessories = list(set(df[df.old_cate_one.eq('Jewelry & Accessories')].old_cate_two))
JewelryAccessories.append('总')
HomeAppliances = list(set(df[df.old_cate_one.eq('Home Appliances')].old_cate_two))
HomeAppliances.append('总')
MobilesAccessories = list(set(df[df.old_cate_one.eq('Mobiles & Accessories')].old_cate_two))
MobilesAccessories.append('总')
Electronics = list(set(df[df.old_cate_one.eq('Electronics')].old_cate_two))
Electronics.append('总')

autofetch_front_cate_one_goods = ['Women\'s Shoes', 'Women\'s Clothing', 'Women\'s Bags', 'Watches', 'Men\'s Shoes',
                                  'Men\'s Clothing', 'Men\'s Bags', 'Jewelry & Accessories', 'Home Appliances',
                                  'Mobiles & Accessories', 'Electronics']

autofetch_front_cate_two_goods = {
    'Women\'s Shoes': WomenShoes,
    'Women\'s Clothing': WomenClothing,
    'Women\'s Bags': WomenBags,
    'Watches': Watches,
    'Men\'s Shoes': MenShoes,
    'Men\'s Clothing': MenClothing,
    'Men\'s Bags': MenBags,
    'Jewelry & Accessories': JewelryAccessories,
    'Home Appliances': HomeAppliances,
    'Mobiles & Accessories': MobilesAccessories,
    'Electronics': Electronics,
}

fig_auto_fetch_ca_goods = []

layout = \
    html.Div([
        # html.Div([header(app)], ),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("**营销-商品维度下载**",
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
                            id='date-picker-single-cate-autofetch',
                            min_date_allowed=(datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
                            max_date_allowed=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                            date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                            calendar_orientation='vertical',
                            display_format="YY/M/D-Q-ωW-E",  # q ε
                            style={"height": "100%", "width": "200px", 'font-size': '9px'}
                        )
                    ],
                        className="padding-top-bot"
                    ),
                    html.Div([
                        html.Label("业务类型：",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        dcc.Dropdown(
                            id='my-dropdown-AIfetch-ca-goods',
                            options=[{'label': 'Cf', 'value': 'cf'},
                                     {'label': 'Seller', 'value': 'seller'},
                                     {'label': 'Total', 'value': 'total'}],
                            multi=False,
                            value='seller',
                            style={'width': '150px', 'font-family': 'Microsoft YaHei',
                                   'font-size': 8, 'textAlign': 'left'}
                        )
                    ],
                        className="padding-top-bot"
                    ),
                    html.Div([
                        html.Label("一级类目：",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        dcc.Dropdown(
                            id='AIfetch-front-cate-one-goods',
                            placeholder="Select CategoryOne",
                            value='Home Appliances',
                            options=[{'label': v, 'value': v} for v in autofetch_front_cate_one_goods],
                            # persistence=True,
                            # multi=True
                            style={'width': '150px', 'font-family': 'Microsoft YaHei',
                                   'font-size': 8, 'textAlign': 'left'}
                        )
                    ],
                        className="padding-top-bot"
                    ),
                    html.Div(
                        [
                            dcc.Dropdown(
                                id='AIfetch-front-cate-two-goods',
                                # multi=True
                                style={'width': '150px', 'font-family': 'Microsoft YaHei',
                                       'font-size': 8, 'textAlign': 'left'}
                            )
                        ],
                        id='AIfetch-front-cate-two-container-goods',
                        # className="padding-top-bot"
                    ),
                    html.Div([
                        html.Button('Click Submit', id='AIgoods-filter-button',
                                    style={'font-family': 'Microsoft YaHei', 'font-size': 9, 'width': '150px'}
                                    )
                    ], className="padding-top-bot",
                        # style={'padding-top': '10px'}
                    ),
                    html.Div([
                        html.A(html.Button("Download Data", id='data_autofetch_download_button_goods',
                                           style={"width": '150px', 'font-family': 'Microsoft YaHei', 'font-size': 9}
                                           ),
                               id='AIfetch-download-link-goods',
                               href="",
                               download="数据.csv",
                               target="_blank",
                               ),
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
                buld_modal_info_overlay('AICateGoods', 'bottom', dedent(f"""
                默认下载在架商品 最多下载50w行数据\n
                日期：对应日期分区的全量数据
                如需下载一级，则二级选择'总'\n
                如需下载二级，则二级选择相应的类目\n
                """)
                                        )
            ),
            html.Div([
                html.Div([
                    html.Div([
                        html.Label(
                            id='show-AICateGoods-modal',
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
                                              'font-family': 'Microsoft YaHei', 'font-size': 13}
                                       ),
                            dcc.Checklist(
                                id='AIfetch_goods_checklist',
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
                                value=["item_level", "black_white_list"],
                                style={'font-family': 'Microsoft YaHei', 'font-size': 9},
                                labelStyle={'display': 'inline-block'}
                            ),
                        ],
                        # className="padding-top-bot"
                    ),
                    html.Div(
                        [
                            dcc.Graph(
                                id='my-graph-AIfetch-ca-goods',
                                config={"displayModeBar": False},
                            )
                        ]
                    )
                ],
                    className="bg-white add_yingying"
                )
            ],
                className="ten columns card-left",
                id="AICateGoods-div"
            )
        ], className="row app-body"
        )
    ],
    )

for id in ['AICateGoods']:
    @app.callback([Output(f"{id}-modal", 'style'), Output(f"{id}-div", 'style')],
                  [Input(f'show-{id}-modal', 'n_clicks'),
                   Input(f'close-{id}-modal', 'n_clicks')])
    def toggle_modal(n_show, n_close):
        ctx = dash.callback_context
        if ctx.triggered and ctx.triggered[0]['prop_id'].startswith('show-'):
            return {"display": "block"}, {'zIndex': 1003}
        else:
            return {"display": "none"}, {'zIndex': 0}


# 二级下拉 回调数据
@app.callback(Output('AIfetch-front-cate-two-container-goods', 'children'),
              [Input('AIfetch-front-cate-one-goods', 'value')]
              )
def set_front_cate_two(autofetch_cate_one_goods):
    autofetch_cate_two_goods = autofetch_front_cate_two_goods[autofetch_cate_one_goods]
    return \
        html.Div([
            html.Label("二级类目：",
                       style={'textAlign': 'left', 'width': '100%', 'position': 'relative',
                              'font-family': 'Microsoft YaHei', 'font-size': 13}),
            dcc.Dropdown(
                id='AIfetch-front-cate-two-goods',
                placeholder="Select CategoryTwo",
                # value=autofetch_cate_two_goods[0],
                options=[{'label': v, 'value': v} for v in autofetch_cate_two_goods],
                style={'width': '150px', 'font-family': 'Microsoft YaHei',
                       'font-size': 8, 'textAlign': 'left'},
                # persistence_type='session',
                # persistence=cate_one,
                # multi=True
            )
        ],
            className="padding-top-bot"
        )


@app.callback([Output('my-graph-AIfetch-ca-goods', 'figure'), Output('AIfetch-download-link-goods', 'href')],
              [Input('AIgoods-filter-button', 'n_clicks')],
              [
                  State('date-picker-single-cate-autofetch', 'date'),
                  State('my-dropdown-AIfetch-ca-goods', 'value'),
                  State('AIfetch-front-cate-one-goods', 'value'),
                  State('AIfetch-front-cate-two-goods', 'value'),
                  State('AIfetch_goods_checklist', 'value')
              ], )
def update_table_ca(n_clicks, event_day,
                    selected_dropdown_value_autofetch_ca,
                    autofetch_cate_one_goods, autofetch_cate_two_out_goods,
                    goods_check_auto_value):
    goods_checklist = {"cat1_cn": 'cat1_cn',
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

    global fig_auto_fetch_ca_goods
    goods_cate_select_dict_autofetch = {'Women\'s Shoes': "Women\\'s Shoes",
                                        'Women\'s Clothing': "Women\\'s Clothing",
                                        'Women\'s Bags': "Women\\'s Bags",
                                        'Watches': 'Watches',
                                        'Men\'s Shoes': "Men\\'s Shoes",
                                        'Men\'s Clothing': "Men\\'s Clothing",
                                        'Men\'s Bags': "Men\\'s Bags",
                                        'Jewelry & Accessories': 'Jewelry & Accessories',
                                        'Home Appliances': 'Home Appliances',
                                        'Mobiles & Accessories': 'Mobiles & Accessories',
                                        'Electronics': 'Electronics'}
    if autofetch_cate_two_out_goods == '总':
        con_red_atuofetch_goods = \
            prestodb.dbapi.connect(
                host='ec2-54-68-88-224.us-west-2.compute.amazonaws.com',
                port=80,
                user='hadoop',
                catalog='hive',
                schema='default',
            )

        sql_autofetch_cateone_goods = """
        SELECT a.pno,a.front_cat1,a.front_cat2,a.front_cat3,a.seller_type,{2}
        from (
        SELECT 
            pno,front_cat1,front_cat2,front_cat3,
            seller_type,
            {2}
            FROM jiayundw_dm.product_profile_df
            WHERE is_online=1 and date_id ='{3}'
        UNION ALL
        SELECT 
            pno,front_cat1,front_cat2,front_cat3,
            'total' as seller_type,
            {2}
            FROM jiayundw_dm.product_profile_df
            WHERE is_online=1 and and date_id ='{3}'
            ) a 
        WHERE front_cat1='{0}' and a.seller_type='{1}'
        limit 500000	
        """.format(goods_cate_select_dict_autofetch[autofetch_cate_one_goods],
                   selected_dropdown_value_autofetch_ca,
                   ', '.join((goods_checklist[g]) for g in goods_check_auto_value),
                   event_day
                   )

        print("打印AIautofetchgoods-商品维度数据-开始下载中")

        cursor = con_red_atuofetch_goods.cursor()
        cursor.execute(sql_autofetch_cateone_goods)
        data = cursor.fetchall()
        column_descriptions = cursor.description
        if data:
            data_autofetch_cateone_goods = pd.DataFrame(data)
            data_autofetch_cateone_goods.columns = [c[0] for c in column_descriptions]
        else:
            data_autofetch_cateone_goods = pd.DataFrame()

        csv_string_download_autofetch_goods = data_autofetch_cateone_goods.to_csv(index=False, encoding='utf-8')
        csv_string_download_autofetch_goods = "data:text/csv;charset=utf-8," + urllib.parse.quote(
            csv_string_download_autofetch_goods)

        print("打印AIautofetchgoods-商品维度数据-下载完成")

        data_auto_fetch_ca_cateone_fig_goods = data_autofetch_cateone_goods.head(10)

        fig_auto_fetch_ca_goods = \
            go.Figure(data=[
                go.Table(
                    header=
                    dict(values=list(data_auto_fetch_ca_cateone_fig_goods.columns),
                         fill_color='rgb(0, 81, 108)',
                         line=dict(color='rgb(0, 81, 108)', width=0.5),
                         align=['left'],
                         height=20,
                         font=dict(size=9, family='Microsoft YaHei', color='rgb(255, 255, 255)')),
                    # columnwidth=[10, 10, 4],
                    cells=
                    dict(
                        values=[
                            data_auto_fetch_ca_cateone_fig_goods[h].tolist()
                            for h in data_auto_fetch_ca_cateone_fig_goods.columns[0:]
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
        fig_auto_fetch_ca_goods.update_layout(
            autosize=False,
            # width=1000,
            # height=615,  # 设置高度
            title=
            dict(text=
                 f"{autofetch_cate_one_goods}--{autofetch_cate_two_out_goods} "
                 f"{selected_dropdown_value_autofetch_ca} 下载数据TOP预览",
                 font=dict(family='Microsoft YaHei', size=13),
                 x=0.5, y=0.88)
        )

    else:
        con_red_atuofetch_goods = \
            prestodb.dbapi.connect(
                host='ec2-54-68-88-224.us-west-2.compute.amazonaws.com',
                port=80,
                user='hadoop',
                catalog='hive',
                schema='default',
            )

        sql_autofetch_cateone_goods = """
        
        SELECT a.pno,a.front_cat1,a.front_cat2,a.front_cat3,a.seller_type,{3}
        from (
        SELECT 
            pno,front_cat1,front_cat2,front_cat3,
            seller_type,
            {3}
            FROM jiayundw_dm.product_profile_df
            WHERE is_online=1 and date_id ='{4}'
        UNION ALL
        SELECT 
            pno,front_cat1,front_cat2,front_cat3,
            'total' as seller_type,
            {3}
            FROM jiayundw_dm.product_profile_df
            WHERE is_online=1 and date_id ='{4}'
            ) a 
        WHERE front_cat1='{0}' and front_cat2='{1}' and a.seller_type='{2}'
        limit 500000
        """.format(goods_cate_select_dict_autofetch[autofetch_cate_one_goods],
                   autofetch_cate_two_out_goods,
                   selected_dropdown_value_autofetch_ca,
                   ', '.join((goods_checklist[n]) for n in goods_check_auto_value),
                   event_day
                   )

        print("打印AIautofetchgoods-商品维度数据-开始下载中")

        cursor = con_red_atuofetch_goods.cursor()
        cursor.execute(sql_autofetch_cateone_goods)
        data = cursor.fetchall()
        column_descriptions = cursor.description
        if data:
            data_autofetch_cateone_goods = pd.DataFrame(data)
            data_autofetch_cateone_goods.columns = [c[0] for c in column_descriptions]
        else:
            data_autofetch_cateone_goods = pd.DataFrame()

        csv_string_download_autofetch_goods = data_autofetch_cateone_goods.to_csv(index=False, encoding='utf-8')
        csv_string_download_autofetch_goods = "data:text/csv;charset=utf-8," + urllib.parse.quote(
            csv_string_download_autofetch_goods)

        print("打印AIautofetchgoods-商品维度数据-下载完成")

        data_auto_fetch_ca_cateone_fig_goods = data_autofetch_cateone_goods.head(15)

        fig_auto_fetch_ca_goods = \
            go.Figure(data=[
                go.Table(
                    header=
                    dict(values=list(data_auto_fetch_ca_cateone_fig_goods.columns),
                         fill_color='rgb(0, 81, 108)',
                         line=dict(color='rgb(0, 81, 108)', width=0.5),
                         align=['left'],
                         height=20,
                         font=dict(size=9, family='Microsoft YaHei', color='rgb(255, 255, 255)')),
                    # columnwidth=[10, 10, 4],
                    cells=
                    dict(
                        values=[
                            data_auto_fetch_ca_cateone_fig_goods[k].tolist()
                            for k in data_auto_fetch_ca_cateone_fig_goods.columns[0:]
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
        fig_auto_fetch_ca_goods.update_layout(
            autosize=False,
            # width=1000,
            # height=615,  # 设置高度
            title=
            dict(text=
                 f"{autofetch_cate_one_goods}--{autofetch_cate_two_out_goods} "
                 f"{selected_dropdown_value_autofetch_ca} 下载数据TOP预览",
                 font=dict(family='Microsoft YaHei', size=13),
                 x=0.5, y=0.88)
        )

    return fig_auto_fetch_ca_goods, csv_string_download_autofetch_goods


