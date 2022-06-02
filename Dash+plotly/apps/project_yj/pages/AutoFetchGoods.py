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
print("打印atuofetchgoods")

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
                        html.Label("**商品维度下载**",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 15}),
                    ],
                        className="padding-top-bot"
                    ),
                    html.Div([
                        html.Label("业务类型：",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        dcc.Dropdown(
                            id='my-dropdown-autofetch-ca-goods',
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
                            id='autofetch-front-cate-one-goods',
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
                                id='autofetch-front-cate-two-goods',
                                # multi=True
                                style={'width': '150px', 'font-family': 'Microsoft YaHei',
                                       'font-size': 8, 'textAlign': 'left'}
                            )
                        ],
                        id='autofetch-front-cate-two-container-goods',
                        # className="padding-top-bot"
                    ),
                    html.Div([
                        html.A(html.Button("Download Data", id='data_autofetch_download_button_goods',
                                           style={"width": '150px', 'font-family': 'Microsoft YaHei', 'font-size': 9}
                                           ),
                               id='autofetch-download-link-goods',
                               href="",
                               download="数据.csv",
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
                buld_modal_info_overlay('AutoCateGoods', 'bottom', dedent(f"""
                最多下载50w行数据\n
                如需下载一级，则二级选择'总'\n
                如需下载二级，则二级选择相应的类目\n
                """)
                                        )
            ),
            html.Div([
                html.Div([
                    html.Div([
                        html.Label(
                            id='show-AutoCateGoods-modal',
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
                                id='autofetch_goods_checklist',
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
                                value=["front_cate_one", "front_cate_two"],
                                style={'font-family': 'Microsoft YaHei', 'font-size': 9},
                                labelStyle={'display': 'inline-block'}
                            ),
                        ],
                        # className="padding-top-bot"
                    ),
                    html.Div(
                        [
                            dcc.Graph(
                                id='my-graph-autofetch-ca-goods',
                                config={"displayModeBar": False},
                            )
                        ]
                    )
                ],
                    className="bg-white add_yingying"
                )
            ],
                className="ten columns card-left",
                id="AutoCateGoods-div"
            )
        ], className="row app-body"
        )
    ],
    )

for id in ['AutoCateGoods']:
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
@app.callback(Output('autofetch-front-cate-two-container-goods', 'children'),
              [Input('autofetch-front-cate-one-goods', 'value')]
              )
def set_front_cate_two(autofetch_cate_one_goods):
    autofetch_cate_two_goods = autofetch_front_cate_two_goods[autofetch_cate_one_goods]
    return \
        html.Div([
            html.Label("二级类目：",
                       style={'textAlign': 'left', 'width': '100%', 'position': 'relative',
                              'font-family': 'Microsoft YaHei', 'font-size': 13}),
            dcc.Dropdown(
                id='autofetch-front-cate-two-goods',
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


@app.callback([Output('my-graph-autofetch-ca-goods', 'figure'), Output('autofetch-download-link-goods', 'href')],
              [
                  Input('my-dropdown-autofetch-ca-goods', 'value'),
                  Input('autofetch-front-cate-one-goods', 'value'),
                  Input('autofetch-front-cate-two-goods', 'value'),
                  Input('autofetch_goods_checklist', 'value')
              ], )
def update_table_ca(selected_dropdown_value_autofetch_ca,
                    autofetch_cate_one_goods, autofetch_cate_two_out_goods,
                    goods_check_auto_value):
    goods_checklist = {"front_cate_one": 'front_cate_one',
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
        SELECT item_no ,seller_type,{2}
				from (
				SELECT item_no,
        case when write_uid=5 then 'seller' else 'cf' end seller_type,{2}
        FROM jiayundw_dim.product_basic_info_df
        WHERE active=1 
				UNION ALL
			  SELECT item_no,
        'total' as seller_type,{2}
        FROM jiayundw_dim.product_basic_info_df
        WHERE active=1 
				) a 
				WHERE front_cate_one='{0}' and seller_type='{1}'
				limit 500000	
        """.format(goods_cate_select_dict_autofetch[autofetch_cate_one_goods],
                   selected_dropdown_value_autofetch_ca,
                   ', '.join((goods_checklist[g]) for g in goods_check_auto_value)
                   )

        print("打印autofetchgoods-商品维度数据-开始下载中")

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

        print("打印autofetchgoods-商品维度数据-下载完成")

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
        SELECT item_no,seller_type ,{3}
				from (
				SELECT item_no,
        case when write_uid=5 then 'seller' else 'cf' end seller_type,{3}
        FROM jiayundw_dim.product_basic_info_df
        WHERE active=1 
				UNION ALL
			  SELECT item_no,
        'total' as seller_type,{3}
        FROM jiayundw_dim.product_basic_info_df
        WHERE active=1 
				) a
				WHERE front_cate_one='{0}' and front_cate_two='{1}' and seller_type='{2}'
				limit 500000
        """.format(goods_cate_select_dict_autofetch[autofetch_cate_one_goods],
                   autofetch_cate_two_out_goods,
                   selected_dropdown_value_autofetch_ca,
                   ', '.join((goods_checklist[n]) for n in goods_check_auto_value)
                   )

        print("打印autofetchgoods-商品维度数据-开始下载中")

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

        print("打印autofetchgoods-商品维度数据-下载完成")

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
