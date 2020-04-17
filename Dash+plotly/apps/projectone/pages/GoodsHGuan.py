import pymssql
import pandas as pd  # 数据处理例如：读入，插入需要用的包
import numpy as np  # 平均值中位数需要用的包
import os  # 设置路径需要用的包
import psycopg2
import time
import datetime
from datetime import timedelta  # 设置当前时间及时间间隔计算需要用的包
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
import dash
import urllib
from textwrap import dedent

sys.path.append('/Users/apache/PycharmProjects/shushan-CF/Dash+plotly/apps/projectone')
from appshudashboard import app
from layoutpage import header
from ConfigTag import buld_modal_info_overlay

print("GoodsHGuang-品类数据宏观监控")

df = pd.read_excel('/Users/apache/Downloads/PythonDa/category_relation_new.xlsx', sheet_name='Sheet1')

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
BeautyHealth = list(set(df[df.old_cate_one.eq('Beauty & Health')].old_cate_two))
BeautyHealth.append('总')
Home = list(set(df[df.old_cate_one.eq('Home')].old_cate_two))
Home.append('总')

goodshongguan_front_cate_one = ['全类目', 'Women\'s Shoes', 'Women\'s Clothing', 'Women\'s Bags', 'Watches',
                                'Men\'s Shoes', 'Home',
                                'Men\'s Clothing', 'Men\'s Bags', 'Jewelry & Accessories', 'Home Appliances',
                                'Mobiles & Accessories', 'Electronics', 'Beauty & Health']

goodshongguan_front_cate_two = {
    '全类目': '总',
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
    'Beauty & Health': BeautyHealth,
    'Home': Home,

}

fig_goodshongguan_ca = []

layout = \
    html.Div([
        # html.Div([header(app)], ),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("**宏观监控**",
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
                        dcc.DatePickerRange(
                            id='date-picker-range-goodshongguan',
                            min_date_allowed=(datetime.datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d'),
                            max_date_allowed=(datetime.datetime.now() - timedelta(days=0)).strftime('%Y-%m-%d'),
                            start_date=(datetime.datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
                            end_date=(datetime.datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                            start_date_placeholder_text="Start Date",
                            end_date_placeholder_text="End Date",
                            calendar_orientation='vertical',
                            display_format="YY/M/D-Q-ωW-E",  # q ε
                            style={"height": "100%", "width": "150px", 'font-size': '9px'}
                        )
                    ],
                        className="padding-top-bot"
                    ),
                    html.Div([
                        html.Label("一级类目：",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        dcc.Dropdown(
                            id='goodshongguan-front-cate-one',
                            value='Home Appliances',
                            options=[{'label': v, 'value': v} for v in goodshongguan_front_cate_one],
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
                                id='goodshongguan-front-cate-two',
                                # multi=True
                                style={'width': '150px', 'font-family': 'Microsoft YaHei',
                                       'font-size': 8, 'textAlign': 'left'}
                            )
                        ],
                        id='goodshongguan-front-cate-two-container',
                        # className="padding-top-bot"
                    ),
                ],
                    className="bg-white user-control add_yingying"
                )
            ],
                className="two columns card"
            ),
            html.Div(
                buld_modal_info_overlay('goodshongguang', 'bottom', dedent(f"""
                一级类目：二级类目选择'总' - 展示销售top60数据\n
                二级类目：二级类目选择相应的类目 - 展示销售top30数据\n
                数据来源:商品日报\n
                指标可多选\n
                如页标签左上角"updating"提示，则数据正在加载，请耐心等待\n
                商品维度数据加载很慢，需要耐心等待哦~\n
                """)
                                        )
            ),

            html.Div([
                html.Div([
                    html.Div(
                        [
                            html.Div([
                                html.Label(
                                    id='show-goodshongguang-modal',
                                    children='�',
                                    n_clicks=0,
                                    className='info-icon',
                                    style={"font-size": "13px", 'color': 'rgb(240, 240, 240)'}
                                )
                            ], className="container_title"
                            ),
                            html.Label("指标选择-多选：",
                                       style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                              'font-family': 'Microsoft YaHei', 'font-size': 13}
                                       ),
                            dcc.Checklist(
                                id='goodshongguan_checklist',
                                options=[
                                    {"label": "商品等级", "value": "商品等级"},
                                    {"label": "曝光量", "value": "曝光量"},
                                    {"label": "点击数", "value": "点击数"},
                                    {"label": "加购数", "value": "加购数"},
                                    {"label": "收藏数", "value": "收藏数"},
                                    {"label": "商详访客", "value": "访客数"},
                                    {"label": "下单买家数", "value": "下单买家数"},
                                    {"label": "买家数", "value": "支付买家数"},
                                    {"label": "订单量", "value": "支付订单量"},
                                    {"label": "销量", "value": "支付件数"},
                                    {"label": "评分", "value": "历史评分"},
                                    {"label": "支付转化", "value": "支付转化率"},
                                ],
                                value=["历史评分"],
                                style={'font-family': 'Microsoft YaHei', 'font-size': 9},
                                # labelStyle={'display': 'inline-block'}
                            ),
                        ],
                        # className="padding-top-bot"
                    ),
                    html.Div(
                        [
                            html.Div(
                                id='my-graph-goodshongguan',
                            )
                        ],
                        className="padding-top-bot"
                    )
                ],
                    className="bg-white add_yingying"
                )
            ],
                className="ten columns card-left",
                id='goodshongguang-div'
            )
        ], className="row app-body"
        ),
    ],
    )

for id in ['goodshongguang']:
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
@app.callback(Output('goodshongguan-front-cate-two-container', 'children'),
              [Input('goodshongguan-front-cate-one', 'value')]
              )
def set_front_cate_two(goodshongguan_cate_one):
    goodshongguan_cate_two = goodshongguan_front_cate_two[goodshongguan_cate_one]
    return \
        html.Div([
            html.Label("二级类目：",
                       style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                              'font-family': 'Microsoft YaHei', 'font-size': 13}),
            dcc.Dropdown(
                id='goodshongguan-front-cate-two',
                value=goodshongguan_cate_two[0],
                options=[{'label': v, 'value': v} for v in goodshongguan_cate_two],
                style={'width': '150px', 'font-family': 'Microsoft YaHei',
                       'font-size': 8, 'textAlign': 'left'},
                # persistence_type='session',
                # persistence=cate_one,
                # multi=True
            )
        ],
            className="padding-top-bot"
        )


@app.callback(Output('my-graph-goodshongguan', 'children'),
              [Input('date-picker-range-goodshongguan', 'start_date'),
               Input('date-picker-range-goodshongguan', 'end_date'),
               Input('goodshongguan-front-cate-one', 'value'),
               Input('goodshongguan-front-cate-two', 'value'),
               Input('goodshongguan_checklist', 'value')
               ], )
def update_table_ca(start_date, end_date,
                    goodshongguan_cate_one, goodshongguan_cate_two_out,
                    category_check_auto_value):
    category_checklist = {
        "商品等级": '商品等级',
        "曝光量": '曝光量',
        "点击数": '点击数',
        "加购数": '加购数',
        "收藏数": '收藏数',
        "访客数": '访客数',
        "下单买家数": '下单买家数',
        "支付买家数": '支付买家数',
        "支付订单量": "支付订单量",
        "支付件数": '支付件数',
        "历史评分": "历史评分",
        "支付转化率": "支付转化率",
    }

    con_mssql_goodshongguan = pymssql.connect("172.16.92.2", "sa", "yssshushan2008", "CFflows", charset="utf8")

    global fig_goodshongguan_ca

    if goodshongguan_cate_two_out == '总':
        if category_check_auto_value == []:
            sql_goodshongguan_cateone = (f'''
            select top 60 货号, 前台一级类目,sum(支付金额) as 支付金额
            from CFgoodsday.dbo.商品数据
            WHERE 数据日期 BETWEEN '{start_date}' and '{end_date}' and 前台一级类目='{goodshongguan_cate_one}'
            GROUP BY 货号,前台一级类目
            ORDER BY sum(支付金额) DESC
            ''')
        else:
            sql_goodshongguan_cateone = (f'''
            select top 60 货号, 前台一级类目,sum(支付金额) as 支付金额,
            {', '.join('sum(' + category_checklist[n] + ') ' + 
                       category_checklist[n] for n in category_check_auto_value)}
            from CFgoodsday.dbo.商品数据
            WHERE 数据日期 BETWEEN '{start_date}' and '{end_date}' and 前台一级类目='{goodshongguan_cate_one}'
            GROUP BY 货号,前台一级类目
            ORDER BY sum(支付金额) DESC
            ''')

        # 数据处理
        data_goodshongguan_cateone = pd.read_sql(sql_goodshongguan_cateone, con_mssql_goodshongguan)
        print("打印商品宏观监控-一级类目数据")

        start_date1 = time.strptime(start_date, '%Y-%m-%d')
        end_date1 = time.strptime(end_date, '%Y-%m-%d')
        start_date1 = datetime.datetime(start_date1[0], start_date1[1], start_date1[2])
        end_date1 = datetime.datetime(end_date1[0], end_date1[1], end_date1[2])
        conment_days = (end_date1 - start_date1).days

        for s in category_check_auto_value:
            if s == '历史评分':
                data_goodshongguan_cateone['历史评分'] = \
                    (data_goodshongguan_cateone['历史评分'] / (conment_days + 1)).round(2)

        data_goodshongguan_cateone['支付金额'] = \
            (data_goodshongguan_cateone['支付金额'] / 1).round(1)

        fig_goodshongguan_ca = html.Div(
            [
                html.Div([
                    dash_table.DataTable(
                        page_current=0,
                        page_size=15,
                        data=data_goodshongguan_cateone.to_dict('records'),
                        columns=[{
                            'id': c, 'name': c}
                                    for c in ['货号', '前台一级类目']
                                ] +
                                [{
                                    'id': c, 'name': c,
                                } for c in ['支付金额'] + [category_checklist[n] for n in category_check_auto_value]
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

    else:
        if category_check_auto_value == []:
            sql_goodshongguan_catetwo = (f'''
            select top 30 货号, 前台一级类目,前台二级,sum(支付金额) as 支付金额
            from CFgoodsday.dbo.商品数据
            WHERE 数据日期 BETWEEN '{start_date}' 
            and '{end_date}' and 前台一级类目='{goodshongguan_cate_one}' 
            and 前台二级='{goodshongguan_cate_two_out}'
            GROUP BY 货号,前台一级类目,前台二级
            ORDER BY sum(支付金额) DESC
            ''')
        else:
            sql_goodshongguan_catetwo = (f'''
            select top 30 货号, 前台一级类目,前台二级,sum(支付金额) as 支付金额,
            {', '.join('sum(' + category_checklist[n] + ') ' + 
                       category_checklist[n] for n in category_check_auto_value)}
            from CFgoodsday.dbo.商品数据
            WHERE 数据日期 BETWEEN '{start_date}' 
            and '{end_date}' and 前台一级类目='{goodshongguan_cate_one}' 
            and 前台二级='{goodshongguan_cate_two_out}'
            GROUP BY 货号,前台一级类目,前台二级
            ORDER BY sum(支付金额) DESC
            ''')

        data_goodshongguan_catetwo = pd.read_sql(sql_goodshongguan_catetwo, con_mssql_goodshongguan)
        print("打印商品宏观监控-二级类目数据")

        start_date1 = time.strptime(start_date, '%Y-%m-%d')
        end_date1 = time.strptime(end_date, '%Y-%m-%d')
        start_date1 = datetime.datetime(start_date1[0], start_date1[1], start_date1[2])
        end_date1 = datetime.datetime(end_date1[0], end_date1[1], end_date1[2])
        conment_days = (end_date1 - start_date1).days

        for s in category_check_auto_value:
            if s == '历史评分':
                data_goodshongguan_catetwo['历史评分'] = \
                    (data_goodshongguan_catetwo['历史评分'] / (conment_days + 1)).round(2)

        data_goodshongguan_catetwo['支付金额'] = \
            (data_goodshongguan_catetwo['支付金额'] / 1).round(1)

        fig_goodshongguan_ca = html.Div(
            [
                html.Div([
                    dash_table.DataTable(
                        page_current=0,
                        page_size=15,
                        # page_action='custom',
                        data=data_goodshongguan_catetwo.to_dict('records'),
                        columns=[{
                            'id': c, 'name': c}
                                    for c in ['货号', '前台一级类目', '前台二级']
                                ] +
                                [{
                                    'id': c, 'name': c,
                                } for c in ['支付金额'] + [category_checklist[n] for n in category_check_auto_value]
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

    return fig_goodshongguan_ca

