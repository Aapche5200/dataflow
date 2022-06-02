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

sys.path.append('/Users/apache/PycharmProjects/shushan-CF/Dash+plotly/apps/projectone')
from appshudashboard import app
from layoutpage import header
from pages import AutoFetchGoods
from pages import AutoFetchFilter

print("AutoFetch-类目维度历史数据下载")

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

autofetch_front_cate_one = ['全类目', 'Women\'s Shoes', 'Women\'s Clothing', 'Women\'s Bags', 'Watches', 'Men\'s Shoes',
                            'Men\'s Clothing', 'Men\'s Bags', 'Jewelry & Accessories', 'Home Appliances', 'Home',
                            'Mobiles & Accessories', 'Electronics', 'Beauty & Health']

autofetch_front_cate_two = {
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

fig_auto_fetch_ca = []

layout = \
    html.Div([
        html.Div([header(app)], ),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("**类目维度下载**",
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
                            id='date-picker-range-cate-autofetch',
                            min_date_allowed=(datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d'),
                            max_date_allowed=(datetime.now() - timedelta(days=0)).strftime('%Y-%m-%d'),
                            start_date=(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
                            end_date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
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
                        html.Label("业务类型：",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        dcc.Dropdown(
                            id='my-dropdown-autofetch-ca',
                            options=[{'label': 'Cf', 'value': 'cf'},
                                     {'label': 'Seller', 'value': 'seller'},
                                     {'label': 'Total', 'value': 'total'}],
                            multi=False,
                            value='total',
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
                            id='autofetch-front-cate-one',
                            value='Home Appliances',
                            options=[{'label': v, 'value': v} for v in autofetch_front_cate_one],
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
                                id='autofetch-front-cate-two',
                                # multi=True
                                style={'width': '150px', 'font-family': 'Microsoft YaHei',
                                       'font-size': 8, 'textAlign': 'left'}
                            )
                        ],
                        id='autofetch-front-cate-two-container',
                        # className="padding-top-bot"
                    ),
                    html.Div([
                        html.A(html.Button("Download Data", id='data_autofetch_download_button',
                                           style={"width": '150px', 'font-family': 'Microsoft YaHei', 'font-size': 9}
                                           ),
                               id='autofetch-download-link',
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
                className="two columns card-top"
            ),
            html.Div([
                html.Div([
                    html.Div(
                        [
                            html.Label("指标选择-多选：",
                                       style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                              'font-family': 'Microsoft YaHei', 'font-size': 13}
                                       ),
                            dcc.Checklist(
                                id='autofetch_cate_checklist',
                                options=[{"label": "支付金额", "value": "支付金额"},
                                         {"label": "在售商品数", "value": "在售商品数"},
                                         {"label": "商详访客", "value": "独立访客"},
                                         {"label": "曝光数量", "value": "曝光数量"},
                                         {"label": "买家数", "value": "买家数"},
                                         {"label": "销量", "value": "支付商品件数"},
                                         {"label": "支付订单量", "value": "支付订单量"},
                                         {"label": "支付商品数", "value": "支付商品数"},
                                         ],
                                value=["支付金额"],
                                style={'font-family': 'Microsoft YaHei', 'font-size': 9},
                                # labelStyle={'display': 'inline-block'}
                            ),
                        ],
                        # className="padding-top-bot"
                    ),
                    html.Div(
                        [
                            dcc.Graph(
                                id='my-graph-autofetch-ca',
                                config={"displayModeBar": False},
                            )
                        ],
                        # className="padding-top-bot"
                    )
                ],
                    className="bg-white add_yingying"
                )
            ],
                className="ten columns card-left-top"
            )
        ], className="row app-body"
        ),
        html.Div(AutoFetchGoods.layout),
        html.Div(AutoFetchFilter.layout),
    ],
    )


# 二级下拉 回调数据
@app.callback(Output('autofetch-front-cate-two-container', 'children'),
              [Input('autofetch-front-cate-one', 'value')]
              )
def set_front_cate_two(autofetch_cate_one):
    autofetch_cate_two = autofetch_front_cate_two[autofetch_cate_one]
    return \
        html.Div([
            html.Label("二级类目：",
                       style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                              'font-family': 'Microsoft YaHei', 'font-size': 13}),
            dcc.Dropdown(
                id='autofetch-front-cate-two',
                value=autofetch_cate_two[0],
                options=[{'label': v, 'value': v} for v in autofetch_cate_two],
                style={'width': '150px', 'font-family': 'Microsoft YaHei',
                       'font-size': 8, 'textAlign': 'left'},
                # persistence_type='session',
                # persistence=cate_one,
                # multi=True
            )
        ],
            className="padding-top-bot"
        )


@app.callback([Output('my-graph-autofetch-ca', 'figure'), Output('autofetch-download-link', 'href')],
              [Input('date-picker-range-cate-autofetch', 'start_date'),
               Input('date-picker-range-cate-autofetch', 'end_date'),
               Input('my-dropdown-autofetch-ca', 'value'),
               Input('autofetch-front-cate-one', 'value'),
               Input('autofetch-front-cate-two', 'value'),
               Input('autofetch_cate_checklist', 'value')
               ], )
def update_table_ca(start_date, end_date,
                    selected_dropdown_value_autofetch_ca,
                    autofetch_cate_one, autofetch_cate_two_out,
                    category_check_auto_value):
    category_checklist = {'支付金额': '支付金额', '在售商品数': '在售商品数', '独立访客': '独立访客', '曝光数量': '曝光数量',
                          '买家数': '买家数', '支付商品件数': '支付商品件数', '支付订单量': '支付订单量',
                          '支付商品数': '支付商品数'
                          }

    con_mssql_autofetch = pymssql.connect("172.16.92.2", "sa", "yssshushan2008", "CFflows", charset="utf8")

    global fig_auto_fetch_ca

    if autofetch_cate_two_out == '总':

        # 一级取数 以下方法可以带入类目实现多选类目功能
        sql_auto_fetch_cateone = (f'''
        SELECT distinct 日期 ,一级类目 as 前台一级类目,业务类型 as 类型,
        {', '.join((category_checklist[n]) for n in category_check_auto_value)}
        from CFcategory.dbo.category_123_day
        WHERE (三级类目 = ' ' or 三级类目 is null) and (二级类目 = ' ' or 二级类目 is null) and 日期
        BETWEEN getdate()-90 and getdate()-1
        ''')
        # 数据处理
        data_auto_fetch_cateone = pd.read_sql(sql_auto_fetch_cateone, con_mssql_autofetch)
        print("打印atuofetch-类目维度-一级类目数据")

        data_auto_fetch_cateone.日期 = pd.to_datetime(data_auto_fetch_cateone.日期, format='%Y-%m-%d')
        data_auto_fetch_ca_cateone = data_auto_fetch_cateone[
            (data_auto_fetch_cateone.日期 >= start_date) & (data_auto_fetch_cateone.日期 <= end_date)]

        df_download_autofetch_cateone = data_auto_fetch_ca_cateone[
            data_auto_fetch_ca_cateone['类型'] == selected_dropdown_value_autofetch_ca]
        df_download_autofetch = \
            df_download_autofetch_cateone[df_download_autofetch_cateone['前台一级类目'] == autofetch_cate_one]
        csv_string_download_autofetch = df_download_autofetch.to_csv(index=False, encoding='utf-8')
        csv_string_download_autofetch = "data:text/csv;charset=utf-8," + urllib.parse.quote(
            csv_string_download_autofetch)

        data_auto_fetch_ca_cateone_fig = df_download_autofetch.head(10)

        fig_auto_fetch_ca = \
            go.Figure(data=[
                go.Table(
                    header=
                    dict(values=list(data_auto_fetch_ca_cateone_fig.columns),
                         fill_color='rgb(0, 81, 108)',
                         line=dict(color='rgb(0, 81, 108)', width=0.5),
                         align=['left'],
                         height=20,
                         font=dict(size=9, family='Microsoft YaHei', color='rgb(255, 255, 255)')),
                    # columnwidth=[10, 10, 4],
                    cells=
                    dict(
                        values=[
                            data_auto_fetch_ca_cateone_fig[h].tolist()
                            for h in data_auto_fetch_ca_cateone_fig.columns[0:]
                        ],
                        # height=18.5,
                        fill_color='rgb(255, 255, 255)',
                        line=dict(color='rgb(0, 81, 108)', width=0.5),
                        align=['left'],
                        font=dict(size=8, family='Microsoft YaHei'),
                        # suffix=(None, None, None, "w"),
                    )
                )
            ]
            )
        fig_auto_fetch_ca.update_layout(
            autosize=False,
            # width=1000,
            # height=615,  # 设置高度
            title=
            dict(text=
                 f"{start_date}--{end_date} "
                 f"{selected_dropdown_value_autofetch_ca} 下载数据TOP预览",
                 font=dict(family='Microsoft YaHei', size=13),
                 x=0.5, y=0.88)
        )

    else:
        # 二级取数 以下方法可以带入类目实现多选类目功能-二级类目
        sql_auto_fetch_cattwo = (f'''
        SELECT distinct 日期 ,一级类目 as 前台一级类目,二级类目 as 前台二级类目,业务类型 as 类型,
        {', '.join((category_checklist[m]) for m in category_check_auto_value)}
        from CFcategory.dbo.category_123_day
        WHERE (三级类目 = ' ' or 三级类目 is null) and (二级类目 != ' ' and 二级类目 is not null) and 日期
        BETWEEN getdate()-90 and getdate()-1
        ''')
        # 二级数据处理
        data_auto_fetch_catetwo = pd.read_sql(sql_auto_fetch_cattwo, con_mssql_autofetch)
        print("打印atuofetch-类目维度-二级类目数据")

        data_auto_fetch_catetwo.日期 = pd.to_datetime(data_auto_fetch_catetwo.日期, format='%Y-%m-%d')
        data_auto_fetch_ca_catetwo = data_auto_fetch_catetwo[
            (data_auto_fetch_catetwo.日期 >= start_date) & (data_auto_fetch_catetwo.日期 <= end_date)]

        df_download_autofetch_catetwo = data_auto_fetch_ca_catetwo[
            data_auto_fetch_ca_catetwo['类型'] == selected_dropdown_value_autofetch_ca]
        df_download_autofetch = \
            df_download_autofetch_catetwo[
                (df_download_autofetch_catetwo['前台一级类目'] == autofetch_cate_one) &
                (df_download_autofetch_catetwo['前台二级类目'] == autofetch_cate_two_out)]
        csv_string_download_autofetch = df_download_autofetch.to_csv(index=False, encoding='utf-8')
        csv_string_download_autofetch = "data:text/csv;charset=utf-8," + urllib.parse.quote(
            csv_string_download_autofetch)

        data_auto_fetch_ca_catetwo_fig = df_download_autofetch.head(10)

        fig_auto_fetch_ca = \
            go.Figure(data=[
                go.Table(
                    header=
                    dict(values=list(data_auto_fetch_ca_catetwo_fig.columns),
                         fill_color='rgb(0, 81, 108)',
                         line=dict(color='rgb(0, 81, 108)', width=0.5),
                         align=['left'],
                         height=20,
                         font=dict(size=9, family='Microsoft YaHei', color='rgb(255, 255, 255)')),
                    # columnwidth=[10, 8, 8, 3],
                    cells=
                    dict(
                        values=[
                            data_auto_fetch_ca_catetwo_fig[i].tolist()
                            for i in data_auto_fetch_ca_catetwo_fig.columns[0:]
                        ],
                        # height=18.5,
                        fill_color='rgb(255, 255, 255)',
                        line=dict(color='rgb(0, 81, 108)', width=0.5),
                        align=['left'],
                        font=dict(size=8, family='Microsoft YaHei'),
                        # suffix=(None, None, None, None, "w"),
                    )
                )
            ]
            )
        fig_auto_fetch_ca.update_layout(
            autosize=False,
            # width=1000,
            height=500,  # 设置高度
            title=
            dict(text=
                 f"{start_date}--{end_date} "
                 f"{selected_dropdown_value_autofetch_ca} 下载数据TOP预览",
                 font=dict(family='Microsoft YaHei', size=13),
                 x=0.5, y=0.88)
        )

    return fig_auto_fetch_ca, csv_string_download_autofetch
