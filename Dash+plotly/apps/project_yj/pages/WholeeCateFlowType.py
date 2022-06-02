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
import dash_table
from plotly.subplots import make_subplots  # 画子图加载包
import dash_auth
import dash_table
import dash
import sys
import urllib
import dash_table.FormatTemplate as FormatTemplate
from dash_table.Format import Format, Scheme, Sign, Symbol
from textwrap import dedent

sys.path.append(
    '/Users/apache/PycharmProjects/shushan-CF/Dash+plotly/apps/projectone')

from ConfigTag import buld_modal_info_overlay
from layoutpage import header
from appshudashboard import app

con_mssql = pymysql.connect("127.0.0.1",
                            "root",
                            "yssshushan2008",
                            "CFcategory",
                            charset="utf8")

# 可视化table取数
sql_table_ca = ('''
select
event_date as log_date,
front_cate_one,
首页点击uv as HomeUv,
category页点击uv as CateUv,
商详页点击uv DetailUv,
搜索结果页点击uv SearchUv,
首页销售 HomeGmv,
category页销售 CateGmv,
商详页销售 DetailGmv,
搜索结果页销售 SearchGmv,
首页cvr HomeCvr,
category页cvr CateCvr,
商详页cvr DetailCvr,
搜索结果页cvr as SearchCvr
from CFflows.CategoryPages
order by event_date DESC

''')

# 可视化table数据处理
data_table_ca = pd.read_sql(sql_table_ca, con_mssql)
print("打印WholeeDashCateSec-类目数据看板")

fig_table_ca = []
csv_string_download = []

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
                        style={'height': '43px', 'weight': '100%'},
                        className="bg-white user-control add_yingying"
                    )
                ],
                    className="five columns card-left-top"
                ),
                html.Div([
                    html.Div([
                        html.Div([
                            dcc.DatePickerRange(
                                id='date-picker-range-cate-yewuxian-ott-Wholee',
                                min_date_allowed=(
                                    datetime.now() -
                                    timedelta(
                                        days=90)).strftime('%Y-%m-%d'),
                                max_date_allowed=(
                                    datetime.now() -
                                    timedelta(
                                        days=0)).strftime('%Y-%m-%d'),
                                start_date=(
                                    datetime.now() -
                                    timedelta(
                                        days=1)).strftime('%Y-%m-%d'),
                                # updatemode="bothdates",
                                end_date=(
                                    datetime.now() -
                                    timedelta(
                                        days=1)).strftime('%Y-%m-%d'),
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
                        style={'height': '43px', 'weight': '100%'},
                        className="bg-white add_yingying"
                    ),
                ],
                    className="five columns card-left-top"
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
                                           download="类目分场景数据看板.csv",
                                           target="_blank",
                                           ),
                                ],
                                # className="padding-top-bot"
                            )
                        ],
                        style={'height': '43px', 'weight': '100%'},
                        className="bg-white add_yingying"
                    )
                ],
                    className="two columns card-left-top",
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
                UV逻辑：商品维度(.9.1)-点击的人数\n
                访客、销售、销量、曝光=统计时间内日累计数据\n
                环比=(本周-上周)/上周 - Cvr=支付买家数/曝光UV\n
                Cate:类目导航页\n
                日期可选范围：近90天\n
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
                            style={
                                "font-size": "13px",
                                'color': 'rgb(240, 240, 240)'}
                        )
                    ], className="container_title"
                    ),
                    html.Div(
                        id='my-graph-table-ca-ott-Wholee',
                        className="bg-white add_yingying"
                    )
                ],
                    className="one-all column card-left",
                    id="WholeeCateFlowType-div"
                )
            ], className="row app-body"
            ),
        ],
        )
    ])

for id in ['WholeeCateFlowType']:
    @app.callback([Output(f"{id}-modal", 'style'), Output(f"{id}-div", 'style')],
                  [Input(f'show-{id}-modal', 'n_clicks'),
                   Input(f'close-{id}-modal', 'n_clicks')])
    def toggle_modal(n_show, n_close):
        ctx = dash.callback_context
        if ctx.triggered and ctx.triggered[0]['prop_id'].startswith('show-'):
            return {"display": "block"}, {'zIndex': 1003}
        else:
            return {"display": "none"}, {'zIndex': 0}


@app.callback([Output('my-graph-table-ca-ott-Wholee',
                      'children'),
               Output('download-link-ott-Wholee',
                      'href')],
              [Input('date-picker-range-cate-yewuxian-ott-Wholee',
                     'start_date'),
               Input('date-picker-range-cate-yewuxian-ott-Wholee',
                     'end_date'),
               ],
              )
def update_table_ca(start_date, end_date, ):
    global fig_table_ca, csv_string_download
    data_table_ca.log_date = pd.to_datetime(
        data_table_ca.log_date, format='%Y-%m-%d')
    data_table_ca_now = data_table_ca[(data_table_ca.log_date >= start_date) & (
        data_table_ca.log_date <= end_date)]
    data_table_ca_past = \
        data_table_ca[(data_table_ca.log_date >=
                       (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=7))) &
                      (data_table_ca.log_date <=
                       (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=7)))]

    data_table_ca_now_sum = data_table_ca_now.groupby(
        by='front_cate_one', as_index=False).sum()

    data_table_ca_past_sum = data_table_ca_past.groupby(
        by='front_cate_one', as_index=False).sum()

    data_table_ca_past_sum.rename(
        columns={
            'HomeUv': 'HomeUv_past',
            'CateUv': 'CateUv_past',
            'DetailUv': 'DetailUv_past',
            'SearchUv': 'SearchUv_past',
            'HomeGmv': 'HomeGmv_past',
            'CateGmv': 'CateGmv_past',
            'DetailGmv': 'DetailGmv_past',
            'SearchGmv': 'SearchGmv_past',
            'HomeCvr': 'HomeCvr_past',
            'CateCvr': 'CateCvr_past',
            'DetailCvr': 'DetailCvr_past',
            'SearchCvr': 'SearchCvr_past',
        },
        inplace=True)

    data_table_ca_total = \
        pd.merge(data_table_ca_now_sum, data_table_ca_past_sum,
                 on=['front_cate_one'],
                 how='left')

    data_table_ca_total['HomeCvr_hb'] = \
        ((data_table_ca_total.HomeCvr - data_table_ca_total.HomeCvr_past) /
         data_table_ca_total.HomeCvr_past)
    data_table_ca_total['CateCvr_hb'] = \
        ((data_table_ca_total.CateCvr - data_table_ca_total.CateCvr_past) /
         data_table_ca_total.CateCvr_past)
    data_table_ca_total['DetailCvr_hb'] = \
        ((data_table_ca_total.DetailCvr - data_table_ca_total.DetailCvr_past) /
         data_table_ca_total.DetailCvr_past)
    data_table_ca_total['SearchCvr_hb'] = \
        ((data_table_ca_total.SearchCvr - data_table_ca_total.SearchCvr_past) /
         data_table_ca_total.SearchCvr_past)

    filter_data_table_ca_total = \
        data_table_ca_total[['front_cate_one', "HomeUv", "CateUv", "DetailUv", "SearchUv",
                             "HomeGmv", "CateGmv", "DetailGmv", "SearchGmv", "HomeCvr", "CateCvr",
                             "DetailCvr", "SearchCvr", "HomeCvr_hb", "CateCvr_hb",
                             "DetailCvr_hb", "SearchCvr_hb"]]

    filter_data_table_ca_total = filter_data_table_ca_total.sort_values(
        by='HomeGmv', ascending=False).head(15)  # 降序

    filter_data_table_ca_total.rename(columns={'front_cate_one': '类目',
                                               'HomeUv': '首页UV',
                                               'CateUv': 'CateUV',
                                               'DetailUv': '商详UV',
                                               'SearchUv': '搜索UV',
                                               'HomeGmv': '首页销售',
                                               'CateGmv': 'Cate销售',
                                               'DetailGmv': '商详销售',
                                               'SearchGmv': '搜索销售',
                                               'HomeCvr': '首页Cvr',
                                               'CateCvr': 'CateCvr',
                                               'DetailCvr': '商详Cvr',
                                               'SearchCvr': '搜索Cvr',
                                               'HomeCvr_hb': '首页Cvr环比%',
                                               'CateCvr_hb': 'CateCvr环比%',
                                               'DetailCvr_hb': '商详Cvr环比%',
                                               'SearchCvr_hb': '搜索Cvr环比%'},
                                      inplace=True)

    df_download = filter_data_table_ca_total
    csv_string_download = df_download.to_csv(index=False, encoding='utf-8')
    csv_string_download = "data:text/csv;charset=utf-8," + \
        urllib.parse.quote(csv_string_download)
    print("下载wholee分场景流量数据")
    filter_data_table_ca_total['首页销售'] = \
        (filter_data_table_ca_total['首页销售'] / 1).round(1)
    filter_data_table_ca_total['Cate销售'] = \
        (filter_data_table_ca_total['Cate销售'] / 1).round(1)
    filter_data_table_ca_total['商详销售'] = \
        (filter_data_table_ca_total['商详销售'] / 1).round(1)
    filter_data_table_ca_total['搜索销售'] = \
        (filter_data_table_ca_total['搜索销售'] / 1).round(1)

    filter_data_table_ca_total['首页Cvr环比'] = \
        filter_data_table_ca_total['首页Cvr环比%'].apply(lambda x: format(x, '.2%'))
    filter_data_table_ca_total['首页Cvr环比'] = \
        ['▲' + str(i) if i.find('-') else "▼" + str(i)
         for i in filter_data_table_ca_total['首页Cvr环比']]

    filter_data_table_ca_total['CateCvr环比'] = \
        filter_data_table_ca_total['CateCvr环比%'].apply(lambda x: format(x, '.2%'))
    filter_data_table_ca_total['CateCvr环比'] = \
        ['▲' + str(i) if i.find('-') else "▼" + str(i)
         for i in filter_data_table_ca_total['CateCvr环比']]

    filter_data_table_ca_total['商详Cvr环比'] = \
        filter_data_table_ca_total['商详Cvr环比%'].apply(lambda x: format(x, '.2%'))
    filter_data_table_ca_total['商详Cvr环比'] = \
        ['▲' + str(i) if i.find('-') else "▼" + str(i)
         for i in filter_data_table_ca_total['商详Cvr环比']]

    filter_data_table_ca_total['搜索Cvr环比'] = \
        filter_data_table_ca_total['搜索Cvr环比%'].apply(lambda x: format(x, '.2%'))
    filter_data_table_ca_total['搜索Cvr环比'] = \
        ['▲' + str(i) if i.find('-') else "▼" + str(i)
         for i in filter_data_table_ca_total['搜索Cvr环比']]

    fig_table_ca = html.Div(
        [
            html.Div([
                html.Label(f"{start_date}--{end_date} "
                           f"Wholee 类目场景数据看板")],
                     style={'textAlign': 'center', "font-family": "Microsoft YaHei",
                            "font-size": "15px", 'color': 'rgb(0, 81, 108)',
                            'fontWeight': 'bold'}),
            html.Div([
                dash_table.DataTable(
                    data=filter_data_table_ca_total.to_dict('records'),
                    columns=[{
                        'id': c, 'name': c}
                        for c in ['类目']
                    ] + [{
                        'id': c, 'name': c,
                        'type': 'numeric',
                                'format': Format(
                                    nully='N/A',
                                    precision=1,  # 设置保留位数
                                    scheme=Scheme.fixed,
                                    sign=Sign.parantheses,
                                    symbol=Symbol.yes,
                                    symbol_suffix=u''  # w
                                )
                    } for c in ['首页UV', 'CateUV', '商详UV', '搜索UV',
                                '首页销售', 'Cate销售', '商详销售', '搜索销售',
                                ]
                    ] + [{
                        'id': c, 'name': c,
                        'type': 'numeric',
                        'format': FormatTemplate.percentage(1)}
                        for c in ['首页Cvr', 'CateCvr', '商详Cvr', '搜索Cvr']
                    ] + [{
                        'id': c, 'name': c}
                        for c in ['首页Cvr环比', 'CateCvr环比', '商详Cvr环比', '搜索Cvr环比', ]
                    ],
                    style_header={
                        "font-family": "Microsoft YaHei",
                        "font-size": '10px',
                        "color": "rgb(255, 255, 255)",
                        'fontWeight': 'bold',
                        'textAlign': 'center',
                        'height': 'auto',
                        # 'whiteSpace': 'normal',
                        'backgroundColor': 'rgb(0, 81, 108)',
                        # 'borderColor': 'transparent',
                        'border': '0px',
                        'padding': '0px',
                    },
                    style_as_list_view=True,
                    style_cell={
                        "font-family": "Microsoft YaHei",
                        "font-size": '9px',
                        'padding': '0px',
                        # 'borderColor': 'transparent',
                        'border': '0px',
                        'textAlign': 'center',
                        # 'border-bottom': '1px dotted'
                    },
                    style_data={'whiteSpace': 'normal', 'height': 'auto',
                                'padding': '0px'},
                    style_header_conditional=[
                        {
                            'if': {'column_id': c},
                            'textAlign': 'left',
                        } for c in ['类目', '首页Cvr环比', 'CateCvr环比',
                                    '商详Cvr环比', '搜索Cvr环比',
                                    ]

                    ],
                    style_cell_conditional=[
                        {
                            'if': {'column_id': c},
                            'textAlign': 'left'
                        } for c in ['类目']] +
                    [
                        {
                            'if': {'column_id': c},
                            'textAlign': 'center',
                        } for c in ['首页UV', 'CateUV', '商详UV', '搜索UV',
                                    '首页销售', 'Cate销售', '商详销售', '搜索销售',
                                    '首页Cvr', 'CateCvr', '商详Cvr', '搜索Cvr']
                    ],
                    style_data_conditional=[
                        {
                            'if': {
                                'column_id': '首页Cvr环比',
                                'filter_query': '{首页Cvr环比%} > 0'
                            },
                            'textAlign': 'left',
                            'color': 'green',
                        },
                        {
                            'if': {
                                'column_id': '首页Cvr环比',
                                'filter_query': '{首页Cvr环比%} <= 0'
                            },
                            'textAlign': 'left',
                            'color': 'red',
                        }, {
                            'if': {
                                'column_id': 'CateCvr环比',
                                'filter_query': '{CateCvr环比%} > 0'
                            },
                            'textAlign': 'left',
                            'color': 'green',
                        },
                        {
                            'if': {
                                'column_id': 'CateCvr环比',
                                'filter_query': '{CateCvr环比%} <= 0'
                            },
                            'textAlign': 'left',
                            'color': 'red',
                        }, {
                            'if': {
                                'column_id': '商详Cvr环比',
                                'filter_query': '{商详Cvr环比%} > 0'
                            },
                            'textAlign': 'left',
                            'color': 'green',
                        },
                        {
                            'if': {
                                'column_id': '商详Cvr环比',
                                'filter_query': '{商详Cvr环比%} <= 0'
                            },
                            'textAlign': 'left',
                            'color': 'red',
                        }, {
                            'if': {
                                'column_id': '搜索Cvr环比',
                                'filter_query': '{搜索Cvr环比%} > 0'
                            },
                            'textAlign': 'left',
                            'color': 'green',
                        },
                        {
                            'if': {
                                'column_id': '搜索Cvr环比',
                                'filter_query': '{搜索Cvr环比%} <= 0'
                            },
                            'textAlign': 'left',
                            'color': 'red',
                        },
                        {
                            'if': {'row_index': 'odd'},
                            'backgroundColor': 'rgb(240, 240, 240)'
                        }
                    ]
                )
            ], style={'padding': '5px'}
            ),
        ]
    )

    return fig_table_ca, csv_string_download
