import pymysql
import pandas as pd  # 数据处理例如：读入，插入需要用的包
import numpy as np  # 平均值中位数需要用的包
import os  # 设置路径需要用的包
import psycopg2
import dash
from datetime import datetime, timedelta  # 设置当前时间及时间间隔计算需要用的包
import plotly.graph_objects as go
import plotly
import dash_core_components as dcc  # 交互式组件
import dash_html_components as html  # 代码转html
from dash.dependencies import Input, Output  # 回调
from flask import Flask, render_template
import dash_daq as daq
import plotly.express as px
from plotly.subplots import make_subplots  # 画子图加载包
import dash_auth
import sys
import urllib

sys.path.append('/Users/apache/PycharmProjects/shushan-CF/Dash+plotly/apps/projectone')
from appshudashboard import app

con_mssql = pymysql.connect("127.0.0.1",
                            "root",
                            "yssshushan2008",
                            "CFcategory",
                            charset="utf8")

# 可视化table取数
sql_animation_ca = ('''
SELECT month(日期) as yearmonth,业务类型,一级类目 as category_one,sum(支付金额) as 支付金额,
AVG(在售商品数) as 均在售商品数,sum(支付商品件数) as 销量, sum(独立访客) as 商详访客 from CFcategory.category_123_day
WHERE (二级类目 = ' ' or 二级类目 is null ) and 一级类目 !='全类目' and 日期 BETWEEN '2019-01-01' and '2019-12-31'
GROUP BY month(日期),业务类型,一级类目
HAVING sum(支付金额)  >=0

Union All

SELECT ((month(日期))+12) as yearmonth,业务类型,一级类目 as category_one,sum(支付金额) as 支付金额,
AVG(在售商品数) as 均在售商品数,sum(支付商品件数) as 销量, sum(独立访客) as 商详访客 from CFcategory.category_123_day
WHERE (二级类目 = ' ' or 二级类目 is null ) and 一级类目 !='全类目' and 日期 BETWEEN '2020-01-01' and '2020-12-31'
GROUP BY ((month(日期))+12),业务类型,一级类目
HAVING sum(支付金额)  >=0
ORDER BY yearmonth,业务类型,category_one
''')

# 可视化table数据处理
data_animation_ca = pd.read_sql(sql_animation_ca, con_mssql)
print("打印datadashanimation-类目数据看板")

data_animation_ca_tb = pd.read_csv('/Users/apache/Downloads/PythonDa/待插入数据.csv')
data_animation = data_animation_ca.append(data_animation_ca_tb, ignore_index=False)
data_animation = data_animation.sort_values(by='yearmonth', ascending=True)

# VALID_USERNAME_PASSWORD_PAIRS = {
#     '1': '1'
# }
#
# auth = dash_auth.BasicAuth(
#     app,
#     VALID_USERNAME_PASSWORD_PAIRS
#
# )

fig_animation_ca = []
layout = \
    html.Div([
        # html.Div([header(app)], ),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("业务类型：",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        dcc.Dropdown(
                            id='my-dropdown-animation-ca',
                            options=[{'label': 'Cf', 'value': 'cf'},
                                     {'label': 'Seller', 'value': 'seller'},
                                     {'label': 'Total', 'value': 'total'}],
                            multi=False,
                            value='total',
                            style={'width': '150px', 'font-family': 'Microsoft YaHei',
                                   'font-size': 9, 'textAlign': 'left'}
                        )
                    ],
                        className="padding-top-bot"
                    ),
                    html.Div([
                        html.Label("指标选择：",
                                   style={'textAlign': 'left', 'width': '100%', 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 13}
                                   ),
                        dcc.RadioItems(
                            id="animation-type-radio",
                            options=[
                                {"label": "销售", "value": "支付金额"},
                                {"label": "访客", "value": "商详访客",
                                 "disabled": True},
                                {"label": "销量", "value": "销量",
                                 "disabled": True},
                                {"label": "在售", "value": "均在售商品数",
                                 "disabled": True},
                            ],
                            value="支付金额",
                            labelStyle={'font-family': 'Microsoft YaHei', 'font-size': 9,
                                        'display': 'inline-block'}
                        )
                    ],
                        className="padding-top-bot"
                    ),
                ],
                    className="bg-white user-control add_yingying"
                )
            ],
                className="two columns card"
            ),
            html.Div([
                html.Div([
                    html.Div([
                        dcc.Graph(
                            id='my-graph-animation-ca',
                            config={"displayModeBar": False},
                        ),
                    ]
                    ),
                ],
                    className="bg-white add_yingying"
                )
            ],
                className="ten columns card-left"
            )
        ], className="row app-body"
        )
    ],
    )


@app.callback(
    Output('my-graph-animation-ca', 'figure'),
    [
        Input('my-dropdown-animation-ca', 'value'),
        Input("animation-type-radio", "value")
    ], )
def update_table_ca(selected_dropdown_value_animation_ca,
                    animation_type_radio
                    ):
    global fig_animation_ca

    data_animation_ca_filter = data_animation[data_animation['业务类型'] ==
                                              selected_dropdown_value_animation_ca]

    animation_data_qd = [
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Women\'s Clothing"]['yearmonth']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Women\'s Clothing"][f'{animation_type_radio}']),
            mode='lines',
            name="女装",
            line=dict(width=2, color="rgba(235, 12, 25, 0.01)")),
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Men\'s Clothing"]['yearmonth']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Men\'s Clothing"][f'{animation_type_radio}']),
            mode='lines',
            name="男装",
            line=dict(width=2, color="rgba(234, 143, 116, 0.01)")),
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Jewelry & Accessories"]['yearmonth']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Jewelry & Accessories"][f'{animation_type_radio}']),
            mode='lines',
            name="配饰",
            line=dict(width=2, color="rgba(122, 37, 15, 0.01)")),
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Mobiles & Accessories"]['yearmonth']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Mobiles & Accessories"][f'{animation_type_radio}']),
            mode='lines',
            name="手机配件",
            line=dict(width=2, color="rgba(0, 81, 108, 0.01)")),
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Home Appliances"]['yearmonth']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Home Appliances"][f'{animation_type_radio}']),
            mode='lines',
            name="小家电",
            line=dict(width=2, color="rgba(93, 145, 167, 0.01)")),
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Women\'s Shoes"]['yearmonth']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Women\'s Shoes"][f'{animation_type_radio}']),
            mode='lines',
            name="女鞋",
            line=dict(width=2, color="rgba(0, 164, 220, 0.01)")),
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Beauty & Health"]['yearmonth']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Beauty & Health"][f'{animation_type_radio}']),
            mode='lines',
            name="美妆",
            line=dict(width=2, color="rgba(107, 207, 246, 0.01)")),
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Electronics"]['yearmonth']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Electronics"][f'{animation_type_radio}']),
            mode='lines',
            name="3C",
            line=dict(width=2, color="rgba(0, 137, 130, 0.01)")),
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Watches"]['yearmonth']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Watches"][f'{animation_type_radio}']),
            mode='lines',
            name="手表",
            line=dict(width=2, color="rgba(109, 187, 191, 0.01)")),
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Home"]['yearmonth']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Home"][f'{animation_type_radio}']),
            mode='lines',
            name="家居",
            line=dict(width=2, color="rgba(205, 221, 230, 0.01)"))
    ]

    animation_data_hd = [
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Women\'s Clothing"]['yearmonth']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Women\'s Clothing"][f'{animation_type_radio}']),
            mode='lines',
            name="女装",
            line=dict(width=2, color="rgba(235, 12, 25, 0.01)")),
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Men\'s Clothing"][f'{animation_type_radio}']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Men\'s Clothing"][f'{animation_type_radio}']),
            mode='lines',
            name="男装",
            line=dict(width=2, color="rgba(234, 143, 116, 0.01)")),
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Jewelry & Accessories"]['yearmonth']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Jewelry & Accessories"][f'{animation_type_radio}']),
            mode='lines',
            name="配饰",
            line=dict(width=2, color="rgba(122, 37, 15, 0.01)")),
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Mobiles & Accessories"]['yearmonth']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Mobiles & Accessories"][f'{animation_type_radio}']),
            mode='lines',
            name="手机配件",
            line=dict(width=2, color="rgba(0, 81, 108, 0.01)")),
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Home Appliances"]['yearmonth']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Home Appliances"][f'{animation_type_radio}']),
            mode='lines',
            name="小家电",
            line=dict(width=2, color="rgba(93, 145, 167, 0.01)")),
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Women\'s Shoes"]['yearmonth']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Women\'s Shoes"][f'{animation_type_radio}']),
            mode='lines',
            name="女鞋",
            line=dict(width=2, color="rgba(0, 164, 220, 0.01)")),
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Beauty & Health"]['yearmonth']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Beauty & Health"][f'{animation_type_radio}']),
            mode='lines',
            name="美妆",
            line=dict(width=2, color="rgba(107, 207, 246, 0.01)")),
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Electronics"]['yearmonth']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Electronics"][f'{animation_type_radio}']),
            mode='lines',
            name="3C",
            line=dict(width=2, color="rgba(0, 137, 130, 0.01)")),
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Watches"]['yearmonth']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Watches"][f'{animation_type_radio}']),
            mode='lines',
            name="手表",
            line=dict(width=2, color="rgba(109, 187, 191, 0.01)")),
        dict(
            x=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Home"]['yearmonth']),
            y=(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                        "Home"][f'{animation_type_radio}']),
            mode='lines',
            name="家居",
            line=dict(width=2, color="rgba(205, 221, 230, 0.01)"))
    ]

    animation_data = animation_data_qd + animation_data_hd

    ANM_trace_frames = [dict(data=[
        dict(
            x=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Women\'s Clothing"]['yearmonth'])[k]],
            y=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Women\'s Clothing"][f'{animation_type_radio}'])[k]],
            mode='lines+markers+text',
            textposition="middle right",
            textfont=dict(family="Microsoft YaHei", size=8,
                          color='rgb(235, 12, 25)'),
            text="女装" + str(round((list(
                data_animation_ca_filter[
                    data_animation_ca_filter["category_one"] ==
                    "Women\'s Clothing"][f'{animation_type_radio}'])[k]) / 10000, 1)) + "w",
            marker=dict(color='rgb(235, 12, 25)', size=13,
                        symbol="circle"
                        )
        ),
        dict(
            x=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Men\'s Clothing"]['yearmonth'])[k]],
            y=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Men\'s Clothing"][f'{animation_type_radio}'])[k]],
            mode='markers+text',
            textposition="middle right",
            textfont=dict(family="Microsoft YaHei", size=8,
                          color='rgb(234, 143, 116)'),
            text="男装" + str(round((list(
                data_animation_ca_filter[
                    data_animation_ca_filter["category_one"] ==
                    "Men\'s Clothing"][f'{animation_type_radio}'])[k]) / 10000, 1)) + "w",
            marker=dict(color='rgb(234, 143, 116)', size=13,
                        symbol="circle-dot"
                        )
        ),
        dict(
            x=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Jewelry & Accessories"]['yearmonth'])[k]],
            y=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Jewelry & Accessories"][f'{animation_type_radio}'])[k]],
            mode='markers+text',
            textposition="middle right",
            textfont=dict(family="Microsoft YaHei", size=8,
                          color='rgb(122, 37, 15)'),
            text="配饰" + str(round((list(
                data_animation_ca_filter[
                    data_animation_ca_filter["category_one"] ==
                    "Jewelry & Accessories"][f'{animation_type_radio}'])[k]) / 10000, 1)) + "w",
            marker=dict(color='rgb(122, 37, 15)', size=13,
                        symbol="circle"
                        )
        ),
        dict(
            x=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Mobiles & Accessories"]['yearmonth'])[k]],
            y=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Mobiles & Accessories"][f'{animation_type_radio}'])[k]],
            mode='markers+text',
            textposition="middle left",
            textfont=dict(family="Microsoft YaHei", size=8,
                          color='rgb(0, 81, 108)'),
            text="MAC" + str(round((list(
                data_animation_ca_filter[
                    data_animation_ca_filter["category_one"] ==
                    "Mobiles & Accessories"][f'{animation_type_radio}'])[k]) / 10000, 1)) + "w",
            marker=dict(color='rgb(0, 81, 108)', size=13,
                        symbol="circle"
                        )
        ),
        dict(
            x=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Home Appliances"]['yearmonth'])[k]],
            y=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Home Appliances"][f'{animation_type_radio}'])[k]],
            mode='markers+text',
            textposition="middle right",
            textfont=dict(family="Microsoft YaHei", size=8,
                          color='rgb(93, 145, 167)'),
            text="小家电" + str(round((list(
                data_animation_ca_filter[
                    data_animation_ca_filter["category_one"] ==
                    "Home Appliances"][f'{animation_type_radio}'])[k]) / 10000, 1)) + "w",
            marker=dict(color='rgb(93, 145, 167)', size=13,
                        symbol="circle-dot"
                        )
        ),
        dict(
            x=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Women\'s Shoes"]['yearmonth'])[k]],
            y=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Women\'s Shoes"][f'{animation_type_radio}'])[k]],
            mode='markers+text',
            textposition="middle right",
            textfont=dict(family="Microsoft YaHei", size=8,
                          color='rgb(0, 164, 220)'),
            text="女鞋" + str(round((list(
                data_animation_ca_filter[
                    data_animation_ca_filter["category_one"] ==
                    "Women\'s Shoes"][f'{animation_type_radio}'])[k]) / 10000, 1)) + "w",
            marker=dict(color='rgb(0, 164, 220)', size=13,
                        symbol="circle-open"
                        )
        ),
        dict(
            x=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Beauty & Health"]['yearmonth'])[k]],
            y=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Beauty & Health"][f'{animation_type_radio}'])[k]],
            mode='markers+text',
            textposition="middle left",
            textfont=dict(family="Microsoft YaHei", size=8,
                          color='rgb(107, 207, 246)'),
            text="美妆" + str(round((list(
                data_animation_ca_filter[
                    data_animation_ca_filter["category_one"] ==
                    "Beauty & Health"][f'{animation_type_radio}'])[k]) / 10000, 1)) + "w",
            marker=dict(color='rgb(107, 207, 246)', size=13,
                        symbol="circle-dot"
                        )
        ),
        dict(
            x=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Electronics"]['yearmonth'])[k]],
            y=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Electronics"][f'{animation_type_radio}'])[k]],
            mode='markers+text',
            textposition="middle right",
            textfont=dict(family="Microsoft YaHei", size=8,
                          color='rgb(0, 137, 130)'),
            text="3C" + str(round((list(
                data_animation_ca_filter[
                    data_animation_ca_filter["category_one"] ==
                    "Electronics"][f'{animation_type_radio}'])[k]) / 10000, 1)) + "w",
            marker=dict(color='rgb(0, 137, 130)', size=13,
                        symbol="circle-open-dot"
                        )
        ),
        dict(
            x=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Watches"]['yearmonth'])[k]],
            y=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Watches"][f'{animation_type_radio}'])[k]],
            mode='markers+text',
            textposition="middle left",
            textfont=dict(family="Microsoft YaHei", size=8,
                          color='rgb(109, 187, 191)'),
            text="手表" + str(round((list(
                data_animation_ca_filter[
                    data_animation_ca_filter["category_one"] ==
                    "Watches"][f'{animation_type_radio}'])[k]) / 10000, 1)) + "w",
            marker=dict(color='rgb(109, 187, 191)', size=13,
                        symbol="circle-dot"
                        )
        ),
        dict(
            x=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Home"]['yearmonth'])[k]],
            y=[list(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                             "Home"][f'{animation_type_radio}'])[k]],
            mode='markers+text',
            textposition="middle left",
            textfont=dict(family="Microsoft YaHei", size=8,
                          color='rgb(205, 221, 230)'),
            text="家居" + str(round((list(
                data_animation_ca_filter[
                    data_animation_ca_filter["category_one"] ==
                    "Home"][f'{animation_type_radio}'])[k]) / 10000, 1)) + "w",
            marker=dict(color='rgb(205, 221, 230)', size=13,
                        symbol="circle-open"
                        )
        )
    ]) for k in range(len(data_animation_ca_filter[data_animation_ca_filter["category_one"] ==
                                                   "Women\'s Clothing"]['yearmonth']))]

    fig_animation_ca = {
        "data": animation_data,
        "frames": ANM_trace_frames,
        "layout": go.Layout(
            title={"text": f"19-20年类目#{animation_type_radio}#月递增动态变化趋势",
                   'font': {'family': 'Microsoft YaHei',
                            'size': 13
                            },
                   "xanchor": 'left',
                   "yanchor": 'top',
                   "xref": 'container',
                   "yref": 'container',
                   "x": 0.35,
                   "y": 0.95
                   },
            height=450,
            showlegend=False,
            xaxis={"title": {"text": "Date（月）",
                             'font': {'family': 'Microsoft YaHei',
                                      'size': 10
                                      }
                             },
                   "tickangle": -45,
                   # "type": "date",
                   "showline": True,
                   "linecolor": 'rgba(204, 204, 204, 0.1)',
                   "linewidth": 2,
                   "showgrid": False,
                   "zeroline": False,
                   "range": [np.min(data_animation_ca_filter[
                                        data_animation_ca_filter["category_one"] ==
                                        "Women\'s Clothing"]['yearmonth']) - 1,
                             np.max(data_animation_ca_filter[
                                        data_animation_ca_filter["category_one"] ==
                                        "Women\'s Clothing"]['yearmonth']) + 1],
                   "autorange": False,
                   "tickmode": "array",
                   "tickvals": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22],
                   "ticktext": ["0",
                                "201901", "201902", "201903",
                                "201904", "201905", "201906",
                                "201907", "201908", "201909",
                                "201910", "201911", "201912",
                                "202001", "202002", "202003",
                                "202004", "202005", "202006",
                                "202007", "202008", "202009",
                                ],
                   'tickfont': {'family': 'Microsoft YaHei',
                                'size': 9
                                }
                   },
            yaxis={"title": {"text": f"{animation_type_radio}",
                             'font': {'family': 'Microsoft YaHei',
                                      'size': 10
                                      }
                             },
                   "showline": False,
                   "showgrid": False,
                   "zeroline": False,
                   "range": [np.min(data_animation_ca_filter[f'{animation_type_radio}']) - 10000,
                             np.max(data_animation_ca_filter[f'{animation_type_radio}']) + 100000],
                   "autorange": False,
                   'tickfont': {'family': 'Microsoft YaHei',
                                'size': 9
                                }
                   },
            hovermode="closest",
            updatemenus=[{"type": "buttons",
                          "buttons": [{
                              "label": "Play",
                              "method": "animate",
                              "args": [None, {
                                  'frame': {'duration': 1500,
                                            'redraw': True
                                            },
                                  'fromcurrent': True,
                                  'transition':
                                      {'duration': 1100,
                                       'easing': 'linear',
                                       # linear cubic-in-out
                                       }
                              }
                                       ]}, {
                              'args': [[None], {
                                  'frame': {'duration': 100,
                                            'redraw': True
                                            },
                                  'mode': 'immediate',
                                  'transition': {'duration': 50}}
                                       ],
                              'label': 'Pause',
                              'method': 'animate'
                          },
                          ],
                          "direction": "left",
                          "x": 0.15,
                          "y": 0.98
                          }
                         ]
        ),
    }

    return fig_animation_ca
