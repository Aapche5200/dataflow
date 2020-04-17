import pymssql
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
import dash
from plotly.subplots import make_subplots  # 画子图加载包
import dash_auth
import dash_table
import sys
import urllib
import dash_table.FormatTemplate as FormatTemplate
from dash_table.Format import Format, Scheme, Sign, Symbol

sys.path.append('/Users/apache/PycharmProjects/shushan-CF/Dash+plotly/apps/projectone')
from appshudashboard import app
from layoutpage import header

con_mssql = pymssql.connect("172.16.92.2", "sa", "yssshushan2008", "CFflows", charset="utf8")

# 可视化table取数
sql_table_ca = ('''
SELECT a.log_date,a.front_cate_one,a.yewu_type,a.uv,a.gmv,a.pv,a.qty,a.item_num,a.pay_user_nums,
b.gmv as total_gmv,c.pv as total_pv, b.item_num as total_item_num
from (
SELECT 日期 as log_date,一级类目 front_cate_one,业务类型 yewu_type,SUM(独立访客) as uv,sum(支付金额) as gmv,
sum(曝光数量) as pv,sum(支付商品件数) as qty,AVG(在售商品数) as item_num,sum(买家数) as pay_user_nums
from CFcategory.dbo.category_123_day
WHERE (三级类目 = ' ' or 三级类目 is null) and (二级类目 = ' ' or 二级类目 is null)
GROUP BY 日期 ,一级类目,业务类型
) as a
LEFT JOIN
(
SELECT 日期 as log_date,一级类目 front_cate_one,业务类型 yewu_type,SUM(独立访客) as uv,sum(支付金额) as gmv,
sum(曝光数量) as pv,sum(支付商品件数) as qty,avg(支付转化率 ) as cvr,AVG(在售商品数) as item_num,sum(买家数) as pay_user_nums
from CFcategory.dbo.category_123_day
WHERE (三级类目 = ' ' or 三级类目 is null) and (二级类目 = ' ' or 二级类目 is null) and 一级类目='全类目'
GROUP BY 日期 ,一级类目,业务类型
) as b on a.log_date=b.log_date and a.yewu_type=b.yewu_type
LEFT JOIN
(
SELECT 日期 as log_date,业务类型 yewu_type,SUM(独立访客) as uv,sum(支付金额) as gmv,
sum(曝光数量) as pv,sum(支付商品件数) as qty,avg(支付转化率 ) as cvr,AVG(在售商品数) as item_num,(sum(支付金额)/sum(买家数)) as kedan_prices
from CFcategory.dbo.category_123_day
WHERE (三级类目 = ' ' or 三级类目 is null) and (二级类目 = ' ' or 二级类目 is null) and 一级类目!='全类目'
GROUP BY 日期 ,业务类型
) as c on a.log_date=c.log_date and a.yewu_type=c.yewu_type
ORDER BY a.log_date,a.front_cate_one,a.yewu_type
''')

# 可视化table数据处理
data_table_ca = pd.read_sql(sql_table_ca, con_mssql)
print("打印datadashboard-类目数据看板")

fig_table_ca = []

app = dash.Dash()

app.layout = \
    html.Div([
        html.Div([header(app)], ),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("日期：",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        html.Br(),
                        dcc.DatePickerRange(
                            id='date-picker-range-cate-yewuxian-test',
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
                            id='my-dropdown-table-ca-test',
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
                        html.A(html.Button("Download Data", id='data_download_button',
                                           style={"width": '150px', 'font-family': 'Microsoft YaHei', 'font-size': 9}
                                           ),
                               id='download-link-test',
                               href="",
                               download="类目数据看板.csv",
                               target="_blank",
                               ),
                    ],
                        className="padding-top-bot"
                    )
                ],
                    className="bg-white user-control"
                )
            ],
                className="two columns card-top"
            ),
            html.Div([
                html.Div(
                    id='my-graph-table-ca-test',
                    className="bg-white"
                )
            ],
                className="ten columns card-left-top"
            )
        ], className="row app-body"
        ),
    ],
    )


@app.callback([Output('my-graph-table-ca-test', 'children'), Output('download-link-test', 'href')],
              [Input('date-picker-range-cate-yewuxian-test', 'start_date'),
               Input('date-picker-range-cate-yewuxian-test', 'end_date'),
               Input('my-dropdown-table-ca-test', 'value')], )
def update_table_ca(start_date, end_date, selected_dropdown_value_table_ca):
    global fig_table_ca
    data_table_ca.log_date = pd.to_datetime(data_table_ca.log_date, format='%Y-%m-%d')
    data_table_ca_now = data_table_ca[(data_table_ca.log_date >= start_date) & (data_table_ca.log_date <= end_date)]
    data_table_ca_past = \
        data_table_ca[(data_table_ca.log_date >=
                       (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=7))) &
                      (data_table_ca.log_date <=
                       (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=7)))]
    # now数据处理
    data_table_ca_now_item_num_mean = \
        data_table_ca_now.groupby(by=['front_cate_one', 'yewu_type'], as_index=False).item_num.mean()
    data_table_ca_now_item_num_mean.rename(columns={'item_num': 'item_num_mean'}, inplace=True)
    data_table_ca_now_sum = data_table_ca_now.groupby(by=['front_cate_one', 'yewu_type'], as_index=False).sum()
    data_table_ca_now_total = \
        pd.merge(data_table_ca_now_sum, data_table_ca_now_item_num_mean, on=['front_cate_one', 'yewu_type'])

    data_table_ca_now_total['cvr'] = \
        (data_table_ca_now_total.pay_user_nums / data_table_ca_now_total.uv)
    data_table_ca_now_total['kd_price'] = data_table_ca_now_total.gmv / data_table_ca_now_total.pay_user_nums

    # past数据处理
    data_table_ca_past_item_num_mean = \
        data_table_ca_past.groupby(by=['front_cate_one', 'yewu_type'], as_index=False).item_num.mean()
    data_table_ca_past_item_num_mean.rename(columns={'item_num': 'item_num_mean'}, inplace=True)
    data_table_ca_past_sum = data_table_ca_past.groupby(by=['front_cate_one', 'yewu_type'], as_index=False).sum()
    data_table_ca_past_total = \
        pd.merge(data_table_ca_past_sum, data_table_ca_past_item_num_mean, on=['front_cate_one', 'yewu_type'])

    data_table_ca_past_total['cvr_past'] = data_table_ca_past_total.pay_user_nums / data_table_ca_past_total.uv
    data_table_ca_past_total['kd_price_past'] = data_table_ca_past_total.gmv / data_table_ca_past_total.pay_user_nums

    data_table_ca_past_total.rename(columns={'uv': 'uv_past', 'gmv': 'gmv_past', 'pv': 'pv_past', 'qty': 'qty_past',
                                             'item_num': 'item_num_past', 'pay_user_nums': 'pay_user_nums_past',
                                             'total_gmv': 'total_gmv_past', 'total_pv': 'total_pv_past',
                                             'total_item_num': 'total_item_num_past',
                                             'item_num_mean': 'item_num_mean_past'}, inplace=True)

    # now and past 数据合并
    data_table_ca_total = \
        pd.merge(data_table_ca_now_total, data_table_ca_past_total, on=['front_cate_one', 'yewu_type'])
    print("表格now and past 数据合并")

    # 求环比 & 占比
    data_table_ca_total['uv_hb'] = \
        ((data_table_ca_total.uv - data_table_ca_total.uv_past) /
         data_table_ca_total.uv_past)
    data_table_ca_total['gmv_hb'] = \
        ((data_table_ca_total.gmv - data_table_ca_total.gmv_past) /
         data_table_ca_total.gmv_past)
    data_table_ca_total['qty_hb'] = \
        ((data_table_ca_total.qty - data_table_ca_total.qty_past) /
         data_table_ca_total.qty_past)
    data_table_ca_total['item_num_hb'] = \
        ((data_table_ca_total.item_num_mean - data_table_ca_total.item_num_mean_past) /
         data_table_ca_total.item_num_mean_past)
    data_table_ca_total['cvr_hb'] = \
        ((data_table_ca_total.cvr - data_table_ca_total.cvr_past) /
         data_table_ca_total.cvr_past)
    data_table_ca_total['kd_price_hb'] = \
        ((data_table_ca_total.kd_price - data_table_ca_total.kd_price_past) /
         data_table_ca_total.kd_price_past)

    data_table_ca_total['pv_zb'] = \
        (data_table_ca_total.pv / data_table_ca_total.total_pv)
    data_table_ca_total['gmv_zb'] = \
        (data_table_ca_total.gmv / data_table_ca_total.total_gmv)
    data_table_ca_total['item_num_zb'] = \
        (data_table_ca_total.item_num / data_table_ca_total.total_item_num)
    print("表格环比及占比数据打印")

    filter_data_table_ca_total = data_table_ca_total[['yewu_type', 'front_cate_one', 'uv', 'gmv', 'qty', 'cvr',
                                                      'kd_price', 'item_num_mean', 'pv_zb', 'gmv_zb', 'item_num_zb',
                                                      'uv_hb', 'gmv_hb', 'qty_hb', 'cvr_hb', 'kd_price_hb',
                                                      'item_num_hb']]

    filter_data_table_ca_total = filter_data_table_ca_total.sort_values(by='gmv', ascending=False).head(50)  # 降序

    filter_data_table_ca_total.rename(columns={'yewu_type': '类型', 'front_cate_one': '类目', 'uv': '访客',
                                               'gmv': '销售', 'qty': '销量', 'cvr': '支付转化',
                                               'kd_price': '客单价', 'item_num_mean': '在售商品数', 'pv_zb': '曝光占比',
                                               'gmv_zb': '销售占比',
                                               'item_num_zb': '在售占比',
                                               'uv_hb': '访客环比%', 'gmv_hb': '销售环比%', 'qty_hb': '销量环比%',
                                               'cvr_hb': '转化环比%', 'kd_price_hb': '客单价环比%',
                                               'item_num_hb': '在售环比%'}, inplace=True)
    print("表格最终作图数据打印")

    df_download = filter_data_table_ca_total[filter_data_table_ca_total['类型'] == selected_dropdown_value_table_ca]
    csv_string_download = df_download.to_csv(index=False, encoding='utf-8')
    csv_string_download = "data:text/csv;charset=utf-8," + urllib.parse.quote(csv_string_download)

    filter_data_table_ca_total['访客'] = \
        (filter_data_table_ca_total['访客'] / 10000).round(1)
    filter_data_table_ca_total['销售'] = \
        (filter_data_table_ca_total['销售'] / 10000).round(1)
    filter_data_table_ca_total['销量'] = \
        (filter_data_table_ca_total['销量'] / 10000).round(1)
    filter_data_table_ca_total['在售商品数'] = \
        (filter_data_table_ca_total['在售商品数'] / 10000).round(1)
    filter_data_table_ca_total['客单价'] = \
        (filter_data_table_ca_total['客单价'] / 1).round(2)

    filter_data_table_ca_total['访客环比'] = filter_data_table_ca_total['访客环比%'].apply(lambda x: format(x, '.2%'))
    filter_data_table_ca_total['访客环比'] = ['▲' + str(i) if i.find('-') else "▼" + str(i)
                                          for i in filter_data_table_ca_total['访客环比']]

    filter_data_table_ca_total['销售环比'] = filter_data_table_ca_total['销售环比%'].apply(lambda x: format(x, '.2%'))
    filter_data_table_ca_total['销售环比'] = ['▲' + str(i) if i.find('-') else "▼" + str(i)
                                          for i in filter_data_table_ca_total['销售环比']]

    filter_data_table_ca_total['销量环比'] = filter_data_table_ca_total['销量环比%'].apply(lambda x: format(x, '.2%'))
    filter_data_table_ca_total['销量环比'] = ['▲' + str(i) if i.find('-') else "▼" + str(i)
                                          for i in filter_data_table_ca_total['销量环比']]

    filter_data_table_ca_total['转化环比'] = filter_data_table_ca_total['转化环比%'].apply(lambda x: format(x, '.2%'))
    filter_data_table_ca_total['转化环比'] = ['▲' + str(i) if i.find('-') else "▼" + str(i)
                                          for i in filter_data_table_ca_total['转化环比']]

    filter_data_table_ca_total['客单价环比'] = filter_data_table_ca_total['客单价环比%'].apply(lambda x: format(x, '.2%'))
    filter_data_table_ca_total['客单价环比'] = ['▲' + str(i) if i.find('-') else "▼" + str(i)
                                           for i in filter_data_table_ca_total['客单价环比']]

    filter_data_table_ca_total['在售环比'] = filter_data_table_ca_total['在售环比%'].apply(lambda x: format(x, '.2%'))
    filter_data_table_ca_total['在售环比'] = ['▼' + str(i) if i.find('-') else "▼" + str(i)
                                          for i in filter_data_table_ca_total['在售环比']]

    fig_table_ca = html.Div(
        [
            html.Div([
                html.Label(f"{start_date}--{end_date} "
                           f"{selected_dropdown_value_table_ca} 类目数据看板")],
                style={'textAlign': 'center', "font-family": "Microsoft YaHei",
                       "font-size": "15px", 'color': 'rgb(0, 81, 108)',
                       'fontWeight': 'bold'}),
            html.Div([
                dash_table.DataTable(
                    data=filter_data_table_ca_total[filter_data_table_ca_total['类型'] ==
                                                    selected_dropdown_value_table_ca].to_dict('records'),
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
                            symbol_suffix=u'w'
                        )
                    } for c in ['访客', '销售', '销量', '在售商品数']
                            ] + [{
                        'id': c, 'name': c}
                                for c in ['客单价']
                            ] + [{
                        'id': c, 'name': c,
                        'type': 'numeric',
                        'format': FormatTemplate.percentage(1)}
                                for c in ['支付转化', '曝光占比', '销售占比', '在售占比']
                            ] + [{
                        'id': c, 'name': c}
                                for c in ['访客环比', '销售环比', '销量环比', '转化环比',
                                          '客单价环比', '在售环比']
                            ],
                    style_header={"font-family": "Microsoft YaHei", "font-size": '9px',
                                  "color": "rgb(255, 255, 255)", 'fontWeight': 'bold',
                                  'textAlign': 'center', 'height': 'auto',
                                  # 'whiteSpace': 'normal',
                                  'backgroundColor': 'rgb(0, 81, 108)',
                                  'border': '0px dashed rgb(0, 81, 108)'},
                    style_as_list_view=True,
                    style_cell={"font-family": "Microsoft YaHei", "font-size": '8px',
                                'border': '0px',
                                'border-bottom': '1px dashed rgb(0, 81, 108)'},
                    style_data={'whiteSpace': 'normal', 'height': 'auto'},
                    style_header_conditional=
                    [
                        {
                            'if': {'column_id': c},
                            'textAlign': 'left',
                        } for c in ['类目', '访客环比', '销售环比',
                                    '销量环比', '转化环比', '客单价环比',
                                    '在售环比',
                                    ]

                    ],
                    style_cell_conditional=
                    [
                        {
                            'if': {'column_id': c},
                            'textAlign': 'left'
                        } for c in ['类目']] +
                    [
                        {
                            'if': {'column_id': c},
                            'textAlign': 'center',
                        } for c in ['访客', '销售', '客单价', '支付转化',
                                    '曝光占比', '销售占比', '在售占比',
                                    '销量', '在售商品数']
                    ],
                    style_data_conditional=
                    [
                        {
                            'if': {
                                'column_id': '访客环比',
                                'filter_query': '{访客环比%} > 0'
                            },
                            'textAlign': 'left',
                            'color': 'green',
                        },
                        {
                            'if': {
                                'column_id': '访客环比',
                                'filter_query': '{访客环比%} <= 0'
                            },
                            'textAlign': 'left',
                            'color': 'red',
                        },                     {
                            'if': {
                                'column_id': '销售环比',
                                'filter_query': '{销售环比%} > 0'
                            },
                            'textAlign': 'left',
                            'color': 'green',
                        },
                        {
                            'if': {
                                'column_id': '销售环比',
                                'filter_query': '{销售环比%} <= 0'
                            },
                            'textAlign': 'left',
                            'color': 'red',
                        },                     {
                            'if': {
                                'column_id': '销量环比',
                                'filter_query': '{销量环比%} > 0'
                            },
                            'textAlign': 'left',
                            'color': 'green',
                        },
                        {
                            'if': {
                                'column_id': '销量环比',
                                'filter_query': '{销量环比%} <= 0'
                            },
                            'textAlign': 'left',
                            'color': 'red',
                        },                     {
                            'if': {
                                'column_id': '转化环比',
                                'filter_query': '{转化环比%} > 0'
                            },
                            'textAlign': 'left',
                            'color': 'green',
                        },
                        {
                            'if': {
                                'column_id': '转化环比',
                                'filter_query': '{转化环比%} <= 0'
                            },
                            'textAlign': 'left',
                            'color': 'red',
                        },                     {
                            'if': {
                                'column_id': '客单价环比',
                                'filter_query': '{客单价环比%} > 0'
                            },
                            'textAlign': 'left',
                            'color': 'green',
                        },
                        {
                            'if': {
                                'column_id': '客单价环比',
                                'filter_query': '{客单价环比%} <= 0'
                            },
                            'textAlign': 'left',
                            'color': 'red',
                        },                     {
                            'if': {
                                'column_id': '在售环比',
                                'filter_query': '{在售环比%} > 0'
                            },
                            'textAlign': 'left',
                            'color': 'green',
                        },
                        {
                            'if': {
                                'column_id': '在售环比',
                                'filter_query': '{在售环比%} <= 0'
                            },
                            'textAlign': 'left',
                            'color': 'red',
                        },
                    ]
                )
            ], style={'padding': '10px'}
            ),
            html.Div([html.Label("注:月和周数据为日数据累加和;数据来源:"
                                 "前台类目交易日报;环比=(本周-上周)/上周")],
                     style={'textAlign': 'left', 'color': 'grey', 'font-size': '5px',
                            "font-family": "Microsoft YaHei"
                            }),
        ]
    )

    return fig_table_ca, csv_string_download


if __name__ == '__main__':
    app.server.run(port=5002)