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
SELECT a.log_date,a.front_cate_one,a.yewu_type,a.uv,a.gmv,a.pv,a.qty,a.item_num,a.pay_user_nums,
b.gmv as total_gmv,c.pv as total_pv, b.item_num as total_item_num
from (
SELECT 日期 as log_date,一级类目 front_cate_one,业务类型 yewu_type,SUM(独立访客) as uv,sum(支付金额) as gmv,
sum(曝光数量) as pv,sum(支付商品件数) as qty,AVG(在售商品数) as item_num,sum(买家数) as pay_user_nums
from CFcategory.category_123_day_wholee
WHERE (三级类目 = ' ' or 三级类目 is null) and (二级类目 = ' ' or 二级类目 is null)
GROUP BY 日期 ,一级类目,业务类型
) as a
LEFT JOIN
(
SELECT 日期 as log_date,一级类目 front_cate_one,业务类型 yewu_type,SUM(独立访客) as uv,sum(支付金额) as gmv,
sum(曝光数量) as pv,sum(支付商品件数) as qty,avg(支付转化率 ) as cvr,AVG(在售商品数) as item_num,sum(买家数) as pay_user_nums
from CFcategory.category_123_day_wholee
WHERE (三级类目 = ' ' or 三级类目 is null) and (二级类目 = ' ' or 二级类目 is null) and 一级类目='全类目'
GROUP BY 日期 ,一级类目,业务类型
) as b on a.log_date=b.log_date and a.yewu_type=b.yewu_type
LEFT JOIN
(
SELECT 日期 as log_date,业务类型 yewu_type,SUM(独立访客) as uv,sum(支付金额) as gmv,
sum(曝光数量) as pv,sum(支付商品件数) as qty,avg(支付转化率 ) as cvr,AVG(在售商品数) as item_num,(sum(支付金额)/sum(买家数)) as kedan_prices
from CFcategory.category_123_day_wholee
WHERE (三级类目 = ' ' or 三级类目 is null) and (二级类目 = ' ' or 二级类目 is null) and 一级类目!='全类目'
GROUP BY 日期 ,业务类型
) as c on a.log_date=c.log_date and a.yewu_type=c.yewu_type
/*where a.log_date BETWEEN date_sub(curdate(),interval 90 day) and date_sub(curdate(),interval 1 day) */
ORDER BY a.log_date,a.front_cate_one,a.yewu_type
''')

sql_table_two = ('''
SELECT a.log_date,a.front_cate_one,front_cate_two,a.yewu_type,a.uv,a.gmv,a.pv,a.qty,a.item_num,a.pay_user_nums,
b.gmv as total_gmv,c.pv as total_pv, b.item_num as total_item_num
from (
SELECT 日期 as log_date,一级类目 front_cate_one,二级类目 front_cate_two,业务类型 yewu_type,SUM(独立访客) as uv,sum(支付金额) as gmv,
sum(曝光数量) as pv,sum(支付商品件数) as qty,AVG(在售商品数) as item_num,sum(买家数) as pay_user_nums
from CFcategory.category_123_day_wholee
WHERE (三级类目 = ' ' or 三级类目 is null) and (二级类目 != ' ' and 二级类目 is not null)
GROUP BY 日期 ,一级类目,二级类目,业务类型
) as a
LEFT JOIN
(
SELECT 日期 as log_date,一级类目 front_cate_one,业务类型 yewu_type,sum(支付金额) as gmv,
sum(在售商品数) as item_num
from CFcategory.category_123_day_wholee
WHERE (三级类目 = ' ' or 三级类目 is null) and (二级类目 != ' ' and 二级类目 is not null) and 一级类目!='全类目'
GROUP BY 日期 ,一级类目,业务类型
) as b on a.log_date=b.log_date and a.yewu_type=b.yewu_type and a.front_cate_one=b.front_cate_one
LEFT JOIN
(
SELECT 日期 as log_date,一级类目 front_cate_one,业务类型 yewu_type,
sum(曝光数量) as pv
from CFcategory.category_123_day_wholee
WHERE (三级类目 = ' ' or 三级类目 is null) and (二级类目 != ' ' and 二级类目 is not null) and 一级类目!='全类目'
GROUP BY 日期,一级类目 ,业务类型
) as c on a.log_date=c.log_date and a.yewu_type=c.yewu_type and a.front_cate_one=c.front_cate_one
/*where a.log_date between date_sub(curdate(),interval 60 day) and date_sub(curdate(),interval 1 day) */
ORDER BY a.log_date,a.front_cate_one,a.yewu_type

''')

sql_table_three = ('''
SELECT a.log_date,a.front_cate_one,a.front_cate_two,a.front_cate_three,a.yewu_type,a.uv,a.gmv,a.pv,a.qty,a.item_num,a.pay_user_nums,
b.gmv as total_gmv,c.pv as total_pv, b.item_num as total_item_num
from (
SELECT 日期 as log_date,一级类目 front_cate_one,二级类目 front_cate_two,三级类目 front_cate_three,业务类型 yewu_type,SUM(独立访客) as uv,sum(支付金额) as gmv,
sum(曝光数量) as pv,sum(支付商品件数) as qty,AVG(在售商品数) as item_num,sum(买家数) as pay_user_nums
from CFcategory.category_123_day_wholee
WHERE (三级类目 != ' ' and 三级类目 is not null) and (二级类目 != ' ' and 二级类目 is not null)
GROUP BY 日期 ,一级类目,二级类目,三级类目,业务类型
) as a
LEFT JOIN
(
SELECT 日期 as log_date,一级类目 front_cate_one,二级类目 front_cate_two,业务类型 yewu_type,sum(支付金额) as gmv,
sum(在售商品数) as item_num
from CFcategory.category_123_day_wholee
WHERE (三级类目 != ' ' and 三级类目 is not null) and (二级类目 != ' ' and 二级类目 is not null) and 一级类目!='全类目'
GROUP BY 日期 ,一级类目,二级类目,业务类型
) as b on a.log_date=b.log_date and a.yewu_type=b.yewu_type and a.front_cate_one=b.front_cate_one and a.front_cate_two=b.front_cate_two
LEFT JOIN
(
SELECT 日期 as log_date,一级类目 front_cate_one,二级类目 front_cate_two,业务类型 yewu_type,
sum(曝光数量) as pv
from CFcategory.category_123_day_wholee
WHERE (三级类目 != ' ' and 三级类目 is not null) and (二级类目 != ' ' and 二级类目 is not null) and 一级类目!='全类目'
GROUP BY 日期,一级类目,二级类目 ,业务类型
) as c on a.log_date=c.log_date and a.yewu_type=c.yewu_type and a.front_cate_one=c.front_cate_one and a.front_cate_two=c.front_cate_two
/*where a.log_date between date_sub(curdate(),interval 30 day) and date_sub(curdate(),interval 1 day) */
ORDER BY a.log_date,a.front_cate_one,a.yewu_type
''')

# 可视化table数据处理
data_table_ca = pd.read_sql(sql_table_ca, con_mssql)
print("打印DashCateSec-类目数据看板")
data_table_two = pd.read_sql(sql_table_two, con_mssql)
data_table_three = pd.read_sql(sql_table_three, con_mssql)

df = pd.read_excel(
    '/Users/apache/Downloads/PythonDa/category_relation_new.xlsx',
    sheet_name='Sheet1')

WomenShoes = list(set(df[df.old_cate_one.eq('Women\'s Shoes')].old_cate_two))
WomenShoes.append('总')
WomenClothing = list(
    set(df[df.old_cate_one.eq('Women\'s Clothing')].old_cate_two))
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
JewelryAccessories = list(
    set(df[df.old_cate_one.eq('Jewelry & Accessories')].old_cate_two))
JewelryAccessories.append('总')
HomeAppliances = list(
    set(df[df.old_cate_one.eq('Home Appliances')].old_cate_two))
HomeAppliances.append('总')
MobilesAccessories = list(
    set(df[df.old_cate_one.eq('Mobiles & Accessories')].old_cate_two))
MobilesAccessories.append('总')
Electronics = list(set(df[df.old_cate_one.eq('Electronics')].old_cate_two))
Electronics.append('总')
BeautyHealth = list(
    set(df[df.old_cate_one.eq('Beauty & Health')].old_cate_two))
BeautyHealth.append('总')
Home = list(set(df[df.old_cate_one.eq('Home')].old_cate_two))
Home.append('总')

autofetch_front_cate_one_goods = [
    '总',
    'Women\'s Shoes',
    'Women\'s Clothing',
    'Women\'s Bags',
    'Watches',
    'Men\'s Shoes',
    'Home',
    'Men\'s Clothing',
    'Men\'s Bags',
    'Jewelry & Accessories',
    'Home Appliances',
    'Mobiles & Accessories',
    'Electronics',
    'Beauty & Health']

autofetch_front_cate_two_goods = {
    '总': '总',
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

fig_table_ca = []
csv_string_download = []

layout = \
    html.Div([
        html.Div([header(app)], ),
        html.Div([
            html.Div([
                html.Div([
                    html.Div([
                        html.Label("**Wholee-数据看板**",
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
                            id='date-picker-range-cate-yewuxian-ott-wholee',
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
                            end_date=(
                                datetime.now() -
                                timedelta(
                                    days=1)).strftime('%Y-%m-%d'),
                            start_date_placeholder_text="Start Date",
                            end_date_placeholder_text="End Date",
                            calendar_orientation='vertical',
                            display_format="YY/M/D-Q-ωW-E",  # q ε
                            style={
                                "height": "100%",
                                "width": "150px",
                                'font-size': '9px'}
                        )
                    ],
                        className="padding-top-bot"
                    ),
                    html.Div([
                        html.Label("业务类型：",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        dcc.Dropdown(
                            id='my-dropdown-table-ca-ott-wholee',
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
                        html.Label("一级类目：",
                                   style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                                          'font-family': 'Microsoft YaHei', 'font-size': 13}),
                        dcc.Dropdown(
                            id='front-cate-one-goods-ott-wholee',
                            placeholder="Select CategoryOne",
                            value='总',
                            options=[{'label': v, 'value': v}
                                     for v in autofetch_front_cate_one_goods],
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
                                id='front-cate-two-goods-ott-wholee',
                                # multi=True
                                style={'width': '150px', 'font-family': 'Microsoft YaHei',
                                       'font-size': 8, 'textAlign': 'left'}
                            )
                        ],
                        id='front-cate-two-container-goods-ott-wholee',
                        # className="padding-top-bot"
                    ),
                    html.Div([
                        html.A(html.Button("Download Data", id='data_download_button_wholee',
                                           style={
                                               "width": '150px', 'font-family': 'Microsoft YaHei', 'font-size': 9}
                                           ),
                               id='download-link-ott-wholee',
                               href="",
                               download="类目数据看板.csv",
                               target="_blank",
                               ),
                    ],
                        className="padding-top-bot"
                    ),
                ],
                    className="bg-white user-control  add_yingying"
                )
            ],
                className="two columns card-top"
            ),
            html.Div(
                buld_modal_info_overlay('WholeeDashCateSec', 'bottom', dedent("""
                目前只展示Wholee一级类目数据/二三级数据目前为空\n
                访客逻辑：商品维度，按照类目聚合，计算商详页UV\n
                访客、销售、销量、曝光及相应的占比数据=统计时间内日累计数据\n
                在售商品数=统计时间内日均值\n
                数据来源:前台类目交易日报\n
                环比=(本周-上周)/上周 - 支付转化=支付买家数/访客 - 客单价=销售/支付买家数\n
                一级数据展示：一级和二级均选择'总'\n
                二级数据展示：一级不为'总'，二级选择'总'\n
                三级数据展示：一级不为'总'，二级不为'总'\n
                日期可选范围：一级近90天，二级近60天，三级近30天\n
                """)
                                        )
            ),
            html.Div([
                html.Div([
                    html.Label(
                        id='show-WholeeDashCateSec-modal',
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
                    id='my-graph-table-ca-ott-wholee',
                    className="bg-white add_yingying"
                )
            ],
                className="ten columns card-left-top",
                id="WholeeDashCateSec-div"
            )
        ], className="row app-body"
        ),
    ],
    )

for id in ['WholeeDashCateSec']:
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
@app.callback(Output('front-cate-two-container-goods-ott-wholee', 'children'),
              [Input('front-cate-one-goods-ott-wholee', 'value')]
              )
def set_front_cate_two(cate_one_dash):
    autofetch_cate_two_goods = autofetch_front_cate_two_goods[cate_one_dash]
    return \
        html.Div([
            html.Label("二级类目：",
                       style={'textAlign': 'left', 'width': '100%', 'position': 'relative',
                              'font-family': 'Microsoft YaHei', 'font-size': 13}),
            dcc.Dropdown(
                id='front-cate-two-goods-ott-wholee',
                placeholder="Select CategoryTwo",
                value="总",
                options=[{'label': v, 'value': v}
                         for v in autofetch_cate_two_goods],
                style={'width': '150px', 'font-family': 'Microsoft YaHei',
                       'font-size': 8, 'textAlign': 'left'},
                # persistence_type='session',
                # persistence=cate_one,
                # multi=True
            )
        ],
            className="padding-top-bot"
        )


@app.callback([Output('my-graph-table-ca-ott-wholee',
                      'children'),
               Output('download-link-ott-wholee',
                      'href')],
              [Input('date-picker-range-cate-yewuxian-ott-wholee',
                     'start_date'),
               Input('date-picker-range-cate-yewuxian-ott-wholee',
                     'end_date'),
               Input('my-dropdown-table-ca-ott-wholee',
                     'value'),
               Input('front-cate-one-goods-ott-wholee',
                     'value'),
               Input('front-cate-two-goods-ott-wholee',
                     'value'),
               ],
              )
def update_table_ca(start_date, end_date,
                    selected_dropdown_value_table_ca,
                    front_cate_one_dash,
                    front_cate_two_dash):
    global fig_table_ca, csv_string_download

    if front_cate_one_dash == '总' and front_cate_two_dash == '总':
        data_table_ca.log_date = pd.to_datetime(
            data_table_ca.log_date, format='%Y-%m-%d')
        data_table_ca_now = data_table_ca[(data_table_ca.log_date >= start_date) & (
            data_table_ca.log_date <= end_date)]
        data_table_ca_past = \
            data_table_ca[(data_table_ca.log_date >=
                           (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=7))) &
                          (data_table_ca.log_date <=
                           (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=7)))]

        data_table_ca_now_item_num_mean = data_table_ca_now.groupby(
            by=['front_cate_one', 'yewu_type'], as_index=False).item_num.mean()
        data_table_ca_now_item_num_mean.rename(
            columns={'item_num': 'item_num_mean'}, inplace=True)
        data_table_ca_now_sum = data_table_ca_now.groupby(
            by=['front_cate_one', 'yewu_type'], as_index=False).sum()
        data_table_ca_now_total = pd.merge(
            data_table_ca_now_sum, data_table_ca_now_item_num_mean, on=[
                'front_cate_one', 'yewu_type'])

        data_table_ca_now_total['cvr'] = (
            data_table_ca_now_total.pay_user_nums /
            data_table_ca_now_total.uv)
        data_table_ca_now_total['kd_price'] = data_table_ca_now_total.gmv / \
            data_table_ca_now_total.pay_user_nums

        data_table_ca_past_item_num_mean = data_table_ca_past.groupby(
            by=['front_cate_one', 'yewu_type'], as_index=False).item_num.mean()
        data_table_ca_past_item_num_mean.rename(
            columns={'item_num': 'item_num_mean'}, inplace=True)
        data_table_ca_past_sum = data_table_ca_past.groupby(
            by=['front_cate_one', 'yewu_type'], as_index=False).sum()
        data_table_ca_past_total = pd.merge(
            data_table_ca_past_sum, data_table_ca_past_item_num_mean, on=[
                'front_cate_one', 'yewu_type'])

        data_table_ca_past_total['cvr_past'] = \
            data_table_ca_past_total.pay_user_nums / data_table_ca_past_total.uv
        data_table_ca_past_total['kd_price_past'] = \
            data_table_ca_past_total.gmv / data_table_ca_past_total.pay_user_nums

        data_table_ca_past_total.rename(
            columns={
                'uv': 'uv_past',
                'gmv': 'gmv_past',
                'pv': 'pv_past',
                'qty': 'qty_past',
                'item_num': 'item_num_past',
                'pay_user_nums': 'pay_user_nums_past',
                'total_gmv': 'total_gmv_past',
                'total_pv': 'total_pv_past',
                'total_item_num': 'total_item_num_past',
                'item_num_mean': 'item_num_mean_past'},
            inplace=True)

        data_table_ca_total = pd.merge(
            data_table_ca_now_total, data_table_ca_past_total, on=[
                'front_cate_one', 'yewu_type'])
        print("表格now and past 数据合并")

        data_table_ca_total['uv_hb'] = \
            ((data_table_ca_total.uv - data_table_ca_total.uv_past) /
             data_table_ca_total.uv_past)
        data_table_ca_total['gmv_hb'] = \
            ((data_table_ca_total.gmv - data_table_ca_total.gmv_past) /
             data_table_ca_total.gmv_past)
        data_table_ca_total['qty_hb'] = \
            ((data_table_ca_total.qty - data_table_ca_total.qty_past) /
             data_table_ca_total.qty_past)
        data_table_ca_total['item_num_hb'] = (
            (data_table_ca_total.item_num_mean -
             data_table_ca_total.item_num_mean_past) /
            data_table_ca_total.item_num_mean_past)
        data_table_ca_total['cvr_hb'] = \
            ((data_table_ca_total.cvr - data_table_ca_total.cvr_past) /
             data_table_ca_total.cvr_past)
        data_table_ca_total['kd_price_hb'] = (
            (data_table_ca_total.kd_price -
             data_table_ca_total.kd_price_past) /
            data_table_ca_total.kd_price_past)

        data_table_ca_total['pv_zb'] = \
            (data_table_ca_total.pv / data_table_ca_total.total_pv)
        data_table_ca_total['gmv_zb'] = \
            (data_table_ca_total.gmv / data_table_ca_total.total_gmv)
        data_table_ca_total['item_num_zb'] = \
            (data_table_ca_total.item_num / data_table_ca_total.total_item_num)
        print("表格环比及占比数据打印")

        filter_data_table_ca_total = \
            data_table_ca_total[['yewu_type', 'front_cate_one', 'uv', 'gmv', 'qty', 'cvr',
                                 'kd_price', 'item_num_mean', 'pv_zb', 'gmv_zb', 'item_num_zb',
                                 'uv_hb', 'gmv_hb', 'qty_hb', 'cvr_hb', 'kd_price_hb',
                                 'item_num_hb']]

        filter_data_table_ca_total = filter_data_table_ca_total.sort_values(
            by='gmv', ascending=False).head(50)  # 降序

        filter_data_table_ca_total.rename(
            columns={
                'yewu_type': '类型',
                'front_cate_one': '类目',
                'uv': '访客',
                'gmv': '销售',
                'qty': '销量',
                'cvr': '支付转化',
                'kd_price': '客单价',
                'item_num_mean': '在售商品数',
                'pv_zb': '曝光占比',
                'gmv_zb': '销售占比',
                'item_num_zb': '在售占比',
                'uv_hb': '访客环比%',
                'gmv_hb': '销售环比%',
                'qty_hb': '销量环比%',
                'cvr_hb': '转化环比%',
                'kd_price_hb': '客单价环比%',
                'item_num_hb': '在售环比%'},
            inplace=True)
        print("表格最终作图数据打印")

        df_download = filter_data_table_ca_total[filter_data_table_ca_total['类型'] ==
                                                 selected_dropdown_value_table_ca]
        csv_string_download = df_download.to_csv(index=False, encoding='utf-8')
        csv_string_download = "data:text/csv;charset=utf-8," + \
            urllib.parse.quote(csv_string_download)

        filter_data_table_ca_total['访客'] = \
            (filter_data_table_ca_total['访客'] / 1).round(1)
        filter_data_table_ca_total['销售'] = \
            (filter_data_table_ca_total['销售'] / 1).round(1)
        filter_data_table_ca_total['销量'] = \
            (filter_data_table_ca_total['销量'] / 1).round(1)
        filter_data_table_ca_total['在售商品数'] = \
            (filter_data_table_ca_total['在售商品数'] / 1).round(1)
        filter_data_table_ca_total['客单价'] = \
            (filter_data_table_ca_total['客单价'] / 1).round(2)

        filter_data_table_ca_total['访客环比'] = \
            filter_data_table_ca_total['访客环比%'].apply(lambda x: format(x, '.2%'))
        filter_data_table_ca_total['访客环比'] = \
            ['▲' + str(i) if i.find('-') else "▼" + str(i)
             for i in filter_data_table_ca_total['访客环比']]

        filter_data_table_ca_total['销售环比'] = \
            filter_data_table_ca_total['销售环比%'].apply(lambda x: format(x, '.2%'))
        filter_data_table_ca_total['销售环比'] = \
            ['▲' + str(i) if i.find('-') else "▼" + str(i)
             for i in filter_data_table_ca_total['销售环比']]

        filter_data_table_ca_total['销量环比'] = \
            filter_data_table_ca_total['销量环比%'].apply(lambda x: format(x, '.2%'))
        filter_data_table_ca_total['销量环比'] = \
            ['▲' + str(i) if i.find('-') else "▼" + str(i)
             for i in filter_data_table_ca_total['销量环比']]

        filter_data_table_ca_total['转化环比'] = \
            filter_data_table_ca_total['转化环比%'].apply(lambda x: format(x, '.2%'))
        filter_data_table_ca_total['转化环比'] = \
            ['▲' + str(i) if i.find('-') else "▼" + str(i)
             for i in filter_data_table_ca_total['转化环比']]

        filter_data_table_ca_total['客单价环比'] = \
            filter_data_table_ca_total['客单价环比%'].apply(lambda x: format(x, '.2%'))
        filter_data_table_ca_total['客单价环比'] = \
            ['▲' + str(i) if i.find('-') else "▼" + str(i)
             for i in filter_data_table_ca_total['客单价环比']]

        filter_data_table_ca_total['在售环比'] = \
            filter_data_table_ca_total['在售环比%'].apply(lambda x: format(x, '.2%'))
        filter_data_table_ca_total['在售环比'] = \
            ['▲' + str(i) if i.find('-') else "▼" + str(i)
             for i in filter_data_table_ca_total['在售环比']]

        fig_table_ca = html.Div(
            [
                html.Div([
                    html.Label(f"{start_date}--{end_date} "
                               f"{selected_dropdown_value_table_ca} Wholee类目数据看板")],
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
                                        symbol_suffix=u''
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
                            } for c in ['类目', '访客环比', '销售环比',
                                        '销量环比', '转化环比', '客单价环比',
                                        '在售环比',
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
                            } for c in ['访客', '销售', '客单价', '支付转化',
                                        '曝光占比', '销售占比', '在售占比',
                                        '销量', '在售商品数']
                        ],
                        style_data_conditional=[
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
                            }, {
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
                            }, {
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
                            }, {
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
                            }, {
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
                            }, {
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

    elif '总' not in front_cate_one_dash and front_cate_two_dash == '总':
        data_table_ca_two = data_table_two[data_table_two['front_cate_one'] ==
                                           front_cate_one_dash]
        data_table_ca_two.log_date = pd.to_datetime(
            data_table_ca_two.log_date, format='%Y-%m-%d')
        data_table_ca_now = data_table_ca_two[(data_table_ca_two.log_date >= start_date) & (
            data_table_ca_two.log_date <= end_date)]
        data_table_ca_past = \
            data_table_ca_two[(data_table_ca_two.log_date >=
                               (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=7))) &
                              (data_table_ca_two.log_date <=
                               (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=7)))]

        data_table_ca_now_item_num_mean = data_table_ca_now.groupby(
            by=['front_cate_one', 'front_cate_two', 'yewu_type'], as_index=False).item_num.mean()
        data_table_ca_now_item_num_mean.rename(
            columns={'item_num': 'item_num_mean'}, inplace=True)
        data_table_ca_now_sum = data_table_ca_now.groupby(
            by=['front_cate_one', 'front_cate_two', 'yewu_type'], as_index=False).sum()
        data_table_ca_now_total = \
            pd.merge(data_table_ca_now_sum, data_table_ca_now_item_num_mean,
                     on=['front_cate_one', 'front_cate_two', 'yewu_type'])

        data_table_ca_now_total['cvr'] = (
            data_table_ca_now_total.pay_user_nums /
            data_table_ca_now_total.uv)
        data_table_ca_now_total['kd_price'] = data_table_ca_now_total.gmv / \
            data_table_ca_now_total.pay_user_nums

        data_table_ca_past_item_num_mean = data_table_ca_past.groupby(
            by=['front_cate_one', 'front_cate_two', 'yewu_type'], as_index=False).item_num.mean()
        data_table_ca_past_item_num_mean.rename(
            columns={'item_num': 'item_num_mean'}, inplace=True)
        data_table_ca_past_sum = data_table_ca_past.groupby(
            by=['front_cate_one', 'front_cate_two', 'yewu_type'], as_index=False).sum()
        data_table_ca_past_total = \
            pd.merge(
                data_table_ca_past_sum, data_table_ca_past_item_num_mean,
                on=['front_cate_one', 'front_cate_two', 'yewu_type'])

        data_table_ca_past_total['cvr_past'] = \
            data_table_ca_past_total.pay_user_nums / data_table_ca_past_total.uv
        data_table_ca_past_total['kd_price_past'] = \
            data_table_ca_past_total.gmv / data_table_ca_past_total.pay_user_nums

        data_table_ca_past_total.rename(
            columns={
                'uv': 'uv_past',
                'gmv': 'gmv_past',
                'pv': 'pv_past',
                'qty': 'qty_past',
                'item_num': 'item_num_past',
                'pay_user_nums': 'pay_user_nums_past',
                'total_gmv': 'total_gmv_past',
                'total_pv': 'total_pv_past',
                'total_item_num': 'total_item_num_past',
                'item_num_mean': 'item_num_mean_past'},
            inplace=True)

        data_table_ca_total = \
            pd.merge(
                data_table_ca_now_total, data_table_ca_past_total,
                on=['front_cate_one', 'front_cate_two', 'yewu_type'])
        print("表格now and past 数据合并")

        data_table_ca_total['uv_hb'] = \
            ((data_table_ca_total.uv - data_table_ca_total.uv_past) /
             data_table_ca_total.uv_past)
        data_table_ca_total['gmv_hb'] = \
            ((data_table_ca_total.gmv - data_table_ca_total.gmv_past) /
             data_table_ca_total.gmv_past)
        data_table_ca_total['qty_hb'] = \
            ((data_table_ca_total.qty - data_table_ca_total.qty_past) /
             data_table_ca_total.qty_past)
        data_table_ca_total['item_num_hb'] = (
            (data_table_ca_total.item_num_mean -
             data_table_ca_total.item_num_mean_past) /
            data_table_ca_total.item_num_mean_past)
        data_table_ca_total['cvr_hb'] = \
            ((data_table_ca_total.cvr - data_table_ca_total.cvr_past) /
             data_table_ca_total.cvr_past)
        data_table_ca_total['kd_price_hb'] = (
            (data_table_ca_total.kd_price -
             data_table_ca_total.kd_price_past) /
            data_table_ca_total.kd_price_past)

        data_table_ca_total['pv_zb'] = \
            (data_table_ca_total.pv / data_table_ca_total.total_pv)
        data_table_ca_total['gmv_zb'] = \
            (data_table_ca_total.gmv / data_table_ca_total.total_gmv)
        data_table_ca_total['item_num_zb'] = \
            (data_table_ca_total.item_num / data_table_ca_total.total_item_num)
        print("表格环比及占比数据打印")

        filter_data_table_ca_total = \
            data_table_ca_total[['yewu_type', 'front_cate_one', 'front_cate_two', 'uv', 'gmv', 'qty', 'cvr',
                                 'kd_price', 'item_num_mean', 'pv_zb', 'gmv_zb', 'item_num_zb',
                                 'uv_hb', 'gmv_hb', 'qty_hb', 'cvr_hb', 'kd_price_hb',
                                 'item_num_hb']]

        filter_data_table_ca_total = filter_data_table_ca_total.sort_values(
            by='gmv', ascending=False).head(50)  # 降序

        filter_data_table_ca_total.rename(
            columns={
                'yewu_type': '类型',
                'front_cate_one': '一级类目',
                'front_cate_two': '二级类目',
                'uv': '访客',
                'gmv': '销售',
                'qty': '销量',
                'cvr': '支付转化',
                'kd_price': '客单价',
                'item_num_mean': '在售商品数',
                'pv_zb': '曝光占比',
                'gmv_zb': '销售占比',
                'item_num_zb': '在售占比',
                'uv_hb': '访客环比%',
                'gmv_hb': '销售环比%',
                'qty_hb': '销量环比%',
                'cvr_hb': '转化环比%',
                'kd_price_hb': '客单价环比%',
                'item_num_hb': '在售环比%'},
            inplace=True)
        print("表格最终作图数据打印")

        df_download = filter_data_table_ca_total[filter_data_table_ca_total['类型'] ==
                                                 selected_dropdown_value_table_ca]
        csv_string_download = df_download.to_csv(index=False, encoding='utf-8')
        csv_string_download = "data:text/csv;charset=utf-8," + \
            urllib.parse.quote(csv_string_download)

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

        filter_data_table_ca_total['访客环比'] = \
            filter_data_table_ca_total['访客环比%'].apply(lambda x: format(x, '.2%'))
        filter_data_table_ca_total['访客环比'] = \
            ['▲' + str(i) if i.find('-') else "▼" + str(i)
             for i in filter_data_table_ca_total['访客环比']]

        filter_data_table_ca_total['销售环比'] = \
            filter_data_table_ca_total['销售环比%'].apply(lambda x: format(x, '.2%'))
        filter_data_table_ca_total['销售环比'] = \
            ['▲' + str(i) if i.find('-') else "▼" + str(i)
             for i in filter_data_table_ca_total['销售环比']]

        filter_data_table_ca_total['销量环比'] = \
            filter_data_table_ca_total['销量环比%'].apply(lambda x: format(x, '.2%'))
        filter_data_table_ca_total['销量环比'] = \
            ['▲' + str(i) if i.find('-') else "▼" + str(i)
             for i in filter_data_table_ca_total['销量环比']]

        filter_data_table_ca_total['转化环比'] = \
            filter_data_table_ca_total['转化环比%'].apply(lambda x: format(x, '.2%'))
        filter_data_table_ca_total['转化环比'] = \
            ['▲' + str(i) if i.find('-') else "▼" + str(i)
             for i in filter_data_table_ca_total['转化环比']]

        filter_data_table_ca_total['客单价环比'] = \
            filter_data_table_ca_total['客单价环比%'].apply(lambda x: format(x, '.2%'))
        filter_data_table_ca_total['客单价环比'] = \
            ['▲' + str(i) if i.find('-') else "▼" + str(i)
             for i in filter_data_table_ca_total['客单价环比']]

        filter_data_table_ca_total['在售环比'] = \
            filter_data_table_ca_total['在售环比%'].apply(lambda x: format(x, '.2%'))
        filter_data_table_ca_total['在售环比'] = \
            ['▲' + str(i) if i.find('-') else "▼" + str(i)
             for i in filter_data_table_ca_total['在售环比']]

        fig_table_ca = html.Div(
            [
                html.Div([
                    html.Label(f"{start_date}--{end_date} "
                               f"{selected_dropdown_value_table_ca} Wholee类目数据看板")],
                         style={'textAlign': 'center', "font-family": "Microsoft YaHei",
                                "font-size": "15px", 'color': 'rgb(0, 81, 108)',
                                'fontWeight': 'bold'}),
                html.Div([
                    dash_table.DataTable(
                        data=filter_data_table_ca_total[filter_data_table_ca_total['类型'] ==
                                                        selected_dropdown_value_table_ca].to_dict('records'),
                        columns=[{
                            'id': c, 'name': c}
                            for c in ['一级类目', '二级类目']
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
                        style_header={
                            "font-family": "Microsoft YaHei",
                            "font-size": '10px',
                            "color": "rgb(255, 255, 255)",
                            'fontWeight': 'bold',
                            'textAlign': 'center',
                            'height': 'auto',
                            # 'whiteSpace': 'normal',
                            'backgroundColor': 'rgb(0, 81, 108)',
                            'border': '0px',
                            'padding': '0px',
                        },
                        style_as_list_view=True,
                        style_cell={
                            "font-family": "Microsoft YaHei",
                            "font-size": '9px',
                            'padding': '0px',
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
                            } for c in ['一级类目', '二级类目', '访客环比', '销售环比',
                                        '销量环比', '转化环比', '客单价环比',
                                        '在售环比',
                                        ]

                        ],
                        style_cell_conditional=[
                            {
                                'if': {'column_id': c},
                                'textAlign': 'left'
                            } for c in ['一级类目', '二级类目']] +
                        [
                            {
                                'if': {'column_id': c},
                                'textAlign': 'center',
                            } for c in ['访客', '销售', '客单价', '支付转化',
                                        '曝光占比', '销售占比', '在售占比',
                                        '销量', '在售商品数']
                        ],
                        style_data_conditional=[
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
                            }, {
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
                            }, {
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
                            }, {
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
                            }, {
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
                            }, {
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

    elif '总' not in front_cate_one_dash and '总' not in front_cate_two_dash:
        data_table_ca_three = \
            data_table_three[
                (data_table_three['front_cate_one'] == front_cate_one_dash) &
                (data_table_three['front_cate_two'] == front_cate_two_dash)]
        data_table_three.log_date = pd.to_datetime(
            data_table_three.log_date, format='%Y-%m-%d')
        data_table_ca_now = data_table_three[(data_table_three.log_date >= start_date) & (
            data_table_three.log_date <= end_date)]
        data_table_ca_past = \
            data_table_ca_three[(data_table_three.log_date >=
                                 (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=7))) &
                                (data_table_three.log_date <=
                                 (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=7)))]

        data_table_ca_now_item_num_mean = \
            data_table_ca_now.groupby(
                by=['front_cate_one', 'front_cate_two', 'front_cate_three', 'yewu_type'],
                as_index=False).item_num.mean()
        data_table_ca_now_item_num_mean.rename(
            columns={'item_num': 'item_num_mean'}, inplace=True)
        data_table_ca_now_sum = \
            data_table_ca_now.groupby(
                by=['front_cate_one', 'front_cate_two', 'front_cate_three', 'yewu_type'],
                as_index=False).sum()
        data_table_ca_now_total = pd.merge(
            data_table_ca_now_sum, data_table_ca_now_item_num_mean, on=[
                'front_cate_one', 'front_cate_two', 'front_cate_three', 'yewu_type'])

        data_table_ca_now_total['cvr'] = (
            data_table_ca_now_total.pay_user_nums /
            data_table_ca_now_total.uv)
        data_table_ca_now_total['kd_price'] = data_table_ca_now_total.gmv / \
            data_table_ca_now_total.pay_user_nums

        data_table_ca_past_item_num_mean = data_table_ca_past.groupby(
            by=[
                'front_cate_one',
                'front_cate_two',
                'front_cate_three',
                'yewu_type'],
            as_index=False).item_num.mean()
        data_table_ca_past_item_num_mean.rename(
            columns={'item_num': 'item_num_mean'}, inplace=True)
        data_table_ca_past_sum = data_table_ca_past.groupby(
            by=['front_cate_one', 'front_cate_two', 'front_cate_three', 'yewu_type'], as_index=False).sum()
        data_table_ca_past_total = pd.merge(
            data_table_ca_past_sum, data_table_ca_past_item_num_mean, on=[
                'front_cate_one', 'front_cate_two', 'front_cate_three', 'yewu_type'])

        data_table_ca_past_total['cvr_past'] = \
            data_table_ca_past_total.pay_user_nums / data_table_ca_past_total.uv
        data_table_ca_past_total['kd_price_past'] = \
            data_table_ca_past_total.gmv / data_table_ca_past_total.pay_user_nums

        data_table_ca_past_total.rename(
            columns={
                'uv': 'uv_past',
                'gmv': 'gmv_past',
                'pv': 'pv_past',
                'qty': 'qty_past',
                'item_num': 'item_num_past',
                'pay_user_nums': 'pay_user_nums_past',
                'total_gmv': 'total_gmv_past',
                'total_pv': 'total_pv_past',
                'total_item_num': 'total_item_num_past',
                'item_num_mean': 'item_num_mean_past'},
            inplace=True)

        data_table_ca_total = pd.merge(
            data_table_ca_now_total, data_table_ca_past_total, on=[
                'front_cate_one', 'front_cate_two', 'front_cate_three', 'yewu_type'])
        print("表格now and past 数据合并")

        data_table_ca_total['uv_hb'] = \
            ((data_table_ca_total.uv - data_table_ca_total.uv_past) /
             data_table_ca_total.uv_past)
        data_table_ca_total['gmv_hb'] = \
            ((data_table_ca_total.gmv - data_table_ca_total.gmv_past) /
             data_table_ca_total.gmv_past)
        data_table_ca_total['qty_hb'] = \
            ((data_table_ca_total.qty - data_table_ca_total.qty_past) /
             data_table_ca_total.qty_past)
        data_table_ca_total['item_num_hb'] = (
            (data_table_ca_total.item_num_mean -
             data_table_ca_total.item_num_mean_past) /
            data_table_ca_total.item_num_mean_past)
        data_table_ca_total['cvr_hb'] = \
            ((data_table_ca_total.cvr - data_table_ca_total.cvr_past) /
             data_table_ca_total.cvr_past)
        data_table_ca_total['kd_price_hb'] = (
            (data_table_ca_total.kd_price -
             data_table_ca_total.kd_price_past) /
            data_table_ca_total.kd_price_past)

        data_table_ca_total['pv_zb'] = \
            (data_table_ca_total.pv / data_table_ca_total.total_pv)
        data_table_ca_total['gmv_zb'] = \
            (data_table_ca_total.gmv / data_table_ca_total.total_gmv)
        data_table_ca_total['item_num_zb'] = \
            (data_table_ca_total.item_num / data_table_ca_total.total_item_num)
        print("表格环比及占比数据打印")

        filter_data_table_ca_total = data_table_ca_total[['yewu_type',
                                                          'front_cate_one',
                                                          'front_cate_two',
                                                          'front_cate_three',
                                                          'uv',
                                                          'gmv',
                                                          'qty',
                                                          'cvr',
                                                          'kd_price',
                                                          'item_num_mean',
                                                          'pv_zb',
                                                          'gmv_zb',
                                                          'item_num_zb',
                                                          'uv_hb',
                                                          'gmv_hb',
                                                          'qty_hb',
                                                          'cvr_hb',
                                                          'kd_price_hb',
                                                          'item_num_hb']]

        filter_data_table_ca_total = filter_data_table_ca_total.sort_values(
            by='gmv', ascending=False).head(50)  # 降序

        filter_data_table_ca_total.rename(
            columns={
                'yewu_type': '类型',
                'front_cate_one': '一级类目',
                'front_cate_two': '二级类目',
                'front_cate_three': '三级类目',
                'uv': '访客',
                'gmv': '销售',
                'qty': '销量',
                'cvr': '支付转化',
                'kd_price': '客单价',
                'item_num_mean': '在售商品数',
                'pv_zb': '曝光占比',
                'gmv_zb': '销售占比',
                'item_num_zb': '在售占比',
                'uv_hb': '访客环比%',
                'gmv_hb': '销售环比%',
                'qty_hb': '销量环比%',
                'cvr_hb': '转化环比%',
                'kd_price_hb': '客单价环比%',
                'item_num_hb': '在售环比%'},
            inplace=True)
        print("表格最终作图数据打印")

        df_download = filter_data_table_ca_total[filter_data_table_ca_total['类型'] ==
                                                 selected_dropdown_value_table_ca]
        csv_string_download = df_download.to_csv(index=False, encoding='utf-8')
        csv_string_download = "data:text/csv;charset=utf-8," + \
            urllib.parse.quote(csv_string_download)

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

        filter_data_table_ca_total['访客环比'] = \
            filter_data_table_ca_total['访客环比%'].apply(lambda x: format(x, '.2%'))
        filter_data_table_ca_total['访客环比'] = \
            ['▲' + str(i) if i.find('-') else "▼" + str(i)
             for i in filter_data_table_ca_total['访客环比']]

        filter_data_table_ca_total['销售环比'] = \
            filter_data_table_ca_total['销售环比%'].apply(lambda x: format(x, '.2%'))
        filter_data_table_ca_total['销售环比'] = \
            ['▲' + str(i) if i.find('-') else "▼" + str(i)
             for i in filter_data_table_ca_total['销售环比']]

        filter_data_table_ca_total['销量环比'] = \
            filter_data_table_ca_total['销量环比%'].apply(lambda x: format(x, '.2%'))
        filter_data_table_ca_total['销量环比'] = \
            ['▲' + str(i) if i.find('-') else "▼" + str(i)
             for i in filter_data_table_ca_total['销量环比']]

        filter_data_table_ca_total['转化环比'] = \
            filter_data_table_ca_total['转化环比%'].apply(lambda x: format(x, '.2%'))
        filter_data_table_ca_total['转化环比'] = \
            ['▲' + str(i) if i.find('-') else "▼" + str(i)
             for i in filter_data_table_ca_total['转化环比']]

        filter_data_table_ca_total['客单价环比'] = \
            filter_data_table_ca_total['客单价环比%'].apply(lambda x: format(x, '.2%'))
        filter_data_table_ca_total['客单价环比'] = \
            ['▲' + str(i) if i.find('-') else "▼" + str(i)
             for i in filter_data_table_ca_total['客单价环比']]

        filter_data_table_ca_total['在售环比'] = \
            filter_data_table_ca_total['在售环比%'].apply(lambda x: format(x, '.2%'))
        filter_data_table_ca_total['在售环比'] = \
            ['▲' + str(i) if i.find('-') else "▼" + str(i)
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
                            for c in ['一级类目', '二级类目', '三级类目']
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
                        style_header={
                            "font-family": "Microsoft YaHei",
                            "font-size": '10px',
                            "color": "rgb(255, 255, 255)",
                            'fontWeight': 'bold',
                            'textAlign': 'center',
                            'height': 'auto',
                            # 'whiteSpace': 'normal',
                            'backgroundColor': 'rgb(0, 81, 108)',
                            'border': '0px',
                            'padding': '0px',
                        },
                        style_as_list_view=True,
                        style_cell={
                            "font-family": "Microsoft YaHei",
                            "font-size": '9px',
                            'padding': '0px',
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
                            } for c in ['一级类目', '二级类目', '三级类目', '访客环比', '销售环比',
                                        '销量环比', '转化环比', '客单价环比',
                                        '在售环比',
                                        ]

                        ],
                        style_cell_conditional=[
                            {
                                'if': {'column_id': c},
                                'textAlign': 'left'
                            } for c in ['一级类目', '二级类目', '三级类目']] +
                        [
                            {
                                'if': {'column_id': c},
                                'textAlign': 'center',
                            } for c in ['访客', '销售', '客单价', '支付转化',
                                        '曝光占比', '销售占比', '在售占比',
                                        '销量', '在售商品数']
                        ],
                        style_data_conditional=[
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
                            }, {
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
                            }, {
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
                            }, {
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
                            }, {
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
                            }, {
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

    else:
        fig_table_ca = html.Div([
            html.Label("！！选择错误，请按照规则选择！！")
        ],
            style={'textAlign': 'center', 'color': 'grey', 'font-size': '15px',
                   "font-family": "Microsoft YaHei"
                   }
        )

    return fig_table_ca, csv_string_download
