import pymssql
import pandas as pd  # 数据处理例如：读入，插入需要用的包
import numpy as np  # 平均值中位数需要用的包
import os  # 设置路径需要用的包
import psycopg2
from datetime import datetime, timedelta  # 设置当前时间及时间间隔计算需要用的包

con_mssql = pymssql.connect("172.16.92.2", "sa", "yssshushan2008", "CFflows", charset="utf8")
con_redshift = psycopg2.connect(user='operation', password='Operation123',
                                host='jiayundatapro.cls0csjdlwvj.us-west-2.redshift.amazonaws.com',
                                port=5439, database='jiayundata')

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
print(data_table_ca.head(10))

# 画图加载包
import plotly.graph_objects as go
import plotly
import dash  # dash的核心后端
import dash_core_components as dcc  # 交互式组件
import dash_html_components as html  # 代码转html
from dash.dependencies import Input, Output  # 回调
from flask import Flask, render_template
import dash_daq as daq
import dash_table
from plotly.subplots import make_subplots  # 画子图加载包

fig_table_ca = []
app = dash.Dash()

app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>Category  Table DashBoard</title>
        {%favicon%}
        {%css%}
    </head>
    <body>
        <div></div>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
        <div></div>
    </body>
</html>
'''

app.layout = \
    html.Div([
        html.Div([
            dcc.DatePickerRange(
                id='date-picker-range-cate-yewuxian',
                start_date=(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
                end_date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                start_date_placeholder_text="Start Date",
                end_date_placeholder_text="End Date",
                calendar_orientation='vertical'
            )
        ]
        ),
        html.Div([
            html.Label("业务类型：",
                       style={'textAlign': 'left', "width": "100%", 'position': 'relative',
                              'font': {'family': 'Microsoft YaHei', 'size': 15}}),
            dcc.Dropdown(
                id='my-dropdown-table-ca',
                options=[{'label': 'Cf', 'value': 'cf'},
                         {'label': 'Seller', 'value': 'seller'},
                         {'label': 'Total', 'value': 'total'}],
                multi=True,
                value=['total'],
                style={'width': '150px'}
            )
        ],
            style={'margin': 'auto'}
        ),
        html.Div([
            dcc.Graph(
                id='my-graph-table-ca',
                config={"displayModeBar": False},
                style={"height": "80%", "width": "100%"}
            )
        ],
            style={'margin': 'auto'}
        )
    ],
        style={'margin': 'auto', 'margin-right': '5px', 'margin-left': '5px'})


@app.callback(Output('my-graph-table-ca', 'figure'),
              [Input('date-picker-range-cate-yewuxian', 'start_date'),
               Input('date-picker-range-cate-yewuxian', 'end_date'),
               Input('my-dropdown-table-ca', 'value')], )
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
    print(data_table_ca_total.head(10))

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
    print(data_table_ca_total.head(10))

    filter_data_table_ca_total = data_table_ca_total[['yewu_type', 'front_cate_one', 'uv', 'gmv', 'qty', 'cvr',
                                                      'kd_price', 'item_num_mean', 'pv_zb', 'gmv_zb', 'item_num_zb',
                                                      'uv_hb', 'gmv_hb', 'qty_hb', 'cvr_hb', 'kd_price_hb',
                                                      'item_num_hb']]

    filter_data_table_ca_total = filter_data_table_ca_total.sort_values(by='gmv', ascending=False).head(50)  # 降序

    filter_data_table_ca_total.rename(columns={'yewu_type': '业务类型', 'front_cate_one': '类目', 'uv': '访客',
                                               'gmv': '销售', 'qty': '销量', 'cvr': '支付转化',
                                               'kd_price': '客单价', 'item_num_mean': '在售商品数', 'pv_zb': '曝光占比',
                                               'gmv_zb': '销售占比',
                                               'item_num_zb': '在售占比',
                                               'uv_hb': '访客环比', 'gmv_hb': '销售环比', 'qty_hb': '销量环比',
                                               'cvr_hb': '转化环比', 'kd_price_hb': '客单价环比',
                                               'item_num_hb': '在售环比'}, inplace=True)
    print(filter_data_table_ca_total.head(10))

    dropdown_value_table_ca = {"total": "Total", "seller": "Seller", "cf": 'Cf'}
    for table_ca_yewu_type in selected_dropdown_value_table_ca:
        vals_table_ca = \
            [list(filter_data_table_ca_total[filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['访客环比']),
             list(filter_data_table_ca_total[filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['销售环比']),
             list(filter_data_table_ca_total[filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['销量环比']),
             list(filter_data_table_ca_total[filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['转化环比']),
             list(filter_data_table_ca_total[filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['客单价环比']),
             list(filter_data_table_ca_total[filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['在售环比'])]

        font_color_cell_table = \
            ['black'] * 11 + [['red' if v <= 0 else 'green' for v in vals_table_ca[k]] for k in range(6)]

        fig_table_ca = go.Figure(data=[
            go.Table(header=
                     dict(values=list(filter_data_table_ca_total.columns),
                          fill_color='rgb(0, 81, 108)',
                          line=dict(color='rgb(0, 81, 108)', width=0.5),
                          align=['left', 'left', 'center'],
                          height=25,
                          font=dict(size=10, family='Arial', color='rgb(255, 255, 255)')),
                     columnwidth=[5, 10, 5],
                     cells=
                     dict(
                         values=[
                             filter_data_table_ca_total[
                                 filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['业务类型'],
                             filter_data_table_ca_total[
                                 filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['类目'],
                             ((filter_data_table_ca_total[
                                 filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['访客'])/10000).round(1),
                             (filter_data_table_ca_total[
                                 filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['销售']/10000).round(1),
                             (filter_data_table_ca_total[
                                 filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['销量']/10000).round(1),
                             filter_data_table_ca_total[
                                 filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['支付转化'],
                             filter_data_table_ca_total[
                                 filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['客单价'].round(2),
                             (filter_data_table_ca_total[
                                 filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['在售商品数']/10000).round(1),
                             filter_data_table_ca_total[
                                 filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['曝光占比'],
                             filter_data_table_ca_total[
                                 filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['销售占比'],
                             filter_data_table_ca_total[
                                 filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['在售占比'],
                             filter_data_table_ca_total[
                                 filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['访客环比'],
                             filter_data_table_ca_total[
                                 filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['销售环比'],
                             filter_data_table_ca_total[
                                 filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['销量环比'],
                             filter_data_table_ca_total[
                                 filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['转化环比'],
                             filter_data_table_ca_total[
                                 filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['客单价环比'],
                             filter_data_table_ca_total[
                                 filter_data_table_ca_total['业务类型'] == table_ca_yewu_type]['在售环比'],

                         ],
                         fill_color='rgb(255, 255, 255)',
                         line=dict(color='rgb(0, 81, 108)', width=0.5),
                         align=['left', 'left', 'center'],
                         font=dict(size=9, family='Arial', color=font_color_cell_table),
                         format=(None, None, None, None, None, ".2%", None, None, ".2%"),  # 按照列顺序设置格式
                         suffix=(None, None, "w", "w", "w", None, None, "w", None)
                     )
                     )
        ]
        )

        fig_table_ca.update_layout(
            autosize=False,
            height=590,
            title=
            dict(text=
                 f"{start_date}--{end_date} "
                 f"{', '.join(str(dropdown_value_table_ca[i]) for i in selected_dropdown_value_table_ca)} 类目数据看板",
                 font=dict(family='Arial', size=15),
                 x=0.5, y=0.9)
        )

    return fig_table_ca


if __name__ == '__main__':
    app.server.run()
