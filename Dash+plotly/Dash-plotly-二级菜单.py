import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import psycopg2
import time
import pymssql

con_mssql = pymssql.connect("172.16.92.2", "sa", "yssshushan2008", "CFflows", charset="utf8")
con_red_atuofetch = psycopg2.connect(user='operation',
                                     password='Operation123',
                                     host='jiayundatapro.cls0csjdlwvj.us-west-2.redshift.amazonaws.com',
                                     port=5439,
                                     database='jiayundata')
df = pd.read_excel('/Volumes/D/数据库/A/映射关系/category_relation_new20200110.xlsx', sheet_name='Sheet1')

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

autofetch_front_cate_one = ['Women\'s Shoes', 'Women\'s Clothing', 'Women\'s Bags', 'Watches', 'Men\'s Shoes',
                            'Men\'s Clothing', 'Men\'s Bags', 'Jewelry & Accessories', 'Home Appliances',
                            'Mobiles & Accessories', 'Electronics', ]

autofetch_front_cate_two = {
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

app = dash.Dash()
app.layout = \
    html.Div([
        '选择一级类目：',
        dcc.Dropdown(
            id='autofetch-front-cate-one',
            value='Home Appliances',
            options=[{'label': v, 'value': v} for v in autofetch_front_cate_one],
            # persistence=True,
            # multi=True
        ),
        html.Br(),

        '选择二级类目：',
        html.Div(
            dcc.Dropdown(
                id='autofetch-front-cate-two',
                # multi=True
            ),
            id='autofetch-front-cate-two-container'),
        html.Br(),
        html.Div(id='autofetch-download-container')
    ])


@app.callback(Output('autofetch-front-cate-two-container', 'children'),
              [Input('autofetch-front-cate-one', 'value')]
              )
def set_front_cate_two(autofetch_cate_one):
    autofetch_cate_two = autofetch_front_cate_two[autofetch_cate_one]
    return dcc.Dropdown(
        id='autofetch-front-cate-two',
        value=autofetch_cate_two[0],
        options=[{'label': v, 'value': v} for v in autofetch_cate_two],
        # persistence_type='session',
        # persistence=cate_one,
        # multi=True
    )


@app.callback(Output('autofetch-download-container', 'children'),
              [Input('autofetch-front-cate-one', 'value'), Input('autofetch-front-cate-two', 'value')]
              )
def set_choice_cate_out(autofetch_cate_one, autofetch_cate_two_out):
    if autofetch_cate_two_out == '总':
        start_time_cateone = time.clock()
        sql_autofetch_cateone = """
        SELECT item_no,front_cate_one,front_cate_two,front_cate_three,product_level,write_uid
        FROM jiayundw_dim.product_basic_info_df
        WHERE active=1 and front_cate_one='{0}' and write_uid=5 
        limit 10
        """.format(autofetch_cate_one)

        data_autofetch_cateone = pd.read_sql(sql_autofetch_cateone, con_red_atuofetch)
        print(data_autofetch_cateone.head(1))
        end_time_cateone = time.clock()
        haoshi_cate = end_time_cateone - start_time_cateone
        print('总')

    else:
        start_time_catetwo = time.clock()
        sql_autofetch_catetwo = """
        SELECT item_no,front_cate_one,front_cate_two,front_cate_three,product_level,write_uid
        FROM jiayundw_dim.product_basic_info_df
        WHERE active=1 and front_cate_one='{0}' and front_cate_two='{1}' and write_uid=5 
        limit 10
        """.format(autofetch_cate_one, autofetch_cate_two_out)

        data_autofetch_catetwo = pd.read_sql(sql_autofetch_catetwo, con_red_atuofetch)
        print(data_autofetch_catetwo.head(1))
        end_time_catetwo = time.clock()
        haoshi_cate = end_time_catetwo - start_time_catetwo
        print('二级')

    return '您当前选择的是：{0}, {1}' \
           '一共耗时：{2}'.format(autofetch_cate_one, autofetch_cate_two_out, haoshi_cate)


if __name__ == '__main__':
    app.run_server()
