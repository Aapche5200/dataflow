import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import sys
import os
from flask import send_from_directory
import dash_auth
sys.path
sys.path.append('/Users/apache/PycharmProjects/shushan-CF/Dash+plotly/apps/projectone')
import appshudashboard
from appshudashboard import app
from pages import DataDashBoard
from pages import CategoryBoard
from pages import AutoFetch
from pages import DataDash
from pages import GoodsSLN
from pages import Return
from pages import DAU
from pages import Shop
from pages import Help
from pages import WholeeCateFlowType

app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>Appshu_DashBoard</title>
        {%favicon%}
        {%css%}
    </head>
    <body>
        <div>
        </div>
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

app.layout = html.Div([
    html.Link(
        rel='stylesheet',
        href='/css/menu_header.css'
    ),
    html.Link(
        rel='stylesheet',
        href='/css/base.css'
    ),
    html.Link(
        rel='stylesheet',
        href='/css/custom.css'
    ),
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content'),
])


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/pages/CategoryBoard':
        return CategoryBoard.layout
    elif pathname == '/pages/DataDashBoard':
        return DataDashBoard.layout
    elif pathname == '/pages/AutoFetch':
        return AutoFetch.layout
    elif pathname == '/pages/GoodsSLN':
        return GoodsSLN.layout
    elif pathname == '/pages/Return':
        return Return.layout
    elif pathname == '/pages/DAU':
        return DAU.layout
    elif pathname == '/pages/WholeeCateFlowType':
        return WholeeCateFlowType.layout
    elif pathname == '/pages/Shop':
        return Shop.layout
    elif pathname == '/pages/Help':
        return Help.layout
    # elif pathname == '/pages/DashTest':
    #     return DashTest.layout
    elif pathname == '/apps/app2':
        return "项目开发中，请期待~by:shushan"
    else:
        return DataDash.layout


@app.server.route('/css/<path:path>')
def static_file(path):
    static_folder = os.path.join(os.getcwd(), 'css')
    return send_from_directory(static_folder, path)


if __name__ == '__main__':
    app.server.run() #'192.168.128.5'
