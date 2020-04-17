import dash_html_components as html
import dash_core_components as dcc


def header(app):
    return html.Div([get_menu()])


def get_menu():
    menu = \
        html.Div(
            [
                html.Div(
                    className="study-browser-banner row",
                    children=[
                        html.H2(className="h2-title", children="CF DashBoard"),
                        # html.H2(className="h2-title-mobile", children="CF数据魔方"),
                    ],
                ),
                html.Div(
                    [
                        dcc.Link(
                            "首页测试",
                            href="/pages/DashTest",
                            className="tab",
                        ),
                        dcc.Link(
                            "交易",
                            href="/pages/CategoryBoard",
                            className="tab first",
                        ),
                        dcc.Link(
                            "类目看板",
                            href="/pages/DataDashBoard",
                            className="tab",
                        ),
                        dcc.Link(
                            "市场",
                            href="/apps/app2",
                            className="tab",
                        ),
                        dcc.Link(
                            "退货",
                            href="/pages/Return",
                            className="tab",
                        ),
                        dcc.Link(
                            "流量",
                            href="/apps/app2",
                            className="tab",
                        ),
                        dcc.Link(
                            "用户",
                            href="/pages/DAU",
                            className="tab",
                        ),
                        dcc.Link(
                            "品类",
                            href="/pages/GoodsSLN",
                            className="tab",
                        ),
                        dcc.Link(
                            "服务",
                            href="/apps/app2",
                            className="tab",
                        ),
                        dcc.Link(
                            "营销",
                            href="/apps/app2",
                            className="tab",
                        ),
                        dcc.Link(
                            "物流",
                            href="/apps/app2",
                            className="tab",
                        ),
                        dcc.Link(
                            "自助取数",
                            href="/pages/AutoFetch",
                            className="tab",
                        ),
                    ],
                    className="row all-tabs study-browser-banner"
                ),
                html.Marquee(
                    '通知：所有端口数据停止更新！！！',  # 周一至周五18:00--次日9:00及周末进行系统维护
                    id='tongzhi',
                    dir='left',
                    style={'font-family': 'Microsoft YaHei', 'font-size': '9px',
                           'color': 'red',
                           'border': '0px'})
            ],
        )
    return menu
