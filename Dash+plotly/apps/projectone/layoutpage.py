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
                            "首页",
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
                            "店铺",
                            href="/pages/Shop",
                            className="tab",
                        ),
                        dcc.Link(
                            "退货",
                            href="/pages/Return",
                            className="tab",
                        ),
                        dcc.Link(
                            "流量",
                            href="/pages/WholeeCateFlowType",
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
                            "物流",
                            href="/apps/app2",
                            className="tab",
                        ),
                        dcc.Link(
                            "自助取数",
                            href="/pages/AutoFetch",
                            className="tab",
                        ),
                        dcc.Link(
                            "Help",
                            href="/pages/Help",
                            className="tab",
                        ),
                    ],
                    className="row all-tabs study-browser-banner"
                ),
                html.Marquee(
                    '流量栏：新增类目场景数据看板 //Wholee Dau&Wholee类目看板&日Wholee会员数据-每天更新中//及时关注Help专栏',
                    # 选择日期，查看对应日期数据；模块右上方有问号可以参考本模块详细说明
                    id='tongzhi',
                    dir='left',
                    style={'font-family': 'Microsoft YaHei', 'font-size': '13px',
                           'color': 'red',
                           'border': '0px'})
            ],
        )
    return menu
