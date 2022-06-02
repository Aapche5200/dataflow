import dash
import dash_auth

#  eager_loading=True 禁用延迟加载，从而使该应用程序加载所有异步资源
app = dash.Dash(eager_loading=True)  # meta_tags=[{'content': 'width=device-width'}],
VALID_USERNAME_PASSWORD_PAIRS = {
    'shangpingzhongtai223': 'shangpingzhongtai223'
}

auth = dash_auth.BasicAuth(
    app,
    VALID_USERNAME_PASSWORD_PAIRS

)

server = app.server
app.config.suppress_callback_exceptions = True
app.css.config.serve_locally = True
app.scripts.config.serve_locally = True


