import dash_html_components as html


def indicator(color, text, id_value):
    return html.Div(
        [
            html.P(id=id_value,),
            html.P(text, className="two columns "),
        ],
        className="two columns",
    )
