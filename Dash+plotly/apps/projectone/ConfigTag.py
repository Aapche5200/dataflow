import dash_core_components as dcc
import dash_html_components as html


def buld_modal_info_overlay(id, side, content):
    div = html.Div([  # modal div
        html.Div([  # content div
            html.Div([
                html.H6([
                    "Tips:",
                    html.Br(),
                    html.Label(
                        id=f'close-{id}-modal',
                        children="×",
                        n_clicks=0,
                        className='info-icon',
                        style={'margin': 0},
                    ),
                ], className="container_title", style={'color': 'white'}),

                dcc.Markdown(
                    content
                ),
            ])
        ],
            className=f'modal-content {side}',
        ),
        html.Div(className='modal')
    ],
        id=f"{id}-modal",
        style={"display": "none"},
    )

    return div
