import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go

app = dash.Dash()

fruits = {'Day': [1, 2, 3, 4, 5, 6],
          'Visitors': [43, 34, 65, 56, 29, 76],
          'Bounce_Rate': [65, 67, 78, 65, 45, 52],
          'Nice_Fruits': ['apple', 'apple', 'grape', 'apple', 'grape', 'grape']}
df_all_fruits = pd.DataFrame(fruits)

Nice_Fruits_list = df_all_fruits['Nice_Fruits'].unique()

app.layout = html.Div(children=[
    html.H1('Payment Curve and predictor'),

    html.Label('fruits_1'),
    dcc.Checklist(
        id='fruits_checklist',
        options=[{'label': i, 'value': i} for i in Nice_Fruits_list],
        value=['apple', 'grape'],
        labelStyle={'display': 'inline-block'}
    ),

    dcc.Graph(
        id='fruits_graph',
        figure={
            'data': [
                go.Scatter(
                    x=df_all_fruits['Visitors'],
                    y=df_all_fruits['Bounce_Rate'],
                    mode='markers',
                    opacity=0.7,
                    marker={
                        'size': 15,
                        'line': {'width': 0.5, 'color': 'white'}
                    },
                )
            ],
            'layout': go.Layout(
                xaxis={'type': 'linear', 'title': 'Visitors'},
                yaxis={'title': 'Bounce_Rate'},
                margin={'l': 40, 'b': 40, 't': 10, 'r': 10},
                hovermode='closest'
            )
        }
    ),

])


@app.callback(
    Output('fruits_graph', 'figure'),
    [Input('fruits_checklist', 'value')]
)
def update_graph(values):
    df = df_all_fruits

    return {'data': [go.Scatter(
        x=df[df['Nice_Fruits'] == v]['Visitors'],
        y=df[df['Nice_Fruits'] == v]['Bounce_Rate'],
        mode='markers',
        marker={
            'size': 15,
            'opacity': 0.5,
            'line': {'width': 0.5, 'color': 'white'}
        }
    ) for v in values],
        'layout': {'margin': {'l': 40, 'r': 0, 't': 20, 'b': 30}}
    }


if __name__ == '__main__':
    app.run_server()
