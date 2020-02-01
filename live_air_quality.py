# https://dash.plot.ly/live-updates
import datetime
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly

import happybase
CONNECTION = happybase.Connection('localhost', 9090)
TABLE_NAME = "air-quality"
COUNTRY = "US"
CITY = "Fairfield"
table = CONNECTION.table(TABLE_NAME)

def get_max_by_day(str_day):
    max_val = '0.0'
    for k, data in table.scan(row_prefix=bytes(str_day, 'utf8')):
        value = data[b"cf:value"].decode("utf-8")
        if float(value) > float(max_val): max_val = value
    return max_val

def analyze_history(number_of_day):
    data_out = {
        'value': [],
        'time': []
    }
    for i in range(number_of_day):
        i_days_ago = datetime.datetime.now() - datetime.timedelta(days=i+1)
        day = i_days_ago.strftime("%Y-%m-%d")
        value = get_max_by_day(day)
        data_out["value"].append(value)
        data_out["time"].append(day)

    return data_out

HISTORY_DATA = analyze_history(10)

def collect_live_data():
    data_out = {
        'time': [],
        'latitude': [],
        'longitude': [],
        'value': []
    }
    # fields = [b"cf:country", b"cf:city", b"cf:location", b"cf:latitude", b"cf:longitude", b"cf:value", b"cf:timestamp"]
    for k, data in table.scan(row_prefix=bytes(datetime.datetime.now().strftime("%Y-%m-%d"), 'utf8')):
        print(k, data)
        time = data[b"cf:timestamp"].decode("utf-8")
        latitude = data[b"cf:latitude"].decode("utf-8")
        longitude = data[b"cf:latitude"].decode("utf-8")
        value = data[b"cf:value"].decode("utf-8")
        data_out["time"].append(time)
        data_out["latitude"].append(latitude)
        data_out["longitude"].append(longitude)
        data_out["value"].append(value)

    return data_out


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = html.Div(
    html.Div([
        html.H4('Live Air Quality Feed'),
        html.Div(id='live-update-text'),
        dcc.Graph(id='live-update-graph'),
        dcc.Interval(
            id='interval-component',
            interval=5*1000, # in milliseconds
            n_intervals=0
        )
    ])
)


@app.callback(Output('live-update-text', 'children'),
              [Input('interval-component', 'n_intervals')])
def update_metrics(n):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    style = {'padding': '5px', 'fontSize': '16px'}
    return [
        html.Span('Country: {}'.format(COUNTRY), style=style),
        html.Span('City: {}'.format(CITY), style=style),
        html.Span('Current time: {}'.format(now), style=style)
    ]

# Multiple components can update everytime interval gets fired.
@app.callback(Output('live-update-graph', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_graph_live(n):
    # Collect some data
    data = collect_live_data()

    # Create the graph with subplots
    fig = plotly.tools.make_subplots(rows=2, cols=1, vertical_spacing=0.2)
    fig['layout']['margin'] = {
        'l': 30, 'r': 10, 'b': 30, 't': 10
    }
    fig['layout']['legend'] = {'x': 0, 'y': 1, 'xanchor': 'left'}

    fig.append_trace({
        'x': data['time'],
        'y': data['value'],
        'name': 'live AQ',
        'mode': 'lines+markers',
        'type': 'scatter'
    }, 1, 1)

    fig.append_trace({
        'x': HISTORY_DATA['time'],
        'y': HISTORY_DATA['value'],
        # 'text': data['time'],
        'name': '10 day ago',
        'mode': 'lines+markers',
        'type': 'scatter'
    }, 2, 1)

    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
