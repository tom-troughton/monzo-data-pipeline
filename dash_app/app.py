import dash
from dash import html, dcc, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import sqlite3
from pathlib import Path
import base64
import io

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Layout
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("SQLite Database Explorer", className="text-center mb-4"),
            dcc.Upload(
                id='upload-database',
                children=html.Div([
                    'Drag and Drop or ',
                    html.A('Select SQLite Database')
                ]),
                style={
                    'width': '100%',
                    'height': '60px',
                    'lineHeight': '60px',
                    'borderWidth': '1px',
                    'borderStyle': 'dashed',
                    'borderRadius': '5px',
                    'textAlign': 'center',
                    'margin': '10px'
                },
                multiple=False
            ),
        ])
    ]),
    
    dbc.Row([
        dbc.Col([
            html.Div(id='table-selector-container', children=[
                dcc.Dropdown(
                    id='table-selector',
                    placeholder='Select a table',
                    style={'display': 'none'}
                )
            ]),
            html.Div(id='column-selector-container', children=[
                dcc.Dropdown(
                    id='x-axis-selector',
                    placeholder='Select X-axis',
                    style={'display': 'none'}
                ),
                dcc.Dropdown(
                    id='y-axis-selector',
                    placeholder='Select Y-axis',
                    style={'display': 'none'}
                )
            ]),
            dcc.Graph(id='visualization')
        ])
    ])
])

def parse_db_contents(contents):
    if contents is None:
        return None
    
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    
    # Save the uploaded file temporarily
    temp_db_path = '/tmp/temp_database.db'
    with open(temp_db_path, 'wb') as f:
        f.write(decoded)
    
    return temp_db_path

@app.callback(
    Output('table-selector', 'options'),
    Output('table-selector', 'style'),
    Input('upload-database', 'contents')
)
def update_table_selector(contents):
    if contents is None:
        return [], {'display': 'none'}
    
    db_path = parse_db_contents(contents)
    conn = sqlite3.connect(db_path)
    
    # Get list of tables
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    conn.close()
    
    options = [{'label': table[0], 'value': table[0]} for table in tables]
    return options, {'display': 'block', 'margin': '10px'}

@app.callback(
    [Output('x-axis-selector', 'options'),
     Output('y-axis-selector', 'options'),
     Output('x-axis-selector', 'style'),
     Output('y-axis-selector', 'style')],
    [Input('table-selector', 'value'),
     Input('upload-database', 'contents')]
)
def update_column_selectors(selected_table, contents):
    if contents is None or selected_table is None:
        return [], [], {'display': 'none'}, {'display': 'none'}
    
    db_path = parse_db_contents(contents)
    conn = sqlite3.connect(db_path)
    
    # Get column names
    df = pd.read_sql_query(f"SELECT * FROM {selected_table} LIMIT 1", conn)
    conn.close()
    
    columns = [{'label': col, 'value': col} for col in df.columns]
    style = {'display': 'block', 'margin': '10px'}
    
    return columns, columns, style, style

@app.callback(
    Output('visualization', 'figure'),
    [Input('x-axis-selector', 'value'),
     Input('y-axis-selector', 'value'),
     Input('table-selector', 'value'),
     Input('upload-database', 'contents')]
)
def update_visualization(x_axis, y_axis, selected_table, contents):
    if None in [x_axis, y_axis, selected_table, contents]:
        return {}
    
    db_path = parse_db_contents(contents)
    conn = sqlite3.connect(db_path)
    
    # Read data
    df = pd.read_sql_query(f"SELECT {x_axis}, {y_axis} FROM {selected_table}", conn)
    conn.close()
    
    # Create visualization
    fig = px.scatter(df, x=x_axis, y=y_axis, title=f'{selected_table}: {y_axis} vs {x_axis}')
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)