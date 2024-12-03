import dash
from dash import html, dcc, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sqlite3
import base64
import numpy as np
import tempfile

# Initialize app with a light theme
app = dash.Dash(__name__, 
                external_stylesheets=[dbc.themes.LITERA],  # Light, modern theme
                meta_tags=[{'name': 'viewport', 'content': 'width=device-width, initial-scale=1'}])

# Custom CSS for cards and layout
CARD_STYLE = {
    'box-shadow': '0 4px 6px 0 rgba(0, 0, 0, 0.1)',
    'margin': '10px',
    'padding': '15px',
    'background-color': 'white',
    'border-radius': '8px',
    'border': 'none'
}

# Layout
app.layout = dbc.Container([
    # Header
    dbc.Row([
        dbc.Col([
            html.H1("Database Analytics Dashboard", 
                   className="text-primary text-center my-4",
                   style={'font-weight': '600'})
        ])
    ]),
    
    # Database Upload Card
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Upload Database", className="card-title"),
                    dcc.Upload(
                        id='upload-database',
                        children=html.Div([
                            html.I(className="fas fa-database me-2"),
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
            ], style=CARD_STYLE)
        ])
    ]),
    
    # Controls Card
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Data Selection", className="card-title"),
                    dcc.Dropdown(id='table-selector', placeholder='Select a table', className="mb-3"),
                    dcc.Dropdown(id='x-axis-selector', placeholder='Select X-axis', className="mb-2"),
                    dcc.Dropdown(id='y-axis-selector', placeholder='Select Y-axis', className="mb-2"),
                ])
            ], style=CARD_STYLE)
        ])
    ]),
    
    # Visualizations
    dbc.Row([
        # Scatter Plot Card
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Scatter Plot", className="card-title"),
                    dcc.Graph(id='scatter-plot')
                ])
            ], style=CARD_STYLE)
        ], md=6),
        
        # Line Plot Card
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Line Plot", className="card-title"),
                    dcc.Graph(id='line-plot')
                ])
            ], style=CARD_STYLE)
        ], md=6),
    ]),
    
    dbc.Row([
        # Bar Chart Card
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Bar Chart", className="card-title"),
                    dcc.Graph(id='bar-chart')
                ])
            ], style=CARD_STYLE)
        ], md=6),
        
        # Box Plot Card
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Box Plot", className="card-title"),
                    dcc.Graph(id='box-plot')
                ])
            ], style=CARD_STYLE)
        ], md=6),
    ]),
    
    # Stats Card
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Data Statistics", className="card-title"),
                    html.Div(id='stats-table')
                ])
            ], style=CARD_STYLE)
        ])
    ]),
    
], fluid=True, className="bg-light min-vh-100 py-3")

# Callback functions (previous callbacks remain the same)
def parse_db_contents(contents):
    if contents is None:
        return None
    try:
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        
        # Create a unique temporary file name
        temp_dir = tempfile.gettempdir()
        temp_db_path = f"{temp_dir}/temp_database_{hash(content_string)}.db"
        
        with open(temp_db_path, 'wb') as f:
            f.write(decoded)
        
        return temp_db_path
    except Exception as e:
        print(f"Error parsing database contents: {e}")
        return None

@app.callback(
    [Output('scatter-plot', 'figure'),
     Output('line-plot', 'figure'),
     Output('bar-chart', 'figure'),
     Output('box-plot', 'figure'),
     Output('stats-table', 'children')],
    [Input('x-axis-selector', 'value'),
     Input('y-axis-selector', 'value'),
     Input('table-selector', 'value'),
     Input('upload-database', 'contents')],
    prevent_initial_call=True
)
def update_visualizations(x_axis, y_axis, selected_table, contents):
    if None in [x_axis, y_axis, selected_table, contents]:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="No data selected",
            annotations=[{
                'text': "Please select a database and columns to visualize",
                'xref': "paper",
                'yref': "paper",
                'showarrow': False,
                'font': {'size': 20}
            }]
        )
        return empty_fig, empty_fig, empty_fig, empty_fig, None
    
    try:
        db_path = parse_db_contents(contents)
        if db_path is None:
            raise Exception("Failed to parse database file")
            
        conn = sqlite3.connect(db_path)
        
        # Read data
        df = pd.read_sql_query(f"SELECT * FROM {selected_table}", conn)
        conn.close()
        
        # Scatter Plot
        scatter_fig = px.scatter(df, x=x_axis, y=y_axis, 
                               title=f'Scatter Plot: {y_axis} vs {x_axis}',
                               template='plotly_white')
        
        # Line Plot
        line_fig = px.line(df, x=x_axis, y=y_axis, 
                          title=f'Line Plot: {y_axis} vs {x_axis}',
                          template='plotly_white')
        
        # Bar Chart
        bar_fig = px.bar(df, x=x_axis, y=y_axis, 
                        title=f'Bar Chart: {y_axis} by {x_axis}',
                        template='plotly_white')
        
        # Box Plot
        box_fig = px.box(df, x=x_axis, y=y_axis, 
                        title=f'Box Plot: Distribution of {y_axis} by {x_axis}',
                        template='plotly_white')
        
        # Statistics Table
        stats_df = df[[x_axis, y_axis]].describe()
        stats_table = dbc.Table.from_dataframe(stats_df, 
                                             striped=True, 
                                             bordered=True, 
                                             hover=True,
                                             className="stats-table")
        
        return scatter_fig, line_fig, bar_fig, box_fig, stats_table
        
    except Exception as e:
        print(f"Error updating visualizations: {e}")
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="Error loading data",
            annotations=[{
                'text': str(e),
                'xref': "paper",
                'yref': "paper",
                'showarrow': False,
                'font': {'size': 20}
            }]
        )
        return empty_fig, empty_fig, empty_fig, empty_fig, None

# Add these callbacks after the existing ones

@app.callback(
    [Output('table-selector', 'options'),
     Output('table-selector', 'value')],
    [Input('upload-database', 'contents')],
    prevent_initial_call=True
)
def update_table_selector(contents):
    if contents is None:
        return [], None
    
    try:
        db_path = parse_db_contents(contents)
        conn = sqlite3.connect(db_path)
        
        # Get list of tables
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        conn.close()
        
        options = [{'label': table[0], 'value': table[0]} for table in tables]
        return options, options[0]['value'] if options else None
    except Exception as e:
        print(f"Error loading database: {e}")
        return [], None

@app.callback(
    [Output('x-axis-selector', 'options'),
     Output('y-axis-selector', 'options'),
     Output('x-axis-selector', 'value'),
     Output('y-axis-selector', 'value')],
    [Input('table-selector', 'value'),
     Input('upload-database', 'contents')],
    prevent_initial_call=True
)
def update_column_selectors(selected_table, contents):
    if contents is None or selected_table is None:
        return [], [], None, None
    
    try:
        db_path = parse_db_contents(contents)
        conn = sqlite3.connect(db_path)
        
        # Get column names
        df = pd.read_sql_query(f"SELECT * FROM {selected_table} LIMIT 1", conn)
        conn.close()
        
        columns = [{'label': col, 'value': col} for col in df.columns]
        
        # Select first two columns as default
        default_x = columns[0]['value'] if columns else None
        default_y = columns[1]['value'] if len(columns) > 1 else None
        
        return columns, columns, default_x, default_y
    except Exception as e:
        print(f"Error updating columns: {e}")
        return [], [], None, None

if __name__ == '__main__':
    app.run_server(debug=True)