import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import sqlite3
import boto3
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path='./src/.env')

# Initialize the Dash app with Bootstrap theme
app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.FLATLY,
        # Add Google Fonts stylesheet
        'https://fonts.googleapis.com/css2?family=Helvetica+Neue&display=swap'
    ],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}]
)

# Add custom CSS to apply the font globally
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            * {
                font-family: "Helvetica Neue", Helvetica, Arial, sans-serif !important;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# S3 Configuration
S3_BUCKET = os.getenv('S3_BUCKET_NAME')
S3_DB_KEY = os.getenv('DB_KEY')
LOCAL_DB_PATH = './data/database_files/dashboard/dash_app_dashboard.db'

def download_db():
    """Download SQLite database from S3"""
    s3_client = boto3.client('s3')
    try:
        s3_client.download_file(S3_BUCKET, S3_DB_KEY, LOCAL_DB_PATH)
    except Exception as e:
        print(f"Error downloading database: {e}")

def load_data():
    """Load data from SQLite database"""
    download_db()
    conn = sqlite3.connect(LOCAL_DB_PATH)
    
    daily_spend_df = pd.read_sql_query("""
        SELECT transaction_date, total_spend
        FROM daily_transactions_summary
        WHERE transaction_date >= date('now', '-30 days')
        ORDER BY transaction_date
    """, conn)
    
    category_spend_df = pd.read_sql_query("""
        SELECT category, total_spend
        FROM category_spending_summary
        ORDER BY total_spend DESC
        LIMIT 10
    """, conn)
    
    conn.close()
    return daily_spend_df, category_spend_df

def create_daily_spend_chart(df):
    """Create daily spending bar chart"""
    fig = px.bar(
        df,
        x='transaction_date',
        y='total_spend',
        title=None,
        labels={
            'transaction_date': 'Date',
            'total_spend': 'Total Spend (£)'
        }
    )
    fig.update_layout(
        xaxis_tickangle=-45,
        bargap=0.2,
        margin=dict(l=10, r=10, t=20, b=10),
        plot_bgcolor='white',
        paper_bgcolor='white',
        height=400,
        font_family="Helvetica Neue",
        font={
            'size': 12,
            'color': '#2c3e50'
        },
        showlegend=False,
        yaxis=dict(
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(211, 211, 211, 0.5)'
        ),
        xaxis=dict(
            showgrid=False,
            tickfont={'size': 10}
        )
    )
    
    fig.update_traces(
        marker_color='#3498db',
        marker_line_color='#2980b9',
        marker_line_width=1,
        hovertemplate='Date: %{x}<br>Spend: £%{y:.2f}<extra></extra>'
    )
    
    return fig

def create_category_spend_chart(df):
    """Create category spending pie chart"""
    fig = px.pie(
        df,
        values='total_spend',
        names='category',
        title=None,  # Remove title as it's in the card header
        hole=0.3
    )
    fig.update_layout(
        margin=dict(l=10, r=10, t=20, b=10),
        paper_bgcolor='white',
        height=400,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.3,
            xanchor="center",
            x=0.5
        )
    )
    return fig

# Navbar
navbar = dbc.Navbar(
    dbc.Container([
        html.A(
            dbc.Row([
                dbc.Col(html.I(className="fas fa-wallet me-2")),
                dbc.Col(dbc.NavbarBrand("Monzo Dashboard", className="ms-2")),
            ],
            align="center",
            className="g-0",
            ),
            style={"textDecoration": "none"},
        )
    ]),
    color="primary",
    dark=True,
    className="mb-4",
)

# Define the layout
app.layout = html.Div([
    navbar,
    dbc.Container([
        # Summary Cards Row
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("Daily Spending", className="card-title"),
                        dcc.Graph(
                            id='daily-spend-chart',
                            config={'displayModeBar': False}
                        )
                    ])
                ], className="shadow-sm")
            ], md=12, lg=8, className="mb-4"),
            
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("Spending by Category", className="card-title"),
                        dcc.Graph(
                            id='category-spend-chart',
                            config={'displayModeBar': False}
                        )
                    ])
                ], className="shadow-sm")
            ], md=12, lg=4, className="mb-4"),
        ]),
        
        # Auto-refresh component
        dcc.Interval(
            id='interval-component',
            interval=300*1000,  # refresh every 5 minutes
            n_intervals=0
        )
    ], fluid=True)
])

@app.callback(
    [dash.Output('daily-spend-chart', 'figure'),
     dash.Output('category-spend-chart', 'figure')],
    [dash.Input('interval-component', 'n_intervals')]
)
def update_charts(_):
    """Update charts with fresh data"""
    daily_spend_df, category_spend_df = load_data()
    
    daily_chart = create_daily_spend_chart(daily_spend_df)
    category_chart = create_category_spend_chart(category_spend_df)
    
    return daily_chart, category_chart

if __name__ == '__main__':
    app.run_server(debug=True)