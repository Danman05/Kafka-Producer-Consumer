from kafka import KafkaConsumer
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.graph_objs as go
import threading
import json
import time

# Project imports
import batch_consumer
import dash_styling # Custom CSS

#region dash initilization
app = dash.Dash(__name__, assets_folder='assets')


# The layout is based of multiple html.Div containers, dcc.Graph components, and a dcc.Interval component. 
# dcc.Interval is a component that triggers a callback periodically, useful for updating data or refreshing components at set intervals.
# dcc.Graph can be used to render any plotly.js-powered data visualization.
# The entire layout is designed to display multiple graphs (large and small) 
app.layout = html.Div([
    dcc.Interval(
        id='interval-component',
        interval=1*1000,
        n_intervals=0,
    ),
    html.Div([
        dcc.Graph(id='live-update-graph'),
    ], style=dash_styling.medium_container),

    html.Div([
        html.Div([
        dcc.Graph(id='small-chart-1', style=dash_styling.small_graph),
        dcc.Graph(id='small-chart-2', style=dash_styling.small_graph),
        ], style=dash_styling.flex),

        html.Div([
        dcc.Graph(id='small-chart-3', style=dash_styling.small_graph),
        dcc.Graph(id='small-chart-4', style=dash_styling.small_graph),
        ], style=dash_styling.flex),

    ], style=dash_styling.small_container),
    html.Div([
        dcc.Graph(id='circle-production'),
    ], style=dash_styling.big_container),
])
#endregion

# Global variable to hold the data
data = [] 

data_lock = threading.Lock() # lock to keep the application thread safe

# region Kafka producer 
def connect_consumer(topic, group_id, bootstrap_servers, offset_type):
    return KafkaConsumer(
        topic,  # Replace with your Kafka topic
        bootstrap_servers=bootstrap_servers,  # Replace with your Kafka broker address
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # Deserialize messages from JSON
        auto_offset_reset=offset_type,  # Start reading at the latest message
    )

def process_batch(consumer, batch_size = 5):
    global data
    # Get the partition information
    for i in range(3):
        try:
            batch = []
            for message in consumer:
                with data_lock:
                    data.append(message.value)
                    batch.append(message.value)
                if len(batch) >= batch_size:
                    # TODO Send batches of data to PySpark
                    # PySpark tasks, what can we compare the batches with.
                    threading.Thread(target=batch_consumer.show_batch, kwargs={'pd_df': batch.copy()}).start()
                    batch = []
                time.sleep(0.5) 

        except Exception as e:
            print(f'Error proccesing batch: {e}') 
        finally:
            consumer.close(),
        time.sleep(2)
    print("Consumer quit")
#endregion


@app.callback(Output('circle-production', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_circle(n):
    global data
    figure = go.Figure()
    if not data:
        return figure
    
    df = pd.DataFrame(list(data))
    latest_entry = df.iloc[-1] if not df.empty else None
    # Exclude 'tick' from the data
    filtered_data = {key: value for key, value in latest_entry.items() if key != 'tick'}
    labels = list(filtered_data.keys())
    values = list(filtered_data.values())
    figure.add_trace(go.Pie(labels=['Iron Plate', 'Copper Plate', 'Electronic Circuit', 'Gear Wheel'], values=values, hole=0.3))
    # Update layout for better presentation
    figure.update_layout(
        title_text="Percentage distribution of item production",
        annotations=[dict(text='Items', x=0.5, y=0.5, font_size=20, showarrow=False)],
    )
    return figure

# Callback function
# Function is linked from the graph with id
@app.callback(Output('live-update-graph', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_graph(n):
    with data_lock:
        global data
        figure = go.Figure()
        if not data:
            return figure 
        
        # Convert deque to DataFrame for easy plotting
        df = pd.DataFrame(list(data))
        df['tick'] = pd.to_datetime(df['tick'], unit='s')

        # Create the figure
        figure = go.Figure()
        figure.add_trace(go.Bar(x=df['tick'], y=df['iron_plate'], marker=dict(color='rgb(211, 211, 211)', cornerradius="30%"), name='Iron Plate'))
        figure.add_trace(go.Bar(x=df['tick'], y=df['copper_plate'], marker=dict(color='rgb(205, 127, 50)', cornerradius="30%"), name='Copper Plate'))
        figure.add_trace(go.Bar(x=df['tick'], y=df['electronic_circuit'], marker=dict(color='rgb(144, 238, 144)', cornerradius="30%"), name='Electronic Circuit'))
        figure.add_trace(go.Bar(x=df['tick'], y=df['gear_wheel'], marker=dict(color='rgb(90, 90, 90)', cornerradius="30%"), name='Gear Wheel'))

        figure.update_layout(title='Batch Production',
                            xaxis_title='Time',
                            yaxis_title='Amount')
        return figure

def single_bar_graph(item, item_name, bar_color, y_range = [0,200]):
    global data
    figure = go.Figure()
    if not data:
        return figure
    df = pd.DataFrame(list(data))
    # Use only the last entry for the latest 'tick' value
    latest_entry = df.iloc[-1] if not df.empty else None

    if latest_entry is not None:
        figure.add_trace(go.Bar(x=[latest_entry['tick']], y=[latest_entry[item]], name=item_name, marker=dict(color=f'rgb({bar_color})')))
        figure.update_layout(
                title=item_name,
                yaxis=dict(range=y_range),
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        )
    return figure

#region Small graphs
@app.callback(Output('small-chart-1', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_iron_single(n):
    return single_bar_graph(item='iron_plate', item_name='Iron Plate', y_range=[0,200], bar_color='211, 211, 211')

@app.callback(Output('small-chart-2', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_copper_single(n):
    return single_bar_graph(item='copper_plate', item_name='Copper Plate',y_range=[0,200],bar_color='205, 127, 50')

@app.callback(Output('small-chart-3', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_electronic_single(n):
    return single_bar_graph(item='electronic_circuit', item_name='Electronic Circuit',y_range=[0,200],bar_color='144, 238, 144')

@app.callback(Output('small-chart-4', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_gear_single(n):
    return single_bar_graph(item='gear_wheel', item_name='Gear Wheel', y_range=[0,200], bar_color='90, 90, 90')
#endregion

# Run the app
# Kafka consumer context is created
# A background thread is created and its job is to be a kafka consumer
# Dash server is started
try:
    consumer = connect_consumer(
        topic='factorio-data-v2',
        group_id='data-v2',
        offset_type='latest',
        bootstrap_servers=['localhost:9092'])
    
    threading.Thread(target=process_batch, kwargs={'consumer': consumer}, daemon=True).start() 

    app.run_server(debug=True, port=8050)
except Exception as e:
    print(f"Error running app: {e}")