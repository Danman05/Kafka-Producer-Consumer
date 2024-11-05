from kafka import KafkaConsumer
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.graph_objs as go
import threading
import json
import time

# Initialize Dash app
app = dash.Dash(__name__)
# Global variable to hold the data
data = []  # Use a deque with a max length to limit data size
GRAPH_UPDATE_INTERVAL = 2 
data_lock = threading.Lock()

# Configure the 
def connect_consumer(topic, group_id, bootstrap_servers, offset_type):
    return KafkaConsumer(
        topic,  # Replace with your Kafka topic
        bootstrap_servers=bootstrap_servers,  # Replace with your Kafka broker address
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # Deserialize messages from JSON
        auto_offset_reset=offset_type,  # Start reading at the latest message
    )
def process_batch(consumer, batch_size = 10):
    # Get the partition information
    try:
        global data
        for message in consumer:
            with data_lock:
                data.append(message.value)
            if len(data) >= batch_size:
                print(f"Processing batch")
            time.sleep(0.3)  # Reduce the sleep time to make updates faster if needed
            # Process any remaining messages in the batch

        if data:
            print(f"Processing last batch: {data}")
        # Add your batch processing logic here

    except Exception as e:
        print(f'Error proccesing batch: {e}') 
    finally:
        consumer.close()

# Dash layout
app.layout = html.Div([
    html.H1("Production Overview"),
    dcc.Graph(id='live-update-graph'),
    dcc.Interval(
        id='interval-component',
        interval=GRAPH_UPDATE_INTERVAL*1000,  # Update every second
        n_intervals=0,
    ),
    dcc.Graph(id='circle-production'),
])
@app.callback(Output('circle-production', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_circle(n):
    with data_lock:
        global data
        figure = go.Figure()
        if not data:
            return figure
        
        # Exclude 'tick' from the data
        filtered_data = {key: value for key, value in data[0].items() if key != 'tick'}
        labels = list(filtered_data.keys())
        values = list(filtered_data.values())
        figure.add_trace(go.Pie(labels=labels, values=values, hole=0.3))
        # Update layout for better presentation
        figure.update_layout(
            title_text="Percentage Distribution of Items in Batch",
            annotations=[dict(text='Items', x=0.5, y=0.5, font_size=20, showarrow=False)]
        )
        return figure
    
# Callback function
# Function is linked from the graph with id
@app.callback(Output('live-update-graph', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_graph(n):
    with data_lock:
        global data
        #print(f"Data in graph: {list(data)}")  # Debug: check data content
        if not data:
            return go.Figure()  # Return an empty figure if no data available
        
        # Convert deque to DataFrame for easy plotting
        df = pd.DataFrame(list(data))
        df['tick'] = pd.to_datetime(df['tick'], unit='s')

        # Create the figure
        figure = go.Figure()
        figure.add_trace(go.Bar(x=df['tick'], y=df['iron_plate'], marker=dict(cornerradius="30%"), name='Iron Plate'))
        figure.add_trace(go.Bar(x=df['tick'], y=df['copper_plate'], marker=dict(cornerradius="30%"), name='Copper Plate'))
        figure.add_trace(go.Bar(x=df['tick'], y=df['gear_wheel'], marker=dict(cornerradius="30%"), name='Gear Wheel'))
        figure.add_trace(go.Bar(x=df['tick'], y=df['electronic_circuit'], marker=dict(cornerradius="30%"), name='Electronic Circuit'))

        figure.update_layout(title='Batch Production',
                            xaxis_title='Time',
                            yaxis_title='Amount')
        return figure
    
# Run the app
# Kafka consumer context is created
# A background thread is created and its job is to be a kafka consumer
# Dash server is started
try:
    consumer = connect_consumer(
        topic='factorio-data-v2',
        group_id='group-f',
        offset_type='earliest',
        bootstrap_servers=['localhost:9092'])
    
    threading.Thread(target=process_batch, kwargs={'consumer': consumer}, daemon=True).start() 

    app.run_server(debug=True, port=8050)
except Exception as e:
    print(f"Error running app: {e}")
