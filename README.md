## Getting Started

This guide will help you set up the project on your Windows machine, including instructions for installing Apache Kafka and configuring the Python environment.

### Prerequisites

- **Kafka 3.8.1**: Download the latest version [here](https://kafka.apache.org/downloads).
- **Spark 3.5.3**: Download the latest version [here](https://spark.apache.org/downloads.html). 
- **Python 3.x**: Ensure you have Python installed, along with `pip` for managing packages.
## Installation

### 1. Install Kafka

1. Download Kafka from the [official website](https://kafka.apache.org/downloads).
2. Follow the [Kafka Quickstart guide](https://kafka.apache.org/quickstart) to set up and start Zookeeper and Kafka servers.
   
### 2. Install Spark

1. Download Spark from the [official website](https://spark.apache.org/downloads.html).
2. Follow installation guide, following this was a easy solution [youtube video](https://www.youtube.com/watch?v=OmcSTQVkrvo).

### 3. Start Zookeeper and Kafka Servers

> **Note**: These commands are for Windows. Open a command prompt and navigate to the Kafka installation directory.

#### Start Zookeeper Server

Run the following command to start the Zookeeper server with the provided configuration file:

```bash
.\bin\windows\zookeeper-server-start.bat config\zookeeper.properties
```
#### Start Kafka Server

Run this command to start the Kafka server with the specified configuration:

```bash
.\bin\windows\kafka-server-start.bat config\kafka.properties
```
#### 4. Set Up the Python Environment

Install the required Python libraries using pip. In your command prompt or terminal, run:
```bash
pip install dash kafka-python pandas plotly
```
These libraries are essential for building the dashboard and working with Kafka.

Usage
To run the project, make sure both Zookeeper and Kafka servers are active, and that the Python environment is set up. Follow the documentation in the code files for additional details on running specific components of the project.

Run producer and consumer python files
```bash
py .\consumer.py
```
```bash
py .\producer.py
```

Additional Resources

[Kafka Documentation](https://kafka.apache.org/documentation/).

[Dash & Plotly Documentation](https://dash.plotly.com/).
