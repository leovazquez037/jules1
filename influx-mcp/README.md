# InfluxDB MCP Server (`influx-mcp`)

## Overview

**influx-mcp** is a Python-based server that implements the Model Context Protocol (MCP). It acts as a bridge between a Large Language Model (LLM) and an InfluxDB time-series database, exposing a set of powerful, IoT-focused tools for data exploration and querying.

The server is designed to be:
- **Versatile:** It can connect to both InfluxDB v2 (using Flux) and InfluxDB v1.x (using InfluxQL).
- **Intelligent:** It can auto-detect the database version or be forced to use a specific one.
- **Secure:** It handles sensitive tokens and passwords securely, preventing them from being logged.
- **Easy to Use:** It's installable as a Python package and configurable via environment variables.

## Features

- **Dual Version Support:** Connects to InfluxDB v1 and v2.
- **Schema Exploration:** Tools to list buckets/databases, measurements, fields, and tags.
- **Powerful Querying:** A flexible `query_timeseries` tool with support for time ranges, filters, aggregation, and downsampling.
- **Convenience Tools:** Shortcuts like `last_point` and `window_stats` for common IoT use cases.
- **MCP Resources:** Addressable time-series queries via `influxdb://` URIs.
- **Safe by Default:** Imposes a hard limit on query results to prevent overloading the client.

## Installation and Setup

### Prerequisites
- Python 3.11+
- An accessible InfluxDB v1 or v2 instance.

### Installation
1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd influx-mcp
   ```

2. **Install dependencies:**
   It's recommended to use a virtual environment.
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -e .
   ```
   The `-e` flag installs the package in "editable" mode.

### Configuration
The server is configured entirely through environment variables.

1. **Create a `.env` file:**
   Copy the example file to create your own local configuration.
   ```bash
   cp .env.example .env
   ```

2. **Edit `.env`:**
   Open the `.env` file and fill in the details for your InfluxDB instance.

   **Example for InfluxDB v2 (Cloud):**
   ```env
   INFLUX_VERSION=2
   INFLUX_URL=https://us-west-2-1.aws.cloud2.influxdata.com
   INFLUX_ORG=my-iot-org
   INFLUX_TOKEN=my-super-secret-auth-token
   INFLUX_DEFAULT_BUCKET=iot-metrics
   ```

   **Example for InfluxDB v1 (local):**
   ```env
   INFLUX_VERSION=1
   INFLUX_URL=http://localhost:8086
   INFLUX_USERNAME=admin
   INFLUX_PASSWORD=admin-password
   INFLUX_DEFAULT_DB=telegraf
   ```

## Usage

### Running the Server
Once configured, you can run the server using:
```bash
python -m influx_mcp.server
```
The server will start and be ready to accept MCP connections.

### Dry Run
To validate your connection settings without starting the full server, use the `--dry-run` flag:
```bash
python -m influx_mcp.server --dry-run
```
This will attempt to ping the database, report the detected version, and list the first few buckets/databases it finds.

## Available MCP Tools

Here are some examples of how to use the provided tools.

### `list_buckets_or_dbs()`
Lists all available data containers.
- **Example Call:** `list_buckets_or_dbs()`
- **Example Output:**
  ```json
  {
    "results": [
      {"name": "iot-devices", "type": "bucket", "retention_policy": null},
      {"name": "system-metrics", "type": "bucket", "retention_policy": null}
    ]
  }
  ```

### `list_measurements(target: str)`
Lists measurements within a bucket or database.
- **Example Call:** `list_measurements(target="iot-devices")`
- **Example Output:**
  ```json
  {"measurements": [{"name": "device_status"}, {"name": "environment_reading"}]}
  ```

### `last_point(...)`
Gets the most recent data point for a series.
- **Example Call:** `last_point(target="iot-devices", measurement="device_status", field="battery", tags={"device_id": "abc-123"})`
- **Example Output:**
  ```json
  {
    "time_iso": "2023-10-27T10:00:00Z",
    "value": 95.5,
    "field": "battery",
    "tags": {"device_id": "abc-123", "location": "warehouse-a"}
  }
  ```

### `query_timeseries(...)`
Performs a detailed query for time-series data.
- **Example Call (24h of aggregated temperature data):**
  ```python
  query_timeseries(
      target="iot-devices",
      measurement="environment_reading",
      field="temperature",
      start="-24h",
      every="10m",
      aggregate="mean",
      tags={"site": "planta1"}
  )
  ```
- **Example Output:**
  ```json
  {
    "series": [
      {"time_iso": "2023-10-27T09:50:00Z", "value": 22.4},
      {"time_iso": "2023-10-27T10:00:00Z", "value": 22.5}
    ],
    "stats": {
      "points_returned": 144,
      "start_effective_iso": "2023-10-26T10:00:00Z",
      "stop_effective_iso": "2023-10-27T10:00:00Z",
      "aggregate_function": "mean",
      "downsample_interval": "10m"
    }
  }
  ```

## MCP Resource: `influxdb://`

You can also query data directly using a resource URI.

**Format:**
`influxdb://<target>/<measurement>?field=<field>&start=<time>&stop=<time>&every=<interval>&aggregate=<func>&tag.key=value`

**Example:**
Get the max RSSI per hour for device `xyz-789` over the last 3 days.
```
influxdb://iot-devices/device_status?field=rssi&start=-3d&every=1h&aggregate=max&tag.device_id=xyz-789
```

Reading this resource via MCP will execute the corresponding query and return a formatted summary along with the full JSON result.
