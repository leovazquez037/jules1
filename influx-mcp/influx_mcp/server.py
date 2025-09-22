import argparse
import json
from typing import Optional
from urllib.parse import parse_qs, urlparse

from loguru import logger
from mcp.server.fastmcp.server import FastMCP, Context
from mcp.server.fastmcp.exceptions import ToolError, ResourceError
from mcp.server.fastmcp.resources import FunctionResource

from influx_mcp import queries
from influx_mcp.client import InfluxClient, influx_client, settings
from influx_mcp.config import setup_logging
from influx_mcp.schemas import (ErrorResponse, LastPointRequest,
                               ListBucketsResponse, ListFieldsResponse,
                               ListMeasurementsResponse, ListTagsResponse,
                               QueryTimeseriesRequest, QueryTimeseriesResponse,
                               WindowStatsRequest, WindowStatsResponse,
                               WritePointRequest, WritePointResponse)

# --- MCP Server Setup ---
server = FastMCP(
    name="influx-mcp",
    instructions="MCP server to query InfluxDB (v1/v2) for time-series data.",
)


# --- Exception Handling ---
def handle_query_error(func):
    """Decorator to catch common exceptions and return a standard error."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ConnectionError as e:
            logger.error(f"Connection error in tool '{func.__name__}': {e}")
            raise ToolError(f"Connection to InfluxDB failed: {e}")
        except ValueError as e:
            logger.warning(f"Value error in tool '{func.__name__}': {e}")
            raise ToolError(f"Invalid parameters or data: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error in tool '{func.__name__}': {e}")
            raise ToolError(f"An unexpected internal error occurred: {e}")
    return wrapper

# --- MCP Tools ---

@server.tool()
@handle_query_error
def list_buckets_or_dbs() -> ListBucketsResponse:
    """
    Lists all available buckets (for InfluxDB v2) or databases (for InfluxDB v1).
    For v1, it may also show retention policies.
    """
    results = queries.list_buckets_or_dbs()
    return ListBucketsResponse(results=results)

@server.tool()
@handle_query_error
def list_measurements(target: str) -> ListMeasurementsResponse:
    """Lists all measurements within a specific bucket or database."""
    results = queries.list_measurements(target=target)
    return ListMeasurementsResponse(measurements=results)

@server.tool()
@handle_query_error
def list_fields(target: str, measurement: str) -> ListFieldsResponse:
    """Lists all field keys for a given measurement."""
    results = queries.list_fields(target=target, measurement=measurement)
    return ListFieldsResponse(fields=results)

@server.tool()
@handle_query_error
def list_tags(target: str, measurement: str) -> ListTagsResponse:
    """Lists all tag keys and a sample of their values for a given measurement."""
    results = queries.list_tags(target=target, measurement=measurement)
    return ListTagsResponse(tags=results)

@server.tool()
@handle_query_error
def last_point(request: LastPointRequest) -> queries.LastPointResponse:
    """Retrieves the most recent data point for a specific time series."""
    return queries.get_last_point(**request.model_dump())

@server.tool()
@handle_query_error
def query_timeseries(request: QueryTimeseriesRequest) -> QueryTimeseriesResponse:
    """
    Queries time-series data with filters, aggregation, and downsampling.
    'start' and 'stop' can be ISO 8601 or relative (e.g., '-24h').
    """
    return queries.get_timeseries_data(**request.model_dump())

@server.tool()
@handle_query_error
def window_stats(request: WindowStatsRequest) -> WindowStatsResponse:
    """
    Calculates aggregate statistics (mean, min, max, etc.) over a specified time window.
    """
    # This is a shortcut for a query_timeseries call
    start_dt, stop_dt = queries.parse_time_range(request.window, "now")

    # We need to run multiple aggregations.
    # For simplicity, we'll do them as separate queries, but a single, more complex query is possible.

    base_params = request.model_dump()
    base_params["start"] = start_dt.isoformat()
    base_params["stop"] = stop_dt.isoformat()
    del base_params["window"]

    def get_agg(func):
        try:
            res = queries.get_timeseries_data(**base_params, aggregate=func, every=request.window.replace('-', ''))
            return res.series[0].value if res.series else None
        except (ValueError, IndexError):
            return None

    count_res = queries.get_timeseries_data(**base_params, aggregate='count', every=request.window.replace('-', ''))

    return WindowStatsResponse(
        mean=get_agg('mean'),
        min=get_agg('min'),
        max=get_agg('max'),
        last=get_agg('last'),
        count=count_res.series[0].value if count_res.series else 0,
        start_iso=start_dt.isoformat(),
        stop_iso=stop_dt.isoformat(),
    )

@server.tool()
@handle_query_error
def write_point(request: WritePointRequest) -> WritePointResponse:
    """
    Writes a single data point to a measurement. (Use with caution)
    """
    target = request.target
    bucket_or_db, rp = queries._parse_target(target)

    point = {
        "measurement": request.measurement,
        "tags": request.tags or {},
        "fields": request.fields,
        "time": request.time_iso
    }

    if influx_client.version == "2":
        success = influx_client.write(bucket=bucket_or_db, record=point)
    else: # v1
        success = influx_client.write([point], database=bucket_or_db, retention_policy=rp)

    return WritePointResponse(ok=bool(success), written=1 if success else 0)


# --- MCP Resource ---

@server.resource("influxdb://{target}/{measurement}")
def read_influxdb_resource(target: str, measurement: str, context: Context) -> str:
    """
    Reads a time-series from InfluxDB as a resource.
    Query parameters like 'field', 'start', 'stop', 'aggregate', 'every', 'limit',
    and tags (e.g., 'device_id=abc') must be provided in the URI.
    Example: influxdb://bucket/meas?field=temp&start=-1d&device_id=123
    """
    query_params = parse_qs(context.request_context.request.url.query)

    # Helper to get a single value from parsed query params
    def get_param(name, default=None):
        return query_params.get(name, [default])[0]

    field = get_param("field")
    if not field:
        raise ResourceError("Query parameter 'field' is required.")

    # Extract tags from any other query parameters
    reserved_params = {"field", "start", "stop", "aggregate", "every", "limit"}
    tags = {k: v[0] for k, v in query_params.items() if k not in reserved_params}

    try:
        request = QueryTimeseriesRequest(
            target=target,
            measurement=measurement,
            field=field,
            start=get_param("start", "-1h"),
            stop=get_param("stop", "now()"),
            aggregate=get_param("aggregate"),
            every=get_param("every"),
            limit=int(get_param("limit", 1000)),
            tags=tags if tags else None
        )
        response = query_timeseries(request)
    except Exception as e:
        raise ResourceError(f"Failed to execute resource query: {e}")


    # Format output
    uri = str(context.request_context.request.url)
    header = f"--- Query Results for {uri} ---\n"
    header += f"Status: {response.stats.points_returned} points returned\n"
    header += f"Time Range: {response.stats.start_effective_iso} to {response.stats.stop_effective_iso}\n"

    body = ""
    for i, point in enumerate(response.series):
        if i < 20: # Show first 20 points
            body += f"{point.time_iso}\t{point.value}\n"
    if len(response.series) > 20:
        body += f"... (truncated, {len(response.series) - 20} more points)\n"

    footer = f"\n--- Full JSON Response ---\n{response.model_dump_json(indent=2)}"

    return header + body + footer

# --- Main Execution ---

def main():
    parser = argparse.ArgumentParser(description="InfluxDB MCP Server")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate connection, print info, and exit without starting the server.",
    )
    args = parser.parse_args()

    setup_logging(settings.mcp_log_level)
    logger.info(f"Loaded settings: {settings!r}")

    if args.dry_run:
        logger.info("--- Performing dry run ---")
        try:
            logger.info(f"Attempting to ping InfluxDB (version: {influx_client.version})...")
            if influx_client.ping():
                logger.success("Ping successful!")
                caps = influx_client.list_buckets_or_dbs()
                logger.info(f"Found {len(caps)} buckets/databases:")
                for cap in caps[:5]:
                    logger.info(f"  - {cap.name} (type: {cap.type})")
                if len(caps) > 5:
                    logger.info("  ...")
            else:
                logger.error("Ping failed. Check connection details.")
        except Exception as e:
            logger.error(f"Dry run failed: {e}")
        return

    logger.info("Starting MCP server...")
    server.run(transport="sse")

if __name__ == "__main__":
    main()
