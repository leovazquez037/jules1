from datetime import datetime
from typing import Any, Dict, List, Tuple

from loguru import logger

from influx_mcp.client import InfluxClient, influx_client
from influx_mcp.schemas import (BucketInfo, FieldInfo, LastPointResponse,
                                MeasurementInfo, QueryStats,
                                QueryTimeseriesResponse, TagInfo,
                                TimeseriesPoint)
from influx_mcp.utils import parse_time_range


def _parse_target(target: str) -> Tuple[str, str | None]:
    """Parses 'db/rp' or 'bucket' into parts."""
    if "/" in target:
        db, rp = target.split("/", 1)
        return db, rp
    return target, None

# --- Schema Queries ---

def list_buckets_or_dbs() -> List[BucketInfo]:
    """Uses the client to list buckets or databases."""
    return influx_client.list_buckets_or_dbs()

def list_measurements(target: str) -> List[MeasurementInfo]:
    """Lists all measurements in a given bucket or database."""
    bucket_or_db, _ = _parse_target(target)

    if influx_client.version == "2":
        query = f'''
        import "influxdata/influxdb/schema"
        schema.measurements(bucket: "{bucket_or_db}")
        '''
        tables = influx_client.query(query)
        return [MeasurementInfo(name=row.get_value()) for table in tables for row in table.records]
    else: # v1
        query = "SHOW MEASUREMENTS"
        results = influx_client.query(query, db=bucket_or_db)
        return [MeasurementInfo(name=item['name']) for item in results.get_points()]

def list_fields(target: str, measurement: str) -> List[FieldInfo]:
    """Lists all field keys for a given measurement."""
    bucket_or_db, _ = _parse_target(target)

    if influx_client.version == "2":
        query = f'''
        import "influxdata/influxdb/schema"
        schema.measurementFieldKeys(
            bucket: "{bucket_or_db}",
            measurement: "{measurement}",
            start: -365d
        )
        '''
        tables = influx_client.query(query)
        # Type information is not easily available in Flux schema queries
        return [FieldInfo(name=row.get_value()) for table in tables for row in table.records]
    else: # v1
        query = f'SHOW FIELD KEYS FROM "{measurement}"'
        results = influx_client.query(query, db=bucket_or_db)
        return [FieldInfo(name=item['fieldKey'], type=item.get('fieldType')) for item in results.get_points()]


def list_tags(target: str, measurement: str) -> List[TagInfo]:
    """Lists all tag keys and their values for a given measurement."""
    bucket_or_db, _ = _parse_target(target)

    if influx_client.version == "2":
        # This can be slow on large datasets. We limit to last 30 days.
        query = f'''
        import "influxdata/influxdb/schema"
        schema.measurementTagKeys(
            bucket: "{bucket_or_db}",
            measurement: "{measurement}",
            start: -30d
        )
        '''
        tables = influx_client.query(query)
        keys = [row.get_value() for table in tables for row in table.records]
        tags = []
        for key in keys:
            values_query = f'''
            import "influxdata/influxdb/schema"
            schema.measurementTagValues(
                bucket: "{bucket_or_db}",
                measurement: "{measurement}",
                tag: "{key}",
                start: -30d
            )
            '''
            values_tables = influx_client.query(values_query)
            values = [row.get_value() for table in values_tables for row in table.records]
            tags.append(TagInfo(key=key, values=values[:100])) # Limit values
        return tags
    else: # v1
        query = f'SHOW TAG KEYS FROM "{measurement}"'
        key_results = influx_client.query(query, db=bucket_or_db)
        keys = [item['tagKey'] for item in key_results.get_points()]
        tags = []
        for key in keys:
            # This can be slow, InfluxQL doesn't have a great way to limit this
            values_query = f'SHOW TAG VALUES FROM "{measurement}" WITH KEY = "{key}"'
            values_results = influx_client.query(values_query, db=bucket_or_db)
            values = [item['value'] for item in values_results.get_points()]
            tags.append(TagInfo(key=key, values=values[:100])) # Limit values
        return tags


# --- Data Queries ---

def query_timeseries_v2(
    bucket: str, measurement: str, field: str, start: datetime, stop: datetime,
    tags: Dict[str, str], aggregate: str | None, every: str | None, limit: int, fill: str
) -> QueryTimeseriesResponse:

    time_filter = f'start: {start.isoformat()}Z, stop: {stop.isoformat()}Z'
    tag_filters = " and ".join([f'r["{k}"] == "{v}"' for k, v in tags.items()]) if tags else ""

    flux_query = f'from(bucket: "{bucket}")\n'
    flux_query += f'  |> range({time_filter})\n'
    flux_query += f'  |> filter(fn: (r) => r["_measurement"] == "{measurement}")\n'
    flux_query += f'  |> filter(fn: (r) => r["_field"] == "{field}")\n'
    if tag_filters:
        flux_query += f'  |> filter(fn: (r) => {tag_filters})\n'

    if aggregate and every:
        fill_part = f'fill(usePrevious: {"true" if fill == "previous" else "false"})' if fill != "none" else ""
        if fill == "linear": # Linear is special
             fill_part = 'interpolate.linear(every: {every})'

        flux_query += f'  |> aggregateWindow(every: {every}, fn: {aggregate}, createEmpty: {"true" if fill != "none" else "false"})\n'
        if fill_part:
             flux_query += f'  |> {fill_part}\n'

    flux_query += f'  |> limit(n: {limit})\n'
    flux_query += f'  |> yield(name: "results")'

    logger.debug(f"Executing Flux query:\n{flux_query}")
    tables = influx_client.query(flux_query)

    series = [
        TimeseriesPoint(time_iso=rec.get_time().isoformat(), value=rec.get_value())
        for table in tables for rec in table.records
    ]

    stats = QueryStats(
        points_returned=len(series),
        start_effective_iso=start.isoformat(),
        stop_effective_iso=stop.isoformat(),
        aggregate_function=aggregate,
        downsample_interval=every,
    )
    return QueryTimeseriesResponse(series=series, stats=stats)


def query_timeseries_v1(
    db: str, rp: str | None, measurement: str, field: str, start: datetime, stop: datetime,
    tags: Dict[str, str], aggregate: str | None, every: str | None, limit: int, fill: str
) -> QueryTimeseriesResponse:

    time_filter = f"time >= '{start.isoformat()}Z' AND time <= '{stop.isoformat()}Z'"
    tag_filters = " AND ".join([f'"{k}" = \'{v}\'' for k, v in tags.items()]) if tags else ""

    target_measurement = f'"{db}"."{rp}"."{measurement}"' if rp else f'"{db}".."{measurement}"'

    if aggregate and every:
        select_clause = f'{aggregate}("{field}")'
        group_by_clause = f'GROUP BY time({every})'
        fill_clause = f'fill({fill})' if fill != "linear" else "" # InfluxQL doesn't support linear
    else:
        select_clause = f'"{field}"'
        group_by_clause = ""
        fill_clause = ""

    q = f'SELECT {select_clause} FROM {target_measurement} WHERE {time_filter}'
    if tag_filters:
        q += f' AND {tag_filters}'
    if group_by_clause:
        q += f' {group_by_clause}'
    if fill_clause:
        q += f' {fill_clause}'

    q += f' LIMIT {limit}'

    logger.debug(f"Executing InfluxQL query: {q}")
    results = influx_client.query(q, db=db)
    points = list(results.get_points())

    series = [
        TimeseriesPoint(time_iso=p['time'], value=p.get(aggregate, p.get(field)))
        for p in points
    ]

    stats = QueryStats(
        points_returned=len(series),
        start_effective_iso=start.isoformat(),
        stop_effective_iso=stop.isoformat(),
        aggregate_function=aggregate,
        downsample_interval=every,
    )
    return QueryTimeseriesResponse(series=series, stats=stats)


def get_timeseries_data(**kwargs) -> QueryTimeseriesResponse:
    """Facade function to route to the correct query implementation."""
    start_dt, stop_dt = parse_time_range(kwargs['start'], kwargs.get('stop'))

    target = kwargs['target']
    bucket_or_db, rp = _parse_target(target)

    # Prepare common args
    query_args = {
        "measurement": kwargs['measurement'],
        "field": kwargs['field'],
        "start": start_dt,
        "stop": stop_dt,
        "tags": kwargs.get('tags'),
        "aggregate": kwargs.get('aggregate'),
        "every": kwargs.get('every'),
        "limit": kwargs.get('limit', 1000),
        "fill": kwargs.get('fill', 'none'),
    }

    if influx_client.version == "2":
        return query_timeseries_v2(bucket=bucket_or_db, **query_args)
    else:
        return query_timeseries_v1(db=bucket_or_db, rp=rp, **query_args)

def get_last_point(**kwargs) -> LastPointResponse:
    """Gets the very last point for a series."""
    # This is a simplified version of query_timeseries
    target = kwargs['target']
    bucket_or_db, rp = _parse_target(target)
    measurement = kwargs['measurement']
    field = kwargs.get('field')
    tags = kwargs.get('tags')

    if influx_client.version == "2":
        tag_filters = " and ".join([f'r["{k}"] == "{v}"' for k, v in tags.items()]) if tags else ""
        field_filter = f'|> filter(fn: (r) => r["_field"] == "{field}")' if field else ""
        q = f'''
        from(bucket: "{bucket_or_db}")
          |> range(start: -365d) // Look back up to a year
          |> filter(fn: (r) => r["_measurement"] == "{measurement}")
          {field_filter}
          {f'|> filter(fn: (r) => {tag_filters})' if tag_filters else ''}
          |> last()
        '''
        tables = influx_client.query(q)
        if not tables or not tables[0].records:
            raise ValueError("No data found for the specified criteria.")

        # With multiple fields, last() can return multiple tables. We just grab the first.
        record = tables[0].records[0]
        return LastPointResponse(
            time_iso=record.get_time().isoformat(),
            value=record.get_value(),
            field=record.get_field(),
            tags={k: v for k, v in record.values.items() if not k.startswith("_") and k != "result" and k != "table"}
        )
    else: # v1
        tag_filters = " AND ".join([f'"{k}" = \'{v}\'' for k, v in tags.items()]) if tags else ""
        select_clause = f'"{field}"' if field else "*"
        q = f'SELECT {select_clause} FROM "{measurement}"'
        if tag_filters:
            q += f' WHERE {tag_filters}'
        q += ' ORDER BY time DESC LIMIT 1'

        results = influx_client.query(q, db=bucket_or_db)
        point = next(results.get_points(), None)
        if not point:
            raise ValueError("No data found for the specified criteria.")

        # Find the first key that is not 'time'
        value_field = field or next((k for k in point.keys() if k != 'time'), None)

        return LastPointResponse(
            time_iso=point['time'],
            value=point[value_field],
            field=value_field,
            tags={k: v for k, v in point.items() if k != 'time' and k != value_field}
        )
