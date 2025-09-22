from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


# --- Generic Models ---

class InfluxTarget(BaseModel):
    """
    Specifies the target for a query, which can be a bucket (v2) or a database (v1).
    An optional retention policy can be specified for v1.
    Example: "my-bucket" or "my-db/my-rp"
    """
    target: str = Field(
        ...,
        description="Target bucket (v2) or database (v1), e.g., 'iot-data' or 'telegraf/autogen'.",
        examples=["iot-data", "telegraf/autogen"],
    )

# --- Tool: list_buckets_or_dbs ---

class BucketInfo(BaseModel):
    name: str = Field(..., description="Name of the bucket or database.")
    type: Literal["bucket", "db"] = Field(..., description="Type of the container.")
    retention_policy: Optional[str] = Field(None, description="Retention policy (for InfluxDB v1).")

class ListBucketsResponse(BaseModel):
    results: List[BucketInfo]

# --- Tool: list_measurements ---

class MeasurementInfo(BaseModel):
    name: str = Field(..., description="Name of the measurement.")

class ListMeasurementsResponse(BaseModel):
    measurements: List[MeasurementInfo]

# --- Tool: list_fields ---

class FieldInfo(BaseModel):
    name: str = Field(..., description="Name of the field key.")
    type: Optional[str] = Field(None, description="Data type of the field, if available.")

class ListFieldsResponse(BaseModel):
    fields: List[FieldInfo]

# --- Tool: list_tags ---

class TagInfo(BaseModel):
    key: str = Field(..., description="Name of the tag key.")
    values: List[str] = Field(..., description="List of observed values for this tag key.")

class ListTagsResponse(BaseModel):
    tags: List[TagInfo]

# --- Tool: last_point ---

class LastPointRequest(InfluxTarget):
    measurement: str = Field(..., description="The measurement to query.")
    field: Optional[str] = Field(None, description="Optional: specific field to retrieve. If None, returns the most recent field.")
    tags: Optional[Dict[str, str]] = Field(None, description="Optional: key-value pairs to filter by tags.")

class LastPointResponse(BaseModel):
    time_iso: str = Field(..., description="The ISO 8601 timestamp of the last point (UTC).")
    value: Any = Field(..., description="The value of the last point.")
    field: str = Field(..., description="The field key of the last point.")
    tags: Dict[str, str] = Field(..., description="All tags associated with the last point.")

# --- Tool: query_timeseries ---

class QueryTimeseriesRequest(InfluxTarget):
    measurement: str = Field(..., description="The measurement to query.")
    field: str = Field(..., description="The field to retrieve values from.")
    start: str = Field(..., description="Start of the time range (ISO 8601 or relative, e.g., '-24h').")
    stop: Optional[str] = Field("now()", description="End of the time range (ISO 8601 or relative, e.g., '-1h'). Defaults to now.")
    tags: Optional[Dict[str, str]] = Field(None, description="Key-value pairs to filter by tags.")
    aggregate: Optional[Literal[
        "mean", "max", "min", "sum", "count", "median", "spread", "last", "first"
    ]] = Field(None, description="Aggregation function to apply.")
    every: Optional[str] = Field(None, description="Downsampling interval (e.g., '5m', '1h'). Requires an aggregate function.")
    limit: int = Field(1000, gt=0, le=50000, description="Maximum number of data points to return.")
    fill: Optional[Literal["none", "previous", "linear"]] = Field("none", description="How to fill null values after aggregation.")


class TimeseriesPoint(BaseModel):
    time_iso: str
    value: Any

class QueryStats(BaseModel):
    points_returned: int
    start_effective_iso: str
    stop_effective_iso: str
    aggregate_function: Optional[str] = None
    downsample_interval: Optional[str] = None

class QueryTimeseriesResponse(BaseModel):
    series: List[TimeseriesPoint]
    stats: QueryStats

# --- Tool: window_stats ---

class WindowStatsRequest(InfluxTarget):
    measurement: str = Field(..., description="The measurement to query.")
    field: str = Field(..., description="The field to calculate statistics on.")
    window: str = Field(..., description="The time window for the stats (e.g., '-24h', '-7d').")
    tags: Optional[Dict[str, str]] = Field(None, description="Key-value pairs to filter by tags.")

class WindowStatsResponse(BaseModel):
    mean: Optional[float] = None
    min: Optional[Any] = None
    max: Optional[Any] = None
    last: Optional[Any] = None
    count: int
    start_iso: str
    stop_iso: str

# --- Tool: write_point (Optional) ---

class WritePointRequest(InfluxTarget):
    measurement: str
    fields: Dict[str, Any]
    tags: Optional[Dict[str, str]] = None
    time_iso: Optional[str] = None

class WritePointResponse(BaseModel):
    ok: bool
    written: int

# --- Error Model ---

class ErrorResponse(BaseModel):
    error: str
    details: Optional[str] = None
