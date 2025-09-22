import pytest

from influx_mcp import server
from influx_mcp.client import InfluxClient
from influx_mcp.schemas import QueryTimeseriesRequest


@pytest.fixture
def mock_influx_client(mocker):
    """Fixture to create a mock InfluxClient."""
    mock_client = mocker.MagicMock(spec=InfluxClient)
    mock_client.version = "2" # Default to v2 for tests
    return mock_client


@pytest.fixture(autouse=True)
def patch_influx_client(monkeypatch, mock_influx_client):
    """Auto-used fixture to replace the global influx_client with a mock."""
    monkeypatch.setattr(server.queries, "influx_client", mock_influx_client)
    monkeypatch.setattr(server, "influx_client", mock_influx_client)
    return mock_influx_client


def test_list_buckets_smoke(mock_influx_client):
    """Smoke test for the list_buckets_or_dbs tool."""
    # Arrange
    mock_influx_client.list_buckets_or_dbs.return_value = [
        server.queries.BucketInfo(name="test-bucket", type="bucket")
    ]

    # Act
    response = server.list_buckets_or_dbs()

    # Assert
    mock_influx_client.list_buckets_or_dbs.assert_called_once()
    assert len(response.results) == 1
    assert response.results[0].name == "test-bucket"


def test_query_timeseries_smoke(mock_influx_client):
    """Smoke test for the query_timeseries tool."""
    # Arrange
    request_model = QueryTimeseriesRequest(
        target="iot-bucket",
        measurement="temp",
        field="value",
        start="-1h",
        tags={"device": "abc"},
        aggregate="mean",
        every="5m"
    )

    # Mock the return value of the underlying query function
    # The tool calls queries.get_timeseries_data, so we mock that
    # For this smoke test, we don't need to mock the client's 'query' method itself,
    # just the function in 'queries.py' that the tool calls.

    # A better approach is to mock the client call inside the query function
    mock_influx_client.query.return_value = [] # Return empty result for simplicity

    # Act
    response = server.query_timeseries(request_model)

    # Assert
    # We expect the tool to call the query function, which in turn calls the client
    mock_influx_client.query.assert_called_once()

    # Check if the generated query string contains expected parts
    called_query_arg = mock_influx_client.query.call_args[0][0]
    assert 'from(bucket: "iot-bucket")' in called_query_arg
    assert 'r["_measurement"] == "temp"' in called_query_arg
    assert 'r["device"] == "abc"' in called_query_arg
    assert 'aggregateWindow(every: 5m, fn: mean' in called_query_arg

    assert response is not None
    assert response.stats.points_returned == 0


def test_last_point_v1_smoke(mock_influx_client):
    """Smoke test for last_point, simulating a v1 client."""
    # Arrange
    mock_influx_client.version = "1"

    # Mock the response from the InfluxDB v1 client library
    from influxdb.resultset import ResultSet
    mock_result_set = ResultSet({
        'series': [{
            'name': 'device_status',
            'columns': ['time', 'battery', 'device_id'],
            'values': [['2023-01-01T12:00:00Z', 99, 'abc-123']]
        }]
    })
    mock_influx_client.query.return_value = mock_result_set

    request = server.LastPointRequest(
        target="iot-db",
        measurement="device_status",
        field="battery",
        tags={"device_id": "abc-123"}
    )

    # Act
    response = server.last_point(request)

    # Assert
    mock_influx_client.query.assert_called_once()
    called_query_arg = mock_influx_client.query.call_args[0][0]
    assert 'SELECT "battery" FROM "device_status"' in called_query_arg
    assert '"device_id" = \'abc-123\'' in called_query_arg
    assert 'ORDER BY time DESC LIMIT 1' in called_query_arg

    assert response.value == 99
    assert response.field == "battery"
