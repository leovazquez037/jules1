from datetime import datetime, timedelta, timezone

import pytest

from influx_mcp.utils import parse_time_range


def test_parse_time_range_relative():
    """Tests parsing of relative time ranges like '-7d'."""
    now = datetime.now(timezone.utc)
    start, stop = parse_time_range("-7d", "now")

    assert stop.year == now.year
    assert stop.month == now.month
    assert stop.day == now.day

    expected_start = now - timedelta(days=7)
    assert (start - expected_start).total_seconds() < 1 # Check they are very close


def test_parse_time_range_iso_8601():
    """Tests parsing of absolute ISO 8601 time ranges."""
    start_iso = "2023-01-01T00:00:00Z"
    stop_iso = "2023-01-02T00:00:00Z"

    start, stop = parse_time_range(start_iso, stop_iso)

    assert start == datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert stop == datetime(2023, 1, 2, 0, 0, 0, tzinfo=timezone.utc)


def test_parse_time_range_mixed():
    """Tests parsing a mix of relative and absolute times."""
    start_iso = "2023-01-01T00:00:00Z"
    start, stop = parse_time_range(start_iso, "now")

    assert start == datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert (stop - datetime.now(timezone.utc)).total_seconds() < 1


def test_parse_time_range_default_stop():
    """Tests that 'stop' defaults to 'now' if not provided."""
    start, stop = parse_time_range("-1h")

    now = datetime.now(timezone.utc)
    expected_start = now - timedelta(hours=1)

    assert (stop - now).total_seconds() < 1
    assert (start - expected_start).total_seconds() < 1


def test_invalid_time_format_raises_error():
    """Tests that an invalid time string raises a ValueError."""
    with pytest.raises(ValueError, match="Invalid time format"):
        parse_time_range("yesterday", "today")

    with pytest.raises(ValueError, match="Invalid time format"):
        parse_time_range("-5y") # 'y' is not a supported unit in our simple parser
