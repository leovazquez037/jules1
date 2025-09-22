import re
from datetime import datetime, timedelta, timezone

from dateutil.parser import parse as parse_iso
from pydantic import SecretStr


def parse_time_range(start: str, stop: str | None = "now") -> tuple[datetime, datetime]:
    """
    Parses start and stop time strings, handling ISO 8601 and relative formats.

    Relative formats:
    - 'now'
    - '-<number><unit>', e.g., '-15m', '-24h', '-7d'

    Returns a tuple of two timezone-aware datetime objects (start, stop) in UTC.
    """
    stop_dt = _parse_time_string(stop if stop else "now")
    start_dt = _parse_time_string(start, relative_to=stop_dt)

    return start_dt, stop_dt


def _parse_time_string(time_str: str, relative_to: datetime | None = None) -> datetime:
    """Helper to parse a single time string."""
    now = datetime.now(timezone.utc)

    if time_str.lower() in ("now", "now()"):
        return now

    # Check for relative time format, e.g., "-7d"
    relative_match = re.match(r"^-(\d+)([mhd])$", time_str)
    if relative_match:
        value = int(relative_match.group(1))
        unit = relative_match.group(2)

        delta = timedelta()
        if unit == 'm':
            delta = timedelta(minutes=value)
        elif unit == 'h':
            delta = timedelta(hours=value)
        elif unit == 'd':
            delta = timedelta(days=value)

        base_time = relative_to if relative_to else now
        return base_time - delta

    # Assume ISO 8601 format
    try:
        dt = parse_iso(time_str)
        # If no timezone info, assume UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError as e:
        raise ValueError(f"Invalid time format '{time_str}'. Must be ISO 8601 or relative (e.g., '-7d').") from e


def mask_sensitive_data(data: dict) -> dict:
    """
    Recursively traverses a dictionary and masks values of fields
    that are SecretStr instances or have names suggesting sensitivity.
    """
    sensitive_keys = {"token", "password"}
    masked_data = {}
    for key, value in data.items():
        if isinstance(value, SecretStr) or key in sensitive_keys:
            masked_data[key] = "***"
        elif isinstance(value, dict):
            masked_data[key] = mask_sensitive_data(value)
        else:
            masked_data[key] = value
    return masked_data
