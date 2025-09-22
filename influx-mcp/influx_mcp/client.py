import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Protocol

import requests
from influxdb import InfluxDBClient
from influxdb_client import InfluxDBClient as InfluxDBClientV2
from influxdb_client.client.write_api import SYNCHRONOUS
from loguru import logger

from influx_mcp.config import Settings, settings
from influx_mcp.schemas import BucketInfo


class InfluxClient(Protocol):
    """
    A protocol defining the interface for a unified InfluxDB client.
    This allows the application to interact with v1 and v2 in the same way.
    """
    version: str

    def ping(self) -> bool:
        """Check if the InfluxDB instance is reachable."""
        ...

    def list_buckets_or_dbs(self) -> List[BucketInfo]:
        """List all available buckets (v2) or databases (v1)."""
        ...

    def query(self, query_string: str, **kwargs) -> Any:
        """Execute a read-only query."""
        ...

    def write(self, *args, **kwargs) -> bool:
        """Write data points."""
        ...

    def close(self) -> None:
        """Close the client connection."""
        ...


class InfluxDBV1ClientImpl(InfluxClient):
    """InfluxDB v1.x client implementation."""
    version = "1"

    def __init__(self, settings: Settings):
        logger.info("Initializing InfluxDB v1 client...")
        self.client = InfluxDBClient(
            host=settings.influx_url.split(":")[0].replace("http://", ""),
            port=int(settings.influx_url.split(":")[2]) if len(settings.influx_url.split(":")) > 2 else 8086,
            username=settings.influx_username,
            password=settings.get_influx_password(),
            timeout=settings.influx_request_timeout_sec,
        )

    def ping(self) -> bool:
        try:
            self.client.ping()
            logger.info("Successfully pinged InfluxDB v1.")
            return True
        except Exception as e:
            logger.warning(f"Failed to ping InfluxDB v1: {e}")
            return False

    def list_buckets_or_dbs(self) -> List[BucketInfo]:
        dbs = self.client.get_list_database()
        results = []
        for db in dbs:
            rps = self.client.get_list_retention_policies(database=db['name'])
            if rps:
                for rp in rps:
                    results.append(BucketInfo(name=f"{db['name']}/{rp['name']}", type="db", retention_policy=f"{rp['duration']}/{rp['replicaN']}"))
            else:
                 results.append(BucketInfo(name=db['name'], type="db"))
        return results

    def query(self, query_string: str, **kwargs) -> Any:
        db = kwargs.get("db")
        return self.client.query(query_string, database=db)

    def write(self, *args, **kwargs) -> bool:
        return self.client.write_points(*args, **kwargs)

    def close(self) -> None:
        self.client.close()


class InfluxDBV2ClientImpl(InfluxClient):
    """InfluxDB v2.x client implementation."""
    version = "2"

    def __init__(self, settings: Settings):
        logger.info("Initializing InfluxDB v2 client...")
        self.client = InfluxDBClientV2(
            url=settings.influx_url,
            token=settings.get_influx_token(),
            org=settings.influx_org,
            timeout=settings.influx_request_timeout_sec * 1000, # ms
        )
        self.query_api = self.client.query_api()
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def ping(self) -> bool:
        try:
            ready = self.client.ready()
            if ready.status == "ready":
                logger.info(f"Successfully pinged InfluxDB v2. Version: {ready.version}")
                return True
            return False
        except Exception as e:
            logger.warning(f"Failed to ping InfluxDB v2: {e}")
            return False

    def list_buckets_or_dbs(self) -> List[BucketInfo]:
        buckets_api = self.client.buckets_api()
        buckets = buckets_api.find_buckets().buckets
        return [BucketInfo(name=b.name, type="bucket") for b in buckets]

    def query(self, query_string: str, **kwargs) -> Any:
        return self.query_api.query(query_string, org=settings.influx_org)

    def write(self, *args, **kwargs) -> bool:
        self.write_api.write(*args, **kwargs)
        return True

    def close(self) -> None:
        self.client.close()


def get_influx_client(settings: Settings) -> InfluxClient:
    """
    Factory function to get the appropriate InfluxDB client based on settings
    and auto-detection.
    """
    version = settings.influx_version
    logger.info(f"Attempting to connect to InfluxDB. Configured version: '{version}'")

    if version == "2":
        return InfluxDBV2ClientImpl(settings)
    if version == "1":
        return InfluxDBV1ClientImpl(settings)

    # Auto-detection logic
    if version == "auto":
        logger.info("Auto-detecting InfluxDB version...")
        # Try v2 first
        try:
            v2_ready_url = f"{settings.influx_url}/api/v2/ready"
            response = requests.get(v2_ready_url, timeout=5)
            if response.status_code == 200:
                logger.info("Detected InfluxDB v2 via /api/v2/ready endpoint.")
                return InfluxDBV2ClientImpl(settings)
        except requests.RequestException:
            logger.warning("Could not connect to v2 /api/v2/ready endpoint. Trying v1.")

        # Try v1 next
        try:
            v1_ping_url = f"{settings.influx_url}/ping"
            response = requests.get(v1_ping_url, timeout=5)
            # v1 ping returns 204 No Content on success
            if response.status_code == 204:
                logger.info("Detected InfluxDB v1 via /ping endpoint.")
                return InfluxDBV1ClientImpl(settings)
        except requests.RequestException:
             logger.warning("Could not connect to v1 /ping endpoint.")

    raise ConnectionError("Could not auto-detect InfluxDB version. Please specify INFLUX_VERSION=1 or INFLUX_VERSION=2.")

# Global client instance
# This can be imported by other modules
try:
    influx_client = get_influx_client(settings)
    logger.info(f"Successfully created InfluxDB client for version {influx_client.version}")
except (ConnectionError, Exception) as e:
    logger.error(f"Failed to initialize InfluxDB client: {e}")
    # To allow app to start and return errors, we can use a dummy client
    class DummyClient(InfluxClient):
        version = "none"
        def __getattr__(self, name):
            def method(*args, **kwargs):
                raise ConnectionError(f"InfluxDB client not initialized: {e}")
            return method
    influx_client = DummyClient()
