import os
import sys
from importlib import reload

import pytest
from pydantic import ValidationError

from influx_mcp import config


def test_settings_load_from_env(monkeypatch):
    """Tests that settings are correctly loaded from environment variables."""
    monkeypatch.setenv("INFLUX_URL", "http://test.com")
    monkeypatch.setenv("INFLUX_TOKEN", "test-token")
    monkeypatch.setenv("INFLUX_ORG", "test-org")
    monkeypatch.setenv("MCP_LOG_LEVEL", "DEBUG")

    # We need to reload the config module to make it use the new env vars
    reload(config)

    settings = config.settings
    assert settings.influx_url == "http://test.com"
    assert settings.get_influx_token() == "test-token"
    assert settings.influx_org == "test-org"
    assert settings.mcp_log_level == "DEBUG"


def test_settings_missing_required_raises_error(monkeypatch):
    """Tests that a missing required variable (INFLUX_URL) causes a validation error."""
    # Ensure INFLUX_URL is not set
    monkeypatch.delenv("INFLUX_URL", raising=False)

    # Reloading the module should now raise an error and exit
    # We catch the SystemExit that our code triggers on validation failure.
    with pytest.raises(SystemExit):
        reload(config)


def test_settings_repr_masks_secrets(monkeypatch):
    """Tests that the __repr__ of the Settings object masks sensitive data."""
    monkeypatch.setenv("INFLUX_URL", "http://test.com")
    monkeypatch.setenv("INFLUX_TOKEN", "a-very-secret-token")
    monkeypatch.setenv("INFLUX_PASSWORD", "a-very-secret-password")

    reload(config)
    settings_repr = repr(config.settings)

    assert "a-very-secret-token" not in settings_repr
    assert "a-very-secret-password" not in settings_repr
    assert "influx_token': '***'" in settings_repr
    assert "influx_password': '***'" in settings_repr
    assert "http://test.com" in settings_repr
