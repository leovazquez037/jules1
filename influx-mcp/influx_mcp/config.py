import logging
import sys
from typing import Literal, Optional

from pydantic import Field, SecretStr, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from influx_mcp.utils import mask_sensitive_data


# --- Pydantic Settings Model ---

class Settings(BaseSettings):
    """
    MCP InfluxDB Server configuration loaded from environment variables.
    """
    # MCP Settings
    mcp_log_level: str = Field("INFO", alias="MCP_LOG_LEVEL")

    # InfluxDB General Settings
    influx_version: Literal["auto", "1", "2"] = Field("auto", alias="INFLUX_VERSION")
    influx_url: str = Field(..., alias="INFLUX_URL") # must be provided
    influx_request_timeout_sec: int = Field(30, alias="INFLUX_REQUEST_TIMEOUT_SEC")

    # InfluxDB v2 Specific Settings
    influx_org: Optional[str] = Field(None, alias="INFLUX_ORG")
    influx_token: Optional[SecretStr] = Field(None, alias="INFLUX_TOKEN")
    influx_default_bucket: Optional[str] = Field(None, alias="INFLUX_DEFAULT_BUCKET")

    # InfluxDB v1 Specific Settings
    influx_username: Optional[str] = Field(None, alias="INFLUX_USERNAME")
    influx_password: Optional[SecretStr] = Field(None, alias="INFLUX_PASSWORD")
    influx_default_db: Optional[str] = Field(None, alias="INFLUX_DEFAULT_DB")
    influx_default_rp: Optional[str] = Field(None, alias="INFLUX_DEFAULT_RP")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore" # Ignore extra env vars
    )

    def __repr__(self) -> str:
        # Mask sensitive fields for logging
        d = self.model_dump()
        d = mask_sensitive_data(d)
        return f"Settings({d})"

    def get_influx_token(self) -> Optional[str]:
        """Safely retrieves the string value of the Influx token."""
        if self.influx_token:
            return self.influx_token.get_secret_value()
        return None

    def get_influx_password(self) -> Optional[str]:
        """Safely retrieves the string value of the Influx password."""
        if self.influx_password:
            return self.influx_password.get_secret_value()
        return None


# --- Global Settings Instance ---

try:
    settings = Settings()
except ValidationError as e:
    logging.basicConfig(level="ERROR")
    logging.error(f"FATAL: Configuration validation failed:\n{e}")
    sys.exit(1)


# --- Configure Logging ---

def setup_logging(level: str = "INFO"):
    """
    Configures the application's logger (using Loguru for simplicity).
    """
    from loguru import logger

    logger.remove()
    logger.add(
        sys.stderr,
        level=level.upper(),
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
               "<level>{level: <8}</level> | "
               "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True,
    )
    # Intercept standard logging
    class InterceptHandler(logging.Handler):
        def emit(self, record):
            logger_opt = logger.opt(depth=6, exception=record.exc_info)
            logger_opt.log(record.levelno, record.getMessage())

    logging.basicConfig(handlers=[InterceptHandler()], level=0)

# Initial setup based on settings
setup_logging(settings.mcp_log_level)
