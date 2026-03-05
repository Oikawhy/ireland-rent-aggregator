"""
AGPARS Configuration Module

Centralized configuration using pydantic-settings.
All settings are loaded from environment variables with sensible defaults.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """PostgreSQL database configuration."""

    model_config = SettingsConfigDict(env_prefix="POSTGRES_", env_file=".env", env_file_encoding="utf-8", extra="ignore")

    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=5432, description="Database port")
    user: str = Field(default="agpars", description="Database user")
    password: str = Field(default="agpars_dev", description="Database password")
    db: str = Field(default="agpars", description="Database name")

    @property
    def url(self) -> str:
        """Generate SQLAlchemy database URL."""
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"

    @property
    def async_url(self) -> str:
        """Generate async SQLAlchemy database URL."""
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"


class RedisSettings(BaseSettings):
    """Redis configuration."""

    model_config = SettingsConfigDict(env_prefix="REDIS_", env_file=".env", env_file_encoding="utf-8", extra="ignore")

    host: str = Field(default="localhost", description="Redis host")
    port: int = Field(default=6379, description="Redis port")
    db: int = Field(default=0, description="Redis database number")
    password: str | None = Field(default=None, description="Redis password")

    @property
    def url(self) -> str:
        """Generate Redis URL."""
        auth = f":{self.password}@" if self.password else ""
        return f"redis://{auth}{self.host}:{self.port}/{self.db}"


class TelegramSettings(BaseSettings):
    """Telegram Bot configuration."""

    model_config = SettingsConfigDict(env_prefix="TELEGRAM_", env_file=".env", env_file_encoding="utf-8", extra="ignore")

    bot_token: str = Field(default="", description="Telegram Bot API token")
    rate_limit_per_second: int = Field(default=30, description="Max messages per second")
    webhook_url: str | None = Field(default=None, description="Webhook URL for production")
    admin_user_id: int = Field(default=578565554, description="First admin Telegram user ID")
    admin_group_id: int = Field(default=-5292864055, description="Admin group chat ID for access requests")


class PublisherSettings(BaseSettings):
    """Publisher-ETL configuration."""

    model_config = SettingsConfigDict(env_prefix="PUBLISH_", env_file=".env", env_file_encoding="utf-8", extra="ignore")

    interval_seconds: int = Field(default=60, description="Sync interval in seconds")
    batch_size: int = Field(default=500, description="Max rows per sync batch")


class CollectorSettings(BaseSettings):
    """Collector/Scraper configuration."""

    model_config = SettingsConfigDict(env_prefix="COLLECTOR_", env_file=".env", env_file_encoding="utf-8", extra="ignore")

    max_concurrent_browsers: int = Field(default=2, description="Max parallel browser contexts")
    default_timeout_ms: int = Field(default=30000, description="Default page timeout")
    max_retries: int = Field(default=3, description="Max job retries before DEAD")
    headless: bool = Field(default=True, description="Run browsers in headless mode")


class RetentionSettings(BaseSettings):
    """Data retention configuration."""

    model_config = SettingsConfigDict(env_prefix="RETENTION_", env_file=".env", env_file_encoding="utf-8", extra="ignore")

    listings_days: int = Field(default=90, description="Days to keep listings after last_seen")
    events_delivered_days: int = Field(default=30, description="Days to keep delivered events")
    events_dead_days: int = Field(default=90, description="Days to keep dead events")
    delivery_log_days: int = Field(default=60, description="Days to keep delivery logs")
    job_log_days: int = Field(default=30, description="Days to keep job logs")


class ObservabilitySettings(BaseSettings):
    """Observability configuration."""

    model_config = SettingsConfigDict(env_prefix="OBSERVABILITY_", env_file=".env", env_file_encoding="utf-8", extra="ignore")

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO", description="Logging level"
    )
    log_format: Literal["json", "console"] = Field(
        default="console", description="Log output format"
    )
    metrics_port: int = Field(default=9090, description="Prometheus metrics port")


class Settings(BaseSettings):
    """Main application settings - aggregates all config sections."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Application metadata
    app_name: str = Field(default="AGPARS", description="Application name")
    environment: Literal["development", "staging", "production", "dev", "prod", "stag"] = Field(
        default="development", description="Deployment environment"
    )
    debug: bool = Field(default=False, description="Enable debug mode")
    timezone: str = Field(default="Europe/Dublin", description="Default timezone")

    # Sub-configurations
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    telegram: TelegramSettings = Field(default_factory=TelegramSettings)
    publisher: PublisherSettings = Field(default_factory=PublisherSettings)
    collector: CollectorSettings = Field(default_factory=CollectorSettings)
    retention: RetentionSettings = Field(default_factory=RetentionSettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)


@lru_cache
def get_settings() -> Settings:
    """
    Get cached application settings.

    Uses lru_cache to ensure settings are loaded only once.
    Clear cache with: get_settings.cache_clear()
    """
    return Settings()


# Convenience alias
settings = get_settings()
