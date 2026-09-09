from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings loaded from ``POLYBOT_*`` variables and ``.env``."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="POLYBOT_",
        extra="ignore",
    )

    database_path: Path = Path("data/polybot.sqlite3")
    geoblock_url: str = "https://polymarket.com/api/geoblock"
    market_search_query: str = "highest temperature"
    max_events: int = Field(default=2, ge=1, le=20)

    astra_enabled: bool = False
    astra_model: str = "gpt-6-astra"
    astra_reasoning_effort: Literal["low", "medium", "high", "xhigh", "max"] = "low"
    astra_max_output_tokens: int = Field(default=1800, ge=256, le=8000)
    astra_budget_usd: Decimal = Decimal("5.00")
    astra_reserve_per_call_usd: Decimal = Decimal("0.25")
    astra_input_usd_per_million: Decimal = Decimal("10.00")
    astra_output_usd_per_million: Decimal = Decimal("50.00")
    openai_api_key: SecretStr | None = Field(default=None, validation_alias="OPENAI_API_KEY")
    openai_base_url: str = "https://api.openai.com/v1"
    openai_fallback_api_key: SecretStr | None = Field(
        default=None, validation_alias="OPENAI_FALLBACK_API_KEY"
    )
    openai_fallback_base_url: str | None = None

    max_event_risk_usd: Decimal = Decimal("2.00")
    max_total_risk_usd: Decimal = Decimal("6.00")
    daily_stop_loss_usd: Decimal = Decimal("2.00")
    total_drawdown_stop_usd: Decimal = Decimal("5.00")
    min_probability_edge: Decimal = Decimal("0.08")
    min_expected_profit_usd: Decimal = Decimal("0.25")
    execution_buffer_usd: Decimal = Decimal("0.02")
    max_book_age_seconds: int = Field(default=180, ge=10, le=3600)

    weather_error_sigma_c: float = Field(default=1.5, gt=0.1, le=10)
    min_ensemble_members: int = Field(default=20, ge=5, le=200)
    max_forecast_horizon_days: int = Field(default=10, ge=0, le=30)
    weather_geocoding_url: str = "https://geocoding-api.open-meteo.com/v1/search"
    weather_ensemble_url: str = "https://ensemble-api.open-meteo.com/v1/ensemble"
    weather_wrh_token_url: str = "https://www.weather.gov/source/wrh/apiKey.js"
    weather_synoptic_url: str = "https://api.synopticdata.com/v2/stations/timeseries"
    weather_awc_metar_url: str = "https://aviationweather.gov/api/data/metar"
    observation_default_cadence_minutes: int = Field(default=60, ge=10, le=180)
    observation_stale_multiplier: float = Field(default=2.5, ge=1.5, le=6)

    http_timeout_seconds: float = Field(default=20.0, ge=1, le=120)

    @field_validator(
        "astra_budget_usd",
        "astra_reserve_per_call_usd",
        "astra_input_usd_per_million",
        "astra_output_usd_per_million",
        "max_event_risk_usd",
        "max_total_risk_usd",
        "daily_stop_loss_usd",
        "total_drawdown_stop_usd",
        "min_probability_edge",
        "min_expected_profit_usd",
        "execution_buffer_usd",
    )
    @classmethod
    def _nonnegative_decimal(cls, value: Decimal) -> Decimal:
        if value < 0:
            raise ValueError("must be non-negative")
        return value

    def ensure_runtime_directories(self) -> None:
        self.database_path.expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
