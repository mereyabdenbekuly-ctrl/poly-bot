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
    # Broader discovery applies only when --paper is active. Observe-mode keeps
    # its small default, and all per-event/global exposure controls remain the
    # same regardless of this research coverage setting.
    paper_max_events: int = Field(default=8, ge=1, le=20)

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
    # ECMWF IFS ENS is collected as a shadow source.  It never changes v1
    # paper decisions; the explicit model selector prevents Open-Meteo from
    # silently blending it with other ensembles.
    ecmwf_enabled: bool = False
    ecmwf_shadow_model: str = "ecmwf_ifs025"
    ecmwf_json_archive_root: Path = Path("data/forecasts/ecmwf-ifs025-json")
    ecmwf_min_members: int = Field(default=50, ge=20, le=50)
    forecast_v2_enabled: bool = False
    forecast_v2_station_min_samples: int = Field(default=5, ge=2, le=1000)
    forecast_v2_pooled_min_samples: int = Field(default=20, ge=5, le=10000)
    forecast_v2_default_residual_sigma_c: float = Field(default=1.5, gt=0.1, le=10)
    forecast_snapshot_min_interval_seconds: int = Field(default=3600, ge=300, le=21600)
    forecast_snapshot_probability_delta: Decimal = Decimal("0.02")
    forecast_outcome_monitor_hours: int = Field(default=336, ge=24, le=2160)
    weather_wrh_token_url: str = "https://www.weather.gov/source/wrh/apiKey.js"
    weather_synoptic_url: str = "https://api.synopticdata.com/v2/stations/timeseries"
    weather_awc_metar_url: str = "https://aviationweather.gov/api/data/metar"
    observation_default_cadence_minutes: int = Field(default=60, ge=10, le=180)
    observation_stale_multiplier: float = Field(default=2.5, ge=1.5, le=6)
    weathernext_enabled: bool = False
    weathernext_surface: str = "gcs_full_ensemble"
    weathernext_snapshot_path: str | None = None
    # Official WeatherNext statistics surface.  This is deliberately kept
    # separate from the raw 64-member export: statistics are descriptive
    # percentiles and must never be interpreted as synthetic scenarios.
    weathernext_statistics_snapshot_path: str | None = None
    weathernext_statistics_variable: Literal[
        "temperature_2m", "station_head_temperature_2m"
    ] = "station_head_temperature_2m"
    weathernext_statistics_bucket: str = "weathernext3_statistics_spatial"
    weathernext_statistics_prefix: str = "weathernext_3_0_0_statistics/zarr"
    weathernext_statistics_store_prefix: str | None = None
    weathernext_statistics_read_max_bytes: int = Field(default=2_000_000_000, ge=1_000_000)
    # Keep an implicit refresh small enough to remain below the transfer
    # ceiling.  A full local day is intentionally opt-in via an explicit
    # bounded setting and may still be refused before any payload read.
    weathernext_statistics_max_hours: int = Field(default=4, ge=1, le=48)
    # Requester Pays: the billing project charged for WeatherNext3 GCS reads.
    # Google access uses local ADC only; API keys are intentionally unsupported.
    weathernext_gcs_bucket: str = "weathernext3_spatial"
    # The full-ensemble WeatherNext 3 bucket is a Zarr-v3 hierarchy.  Keep the
    # root configurable because historical and operational archives live below
    # different year prefixes.  A concrete ``.../predictions.zarr`` prefix can
    # be supplied to avoid discovery/listing during a refresh.
    weathernext_gcs_prefix: str = "weathernext_3_0_0/zarr"
    weathernext_gcs_store_prefix: str | None = None
    weathernext_gcs_project: str | None = None
    # Raw full-ensemble station chunks contain the global grid and can be
    # hundreds of gigabytes for one daily point extraction.  Keep reads
    # opt-in and bounded unless an operator explicitly overrides the guard.
    weathernext_raw_read_max_bytes: int = Field(default=2_000_000_000, ge=1_000_000)

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
        "forecast_snapshot_probability_delta",
    )
    @classmethod
    def _nonnegative_decimal(cls, value: Decimal) -> Decimal:
        if value < 0:
            raise ValueError("must be non-negative")
        return value

    def ensure_runtime_directories(self) -> None:
        self.database_path.expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
