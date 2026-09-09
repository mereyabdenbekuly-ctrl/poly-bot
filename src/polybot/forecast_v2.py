from __future__ import annotations

import hashlib
import json
import math
import statistics
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
from pydantic import Field, model_validator

from polybot.config import Settings
from polybot.forecast_store import ForecastStore
from polybot.models import Bracket, StrictModel, WeatherForecast
from polybot.observations import ObservationHistory

ECMWF_RAW_ALGORITHM_VERSION = "ecmwf-ifs025-raw-ensemble-v1"
FORECAST_V2_ALGORITHM_VERSION = "forecast-engine-v2-station-intraday@1"
ECMWF_IFS_PERTURBED_MEMBER_IDS = tuple(
    f"temperature_2m_max_member{number:02d}" for number in range(1, 51)
)


class EcmwfShadowSnapshot(StrictModel):
    """Exact lightweight ECMWF IFS ENS response used by the shadow engine.

    Open-Meteo is the transport/aggregation surface here, but the request pins
    the upstream model to ``ecmwf_ifs025``.  Missing run/publication metadata is
    kept null rather than inferred.  The official GRIB archiver is implemented
    separately in :mod:`polybot.ecmwf`.
    """

    transport_provider: str = "open-meteo"
    upstream_model: str
    requested_location: str
    matched_location: str
    latitude: float
    longitude: float
    timezone: str
    observation_date: str
    fetched_at_utc: datetime
    source_uri: str
    source_run_id: str | None = None
    init_time_utc: datetime | None = None
    published_at_utc: datetime | None = None
    payload_sha256: str
    archive_path: str
    member_max_c: list[Decimal] = Field(min_length=20, max_length=50)
    member_ids: list[str] | None = None
    control_field_present: bool | None = None
    response_latitude: float | None = None
    response_longitude: float | None = None
    response_elevation_m: float | None = None
    response_timezone: str | None = None
    grid_distance_km: float | None = None
    daily_units: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_provenance(self) -> EcmwfShadowSnapshot:
        if self.fetched_at_utc.tzinfo is None:
            raise ValueError("fetched_at_utc must be timezone-aware")
        if not self.source_uri.startswith("https://"):
            raise ValueError("ECMWF shadow source must use HTTPS")
        if self.member_ids is not None:
            if len(self.member_ids) != len(self.member_max_c):
                raise ValueError("ECMWF member IDs and values must have equal length")
            if len(set(self.member_ids)) != len(self.member_ids):
                raise ValueError("ECMWF member IDs must be unique")
            if self.upstream_model == "ecmwf_ifs025" and tuple(self.member_ids) != (
                ECMWF_IFS_PERTURBED_MEMBER_IDS
            ):
                raise ValueError("ECMWF IFS shadow must contain perturbed members 01..50")
        return self


@dataclass(frozen=True, slots=True)
class StationCorrectionProfile:
    state: str
    scope: str
    sample_count: int
    bias_c: Decimal
    spread_scale: Decimal
    residual_sigma_c: Decimal

    def as_metadata(self) -> dict[str, object]:
        return {
            "state": self.state,
            "scope": self.scope,
            "sample_count": self.sample_count,
            "bias_c": str(self.bias_c),
            "spread_scale": str(self.spread_scale),
            "residual_sigma_c": str(self.residual_sigma_c),
        }


@dataclass(frozen=True, slots=True)
class V2ForecastResult:
    raw_member_max_c: tuple[Decimal, ...]
    corrected_member_max_c: tuple[Decimal, ...]
    raw_probabilities: dict[str, Decimal]
    v2_probabilities: dict[str, Decimal]
    raw_point_c: Decimal
    v2_point_c: Decimal
    profile: StationCorrectionProfile
    intraday_features: dict[str, object]


class OpenMeteoEcmwfIfsEns:
    """Fetch an explicitly selected IFS ENS daily maximum and archive it.

    This small response is suitable for every five-minute shadow cycle.  It is
    intentionally not presented as the official ECMWF byte archive: the latter
    is handled by ``EcmwfIfsEnsAdapter`` and keeps GRIB/index artifacts.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def forecast_from_baseline(self, baseline: WeatherForecast) -> EcmwfShadowSnapshot:
        params = {
            "latitude": baseline.latitude,
            "longitude": baseline.longitude,
            "daily": "temperature_2m_max",
            "timezone": baseline.timezone,
            "start_date": baseline.observation_date.isoformat(),
            "end_date": baseline.observation_date.isoformat(),
            "models": self.settings.ecmwf_shadow_model,
        }
        response = httpx.get(
            self.settings.weather_ensemble_url,
            params=params,
            timeout=self.settings.http_timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("ECMWF shadow response is not an object")
        daily = payload.get("daily")
        if not isinstance(daily, dict):
            raise ValueError("ECMWF shadow response is missing daily data")
        dates = daily.get("time")
        date_value = baseline.observation_date.isoformat()
        if not isinstance(dates, list) or date_value not in dates:
            raise ValueError("ECMWF shadow response lacks the requested date")
        index = dates.index(date_value)
        member_rows: list[tuple[str, Decimal]] = []
        if self.settings.ecmwf_shadow_model == "ecmwf_ifs025":
            unexpected = sorted(
                key
                for key in daily
                if key.startswith("temperature_2m_max_member")
                and key not in ECMWF_IFS_PERTURBED_MEMBER_IDS
            )
            if unexpected:
                raise ValueError(
                    "ECMWF shadow returned unexpected member fields: " + ", ".join(unexpected)
                )
            for key in ECMWF_IFS_PERTURBED_MEMBER_IDS:
                values = daily.get(key)
                if not isinstance(values, list) or index >= len(values) or values[index] is None:
                    raise ValueError(f"ECMWF shadow is missing perturbed member field {key}")
                member_rows.append((key, Decimal(str(values[index]))))
        else:
            for key, values in daily.items():
                if not key.startswith("temperature_2m_max_member") or not isinstance(values, list):
                    continue
                if index >= len(values) or values[index] is None:
                    continue
                member_rows.append((key, Decimal(str(values[index]))))
            member_rows.sort(key=lambda item: item[0])
        if len(member_rows) < self.settings.ecmwf_min_members:
            raise ValueError(
                f"ECMWF shadow returned {len(member_rows)} members; "
                f"need {self.settings.ecmwf_min_members}"
            )
        # IFS ENS has 50 perturbed members on this access surface.  Refuse a
        # silently blended response with a different member count.
        if self.settings.ecmwf_shadow_model == "ecmwf_ifs025" and len(member_rows) != 50:
            raise ValueError(f"expected exactly 50 ECMWF IFS members, got {len(member_rows)}")

        fetched_at = datetime.now(UTC)
        canonical = json.dumps(
            {"request": params, "response": payload},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        digest = hashlib.sha256(canonical).hexdigest()
        control_field_present = "temperature_2m_max" in daily
        response_latitude = _optional_float(payload.get("latitude"))
        response_longitude = _optional_float(payload.get("longitude"))
        response_elevation = _optional_float(payload.get("elevation"))
        grid_distance = (
            None
            if response_latitude is None or response_longitude is None
            else _haversine_km(
                baseline.latitude,
                baseline.longitude,
                response_latitude,
                response_longitude,
            )
        )
        units_payload = payload.get("daily_units")
        daily_units = (
            {
                str(key): str(value)
                for key, value in units_payload.items()
                if isinstance(key, str) and isinstance(value, str)
            }
            if isinstance(units_payload, dict)
            else {}
        )
        archive_path = self._archive(
            digest=digest,
            payload=canonical,
            fetched_at=fetched_at,
            member_ids=[key for key, _ in member_rows],
            control_field_present=control_field_present,
            response_grid={
                "latitude": response_latitude,
                "longitude": response_longitude,
                "elevation_m": response_elevation,
                "timezone": payload.get("timezone"),
                "grid_distance_km": grid_distance,
                "daily_units": daily_units,
            },
        )
        return EcmwfShadowSnapshot(
            upstream_model=self.settings.ecmwf_shadow_model,
            requested_location=baseline.requested_location,
            matched_location=baseline.matched_location,
            latitude=baseline.latitude,
            longitude=baseline.longitude,
            timezone=baseline.timezone,
            observation_date=date_value,
            fetched_at_utc=fetched_at,
            source_uri=str(response.url),
            payload_sha256=digest,
            archive_path=str(archive_path),
            member_max_c=[value for _, value in member_rows],
            member_ids=[key for key, _ in member_rows],
            control_field_present=control_field_present,
            response_latitude=response_latitude,
            response_longitude=response_longitude,
            response_elevation_m=response_elevation,
            response_timezone=(
                str(payload["timezone"]) if isinstance(payload.get("timezone"), str) else None
            ),
            grid_distance_km=grid_distance,
            daily_units=daily_units,
        )

    def _archive(
        self,
        *,
        digest: str,
        payload: bytes,
        fetched_at: datetime,
        member_ids: list[str] | None = None,
        control_field_present: bool | None = None,
        response_grid: dict[str, object] | None = None,
    ) -> Path:
        root = self.settings.ecmwf_json_archive_root.expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        destination = root / digest
        if destination.exists():
            archived = destination / "snapshot.json"
            if hashlib.sha256(archived.read_bytes()).hexdigest() != digest:
                raise ValueError("existing ECMWF JSON archive hash mismatch")
            return destination

        staging = Path(tempfile.mkdtemp(prefix=".tmp-ecmwf-json-", dir=root))
        try:
            snapshot_path = staging / "snapshot.json"
            snapshot_path.write_bytes(payload)
            manifest = {
                "schema_version": 1,
                "payload_sha256": digest,
                "fetched_at_utc": fetched_at.isoformat(),
                "source": "Open-Meteo Ensemble API",
                "upstream_model": self.settings.ecmwf_shadow_model,
                "source_run_id": None,
                "init_time_utc": None,
                "published_at_utc": None,
                "perturbed_member_ids": member_ids,
                "control_field_present": control_field_present,
                "control_member_included": False,
                "response_grid": response_grid or {},
                "provider_processing_note": (
                    "Open-Meteo daily aggregation may include temporal interpolation and "
                    "terrain downscaling; this is not a raw station forecast."
                ),
                "provenance_note": (
                    "Transport pins ECMWF IFS ENS, but this response does not expose "
                    "the upstream run/publication timestamps."
                ),
            }
            (staging / "manifest.json").write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            for path in staging.iterdir():
                path.chmod(0o444)
            with suppress(FileExistsError):
                staging.rename(destination)
            destination.chmod(0o555)
            return destination
        finally:
            # Never recursively remove a caller-provided path.  The temporary
            # directory name and parent are both verified before cleanup.
            if (
                staging.exists()
                and staging.parent == root
                and staging.name.startswith(".tmp-ecmwf-json-")
            ):
                for child in staging.iterdir():
                    child.chmod(0o600)
                    child.unlink()
                staging.rmdir()


def _optional_float(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, str | int | float):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _haversine_km(
    latitude_a: float,
    longitude_a: float,
    latitude_b: float,
    longitude_b: float,
) -> float:
    radius_km = 6371.0088
    lat_a = math.radians(latitude_a)
    lat_b = math.radians(latitude_b)
    delta_lat = lat_b - lat_a
    delta_lon = math.radians(longitude_b - longitude_a)
    haversine = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat_a) * math.cos(lat_b) * math.sin(delta_lon / 2) ** 2
    )
    return 2 * radius_km * math.asin(min(1.0, math.sqrt(haversine)))


class ForecastV2Calibrator:
    """Simple, auditable bias/spread calibration with pooled fallback."""

    def __init__(self, settings: Settings, store: ForecastStore) -> None:
        self.settings = settings
        self.store = store

    def profile(self, *, station_id: str, as_of_utc: datetime) -> StationCorrectionProfile:
        station = self._samples(station_id=station_id, as_of_utc=as_of_utc)
        if len(station) >= self.settings.forecast_v2_station_min_samples:
            return self._fit(station, scope=f"station:{station_id}")
        pooled = self._samples(station_id=None, as_of_utc=as_of_utc)
        if len(pooled) >= self.settings.forecast_v2_pooled_min_samples:
            return self._fit(pooled, scope="pooled")
        return StationCorrectionProfile(
            state="insufficient_history",
            scope=f"station:{station_id}",
            sample_count=len(station),
            bias_c=Decimal(0),
            spread_scale=Decimal(1),
            residual_sigma_c=Decimal(str(self.settings.forecast_v2_default_residual_sigma_c)),
        )

    def _samples(
        self, *, station_id: str | None, as_of_utc: datetime
    ) -> list[tuple[Decimal, Decimal, Decimal]]:
        clauses = [
            "p.algorithm_version = ?",
            "p.phase = 'LEAD_TIME'",
            "p.issued_at_utc < ?",
            "o.recorded_at_utc < ?",
        ]
        params: list[object] = [
            ECMWF_RAW_ALGORITHM_VERSION,
            as_of_utc.isoformat(),
            as_of_utc.isoformat(),
        ]
        if station_id is not None:
            clauses.append("p.station_id = ?")
            params.append(station_id)
        with self.store.connect() as connection:
            rows = connection.execute(
                """
                SELECT p.id, p.event_id, p.point_forecast_c, o.actual_max_c
                FROM forecast_predictions_v2 p
                JOIN (
                    SELECT * FROM (
                        SELECT source_rows.*,
                               ROW_NUMBER() OVER (
                                   PARTITION BY event_id
                                   ORDER BY recorded_at_utc DESC, id DESC
                               ) AS revision_rank
                        FROM forecast_outcome_versions_v2 source_rows
                        WHERE recorded_at_utc < ?
                    ) ranked_outcomes WHERE revision_rank = 1
                ) o ON o.event_id = p.event_id
                WHERE """
                + " AND ".join(clauses)
                + " ORDER BY p.issued_at_utc, p.id",
                (as_of_utc.isoformat(), *params),
            ).fetchall()
            latest: dict[str, Any] = {}
            for row in rows:
                latest[str(row["event_id"])] = row
            result: list[tuple[Decimal, Decimal, Decimal]] = []
            for row in latest.values():
                scenario_rows = connection.execute(
                    "SELECT adjusted_max_c FROM forecast_scenarios_v2 WHERE prediction_id = ?",
                    (row["id"],),
                ).fetchall()
                values = [Decimal(item["adjusted_max_c"]) for item in scenario_rows]
                if not values:
                    continue
                result.append(
                    (
                        Decimal(row["point_forecast_c"]),
                        Decimal(row["actual_max_c"]),
                        _stddev(values),
                    )
                )
        return result

    def _fit(
        self, samples: list[tuple[Decimal, Decimal, Decimal]], *, scope: str
    ) -> StationCorrectionProfile:
        errors = [actual - predicted for predicted, actual, _ in samples]
        bias = sum(errors, Decimal(0)) / Decimal(len(errors))
        centered = [float(error - bias) for error in errors]
        rms = math.sqrt(sum(x * x for x in centered) / len(centered))
        residual = Decimal(str(max(0.35, min(4.0, rms))))
        spreads = [spread for _, _, spread in samples if spread > 0]
        mean_spread = sum(spreads, Decimal(0)) / Decimal(len(spreads)) if spreads else Decimal(0)
        scale = Decimal(1) if mean_spread <= 0 else residual / mean_spread
        scale = max(Decimal("0.5"), min(Decimal("2.5"), scale))
        return StationCorrectionProfile(
            state="fitted",
            scope=scope,
            sample_count=len(samples),
            bias_c=bias,
            spread_scale=scale,
            residual_sigma_c=residual,
        )


def build_v2_forecast(
    *,
    snapshot: EcmwfShadowSnapshot,
    observations: ObservationHistory,
    brackets: dict[str, Bracket],
    profile: StationCorrectionProfile,
    issued_at_utc: datetime,
) -> V2ForecastResult:
    raw = tuple(snapshot.member_max_c)
    raw_mean = _mean(raw)
    corrected = tuple(
        raw_mean + profile.bias_c + profile.spread_scale * (value - raw_mean) for value in raw
    )
    observed_floor = observations.observed_max_c
    adjusted = tuple(
        max(value, observed_floor) if observed_floor is not None else value for value in corrected
    )
    raw_probabilities = _empirical_probabilities(raw, brackets)
    v2_probabilities = _kernel_probabilities(
        adjusted,
        brackets,
        sigma=profile.residual_sigma_c,
        observed_floor=observed_floor,
    )
    return V2ForecastResult(
        raw_member_max_c=raw,
        corrected_member_max_c=adjusted,
        raw_probabilities=raw_probabilities,
        v2_probabilities=v2_probabilities,
        raw_point_c=raw_mean,
        v2_point_c=_mean(adjusted),
        profile=profile,
        intraday_features=extract_intraday_features(
            observations=observations,
            issued_at_utc=issued_at_utc,
        ),
    )


def extract_intraday_features(
    *, observations: ObservationHistory, issued_at_utc: datetime
) -> dict[str, object]:
    rows = [
        item
        for item in observations.observations
        if item.observed_at_utc <= issued_at_utc and item.first_seen_at_utc <= issued_at_utc
    ]
    latest = rows[-1] if rows else None
    available_observed_max = max(
        (item.temperature_c for item in rows),
        default=None,
    )
    trend = None
    if len(rows) >= 2:
        cutoff = rows[-1].observed_at_utc.timestamp() - 3 * 3600
        earlier = next(
            (item for item in reversed(rows[:-1]) if item.observed_at_utc.timestamp() <= cutoff),
            rows[0],
        )
        trend = rows[-1].temperature_c - earlier.temperature_c
    raw = {} if latest is None else latest.raw_payload
    return {
        "observed_max_c": (None if available_observed_max is None else str(available_observed_max)),
        "latest_temperature_c": None if latest is None else str(latest.temperature_c),
        "temperature_change_3h_c": None if trend is None else str(trend),
        "latest_observed_at_utc": (None if latest is None else latest.observed_at_utc.isoformat()),
        "wind_speed": _first_raw_feature(raw, "wind_speed"),
        "cloud": _first_raw_feature(raw, "cloud"),
        "feature_adjustment_applied": False,
        "feature_adjustment_reason": "awaiting out-of-sample validation",
    }


def _first_raw_feature(payload: dict[str, Any], needle: str) -> object | None:
    for key, value in payload.items():
        if needle in key.casefold() and value is not None:
            return value if isinstance(value, str | int | float | bool) else str(value)
    return None


def _empirical_probabilities(
    values: tuple[Decimal, ...], brackets: dict[str, Bracket]
) -> dict[str, Decimal]:
    counts = {market_id: 0 for market_id in brackets}
    for value in values:
        matched = [
            market_id
            for market_id, bracket in brackets.items()
            if _value_in_bracket(value, bracket)
        ]
        if len(matched) != 1:
            raise ValueError(f"ECMWF member {value} matched {len(matched)} market brackets")
        counts[matched[0]] += 1
    total = Decimal(len(values))
    return {market_id: Decimal(count) / total for market_id, count in counts.items()}


def _value_in_bracket(value: Decimal, bracket: Bracket) -> bool:
    lower = None if bracket.lower is None else Decimal(str(bracket.lower))
    upper = None if bracket.upper is None else Decimal(str(bracket.upper))
    lower_ok = lower is None or value > lower or (bracket.lower_inclusive and value == lower)
    upper_ok = upper is None or value < upper or (bracket.upper_inclusive and value == upper)
    return lower_ok and upper_ok


def _kernel_probabilities(
    values: tuple[Decimal, ...],
    brackets: dict[str, Bracket],
    *,
    sigma: Decimal,
    observed_floor: Decimal | None,
) -> dict[str, Decimal]:
    if sigma <= 0:
        raise ValueError("residual sigma must be positive")
    raw: dict[str, Decimal] = {}
    for market_id, bracket in brackets.items():
        probabilities = [
            _truncated_probability(
                mean=float(value),
                sigma=float(sigma),
                lower=bracket.lower,
                upper=bracket.upper,
                floor=None if observed_floor is None else float(observed_floor),
            )
            for value in values
        ]
        raw[market_id] = Decimal(str(sum(probabilities) / len(probabilities)))
    mass = sum(raw.values(), Decimal(0))
    if mass <= 0:
        raise ValueError("v2 probability distribution has zero mass")
    return {market_id: value / mass for market_id, value in raw.items()}


def _truncated_probability(
    *, mean: float, sigma: float, lower: float | None, upper: float | None, floor: float | None
) -> float:
    if floor is None:
        return _interval_probability(mean, sigma, lower, upper)
    if upper is not None and upper <= floor:
        return 0.0
    effective_lower = floor if lower is None else max(lower, floor)
    numerator = _interval_probability(mean, sigma, effective_lower, upper)
    denominator = 1.0 - _normal_cdf((floor - mean) / sigma)
    return 0.0 if denominator <= 0 else max(0.0, min(1.0, numerator / denominator))


def _interval_probability(
    mean: float, sigma: float, lower: float | None, upper: float | None
) -> float:
    lower_cdf = 0.0 if lower is None else _normal_cdf((lower - mean) / sigma)
    upper_cdf = 1.0 if upper is None else _normal_cdf((upper - mean) / sigma)
    return max(0.0, min(1.0, upper_cdf - lower_cdf))


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _mean(values: tuple[Decimal, ...]) -> Decimal:
    return sum(values, Decimal(0)) / Decimal(len(values))


def _stddev(values: list[Decimal]) -> Decimal:
    if len(values) < 2:
        return Decimal(0)
    return Decimal(str(statistics.pstdev(float(value) for value in values)))
