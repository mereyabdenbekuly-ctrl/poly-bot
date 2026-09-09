from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from pydantic import Field

from polybot.config import Settings
from polybot.models import RuleInterpretation, StrictModel

_TOKEN_RE = re.compile(r"mesoToken='(?P<token>[^']+)'", re.I)
_STATION_RE = re.compile(r"^[A-Z0-9]{4}$")
_KNOWN_CADENCE_MINUTES = {"EDDM": 30, "ZUUU": 60}


class ObservationVersion(StrictModel):
    station_id: str
    station_timezone: str
    observed_at_utc: datetime
    first_seen_at_utc: datetime
    source: str
    source_url: str
    temperature_c: Decimal
    displayed_temperature_c: Decimal
    raw_payload: dict[str, Any]
    revision_hash: str
    corrected: bool
    awc_receipt_time_utc: datetime | None = None
    awc_temperature_c: Decimal | None = None


class ObservationHistory(StrictModel):
    station_id: str
    station_name: str
    station_timezone: str
    source_url: str
    observation_date: date
    fetched_at_utc: datetime
    day_started: bool
    day_finished: bool
    expected_cadence_minutes: int
    observations: list[ObservationVersion]
    observed_max_c: Decimal | None
    displayed_max_c: Decimal | None
    latest_observed_at_utc: datetime | None
    stale: bool
    gaps: list[str] = Field(default_factory=list)
    crosscheck_mismatches: list[str] = Field(default_factory=list)
    blocking_reasons: list[str] = Field(default_factory=list)
    warning_reasons: list[str] = Field(default_factory=list)


class ObservationError(RuntimeError):
    pass


class StationObservationCollector:
    """Collect the exact WRH/Synoptic station history and AWC cross-check."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._weather_gov_token: str | None = None

    def fetch(self, rules: RuleInterpretation) -> ObservationHistory:
        if rules.observation_date is None or rules.resolution_source_url is None:
            raise ObservationError("observation date and resolution source URL are required")
        station_id = station_id_from_source_url(rules.resolution_source_url)
        fetched_at = datetime.now(UTC)
        metadata = self._fetch_synoptic(station_id=station_id, recent_minutes=180)
        station = self._single_station(metadata, station_id)
        timezone_name = _required_string(station, "TIMEZONE")
        station_name = _required_string(station, "NAME")
        try:
            timezone = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as error:
            raise ObservationError(f"unknown station timezone: {timezone_name}") from error

        local_start = datetime.combine(rules.observation_date, datetime.min.time(), tzinfo=timezone)
        local_end = local_start + timedelta(days=1)
        utc_start = local_start.astimezone(UTC)
        utc_end = local_end.astimezone(UTC)
        now_local = fetched_at.astimezone(timezone)
        day_started = now_local >= local_start
        day_finished = now_local >= local_end

        # Synoptic rejects a START in the future.  Station metadata is already
        # sufficient to establish identity/timezone for a lead-time forecast;
        # return explicit empty evidence instead of turning a normal pre-event
        # state into a source outage.
        if not day_started:
            return ObservationHistory(
                station_id=station_id,
                station_name=station_name,
                station_timezone=timezone_name,
                source_url=rules.resolution_source_url,
                observation_date=rules.observation_date,
                fetched_at_utc=fetched_at,
                day_started=False,
                day_finished=False,
                expected_cadence_minutes=_KNOWN_CADENCE_MINUTES.get(
                    station_id, self.settings.observation_default_cadence_minutes
                ),
                observations=[],
                observed_max_c=None,
                displayed_max_c=None,
                latest_observed_at_utc=None,
                stale=False,
                warning_reasons=["OBSERVATION_DAY_NOT_STARTED"],
            )

        payload = self._fetch_synoptic(
            station_id=station_id,
            start_utc=utc_start,
            end_utc=utc_end - timedelta(minutes=1),
        )
        station = self._single_station(payload, station_id)
        if _required_string(station, "TIMEZONE") != timezone_name:
            raise ObservationError("station timezone changed between metadata and history")
        units = payload.get("UNITS")
        if not isinstance(units, dict) or units.get("air_temp") != "Celsius":
            raise ObservationError("Synoptic air temperature unit is not Celsius")
        observations_payload = station.get("OBSERVATIONS")
        if observations_payload is None:
            observations_payload = {}
        if not isinstance(observations_payload, dict):
            raise ObservationError("Synoptic OBSERVATIONS is not an object")

        awc_by_time = self._fetch_awc_by_time(
            station_id=station_id,
            utc_start=utc_start,
            utc_end=min(utc_end, fetched_at + timedelta(hours=1)),
        )
        observations = self._parse_observations(
            station_id=station_id,
            timezone_name=timezone_name,
            source_url=rules.resolution_source_url,
            payload=observations_payload,
            observation_date=rules.observation_date,
            fetched_at=fetched_at,
            awc_by_time=awc_by_time,
        )
        cadence = _KNOWN_CADENCE_MINUTES.get(
            station_id, self.settings.observation_default_cadence_minutes
        )
        gaps = _find_gaps(
            observations=observations,
            local_start=local_start,
            cadence_minutes=cadence,
            now=fetched_at,
        )
        latest = observations[-1].observed_at_utc if observations else None
        stale_after = timedelta(minutes=cadence * self.settings.observation_stale_multiplier)
        stale = bool(
            day_started
            and not day_finished
            and (latest is None or fetched_at - latest > stale_after)
        )
        mismatches = [
            f"{item.observed_at_utc.isoformat()}: WRH={item.temperature_c}, "
            f"AWC={item.awc_temperature_c}"
            for item in observations
            if item.awc_temperature_c is not None and item.awc_temperature_c != item.temperature_c
        ]
        blocking: list[str] = []
        warnings: list[str] = []
        if day_started and not observations:
            blocking.append("OBSERVATION_HISTORY_EMPTY")
        if stale:
            blocking.append("OBSERVATION_SOURCE_STALE")
        if mismatches:
            blocking.append("OBSERVATION_CROSSCHECK_MISMATCH")
        if rules.location and rules.location.casefold() not in station_name.casefold():
            blocking.append("OBSERVATION_STATION_IDENTITY_MISMATCH")
        if gaps:
            warnings.append("OBSERVATION_HISTORY_HAS_GAPS")
        if not day_started:
            warnings.append("OBSERVATION_DAY_NOT_STARTED")

        temperatures = [item.temperature_c for item in observations]
        displayed = [item.displayed_temperature_c for item in observations]
        return ObservationHistory(
            station_id=station_id,
            station_name=station_name,
            station_timezone=timezone_name,
            source_url=rules.resolution_source_url,
            observation_date=rules.observation_date,
            fetched_at_utc=fetched_at,
            day_started=day_started,
            day_finished=day_finished,
            expected_cadence_minutes=cadence,
            observations=observations,
            observed_max_c=max(temperatures) if temperatures else None,
            displayed_max_c=max(displayed) if displayed else None,
            latest_observed_at_utc=latest,
            stale=stale,
            gaps=gaps,
            crosscheck_mismatches=mismatches,
            blocking_reasons=blocking,
            warning_reasons=warnings,
        )

    def _fetch_token(self) -> str:
        if self._weather_gov_token is not None:
            return self._weather_gov_token
        response = httpx.get(
            self.settings.weather_wrh_token_url,
            timeout=self.settings.http_timeout_seconds,
            follow_redirects=True,
        )
        response.raise_for_status()
        match = _TOKEN_RE.search(response.text)
        if match is None:
            raise ObservationError("weather.gov WRH token was not found")
        token = match.group("token")
        self._weather_gov_token = token
        return token

    def _fetch_synoptic(
        self,
        *,
        station_id: str,
        recent_minutes: int | None = None,
        start_utc: datetime | None = None,
        end_utc: datetime | None = None,
    ) -> dict[str, Any]:
        params: dict[str, str] = {
            "STID": station_id,
            "showemptystations": "1",
            "complete": "1",
            "token": self._fetch_token(),
            "obtimezone": "utc",
            "units": "metric",
        }
        if recent_minutes is not None:
            params["recent"] = str(recent_minutes)
        elif start_utc is not None and end_utc is not None:
            params["start"] = start_utc.strftime("%Y%m%d%H%M")
            params["end"] = end_utc.strftime("%Y%m%d%H%M")
        else:
            raise ValueError("recent_minutes or start/end is required")
        response = httpx.get(
            self.settings.weather_synoptic_url,
            params=params,
            headers={
                "User-Agent": "Mozilla/5.0 polybot-weather-research/0.1",
                "Origin": "https://www.weather.gov",
                "Referer": f"https://www.weather.gov/wrh/timeseries?site={station_id.lower()}",
            },
            timeout=self.settings.http_timeout_seconds,
            follow_redirects=True,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise ObservationError("Synoptic response is not an object")
        summary = data.get("SUMMARY")
        if isinstance(summary, dict) and summary.get("RESPONSE_CODE") not in (1, "1"):
            raise ObservationError(f"Synoptic rejected request: {summary.get('RESPONSE_MESSAGE')}")
        return data

    @staticmethod
    def _single_station(payload: dict[str, Any], expected_id: str) -> dict[str, Any]:
        rows = payload.get("STATION")
        if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
            raise ObservationError(f"Synoptic returned no unique station for {expected_id}")
        station = rows[0]
        actual = _required_string(station, "STID").upper()
        if actual != expected_id:
            raise ObservationError(f"station mismatch: requested {expected_id}, received {actual}")
        return station

    def _fetch_awc_by_time(
        self, *, station_id: str, utc_start: datetime, utc_end: datetime
    ) -> dict[datetime, dict[str, Any]]:
        hours = max(
            1, min(360, math.ceil((datetime.now(UTC) - utc_start).total_seconds() / 3600) + 2)
        )
        response = httpx.get(
            self.settings.weather_awc_metar_url,
            params={"ids": station_id, "format": "json", "hours": str(hours)},
            headers={"User-Agent": "polybot-weather-research/0.1"},
            timeout=self.settings.http_timeout_seconds,
            follow_redirects=True,
        )
        response.raise_for_status()
        rows = response.json()
        if not isinstance(rows, list):
            raise ObservationError("AWC METAR response is not an array")
        result: dict[datetime, dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict) or str(row.get("icaoId", "")).upper() != station_id:
                continue
            raw_time = row.get("obsTime")
            if not isinstance(raw_time, int | float):
                continue
            observed = datetime.fromtimestamp(raw_time, UTC)
            if utc_start <= observed < utc_end:
                result[observed] = row
        return result

    @staticmethod
    def _parse_observations(
        *,
        station_id: str,
        timezone_name: str,
        source_url: str,
        payload: dict[str, Any],
        observation_date: date,
        fetched_at: datetime,
        awc_by_time: dict[datetime, dict[str, Any]],
    ) -> list[ObservationVersion]:
        dates = payload.get("date_time")
        temperatures = payload.get("air_temp_set_1")
        if dates is None and temperatures is None:
            return []
        if not isinstance(dates, list) or not isinstance(temperatures, list):
            raise ObservationError("Synoptic date_time/air_temp_set_1 arrays are missing")
        if len(dates) != len(temperatures):
            raise ObservationError("Synoptic observation arrays are not aligned")
        timezone = ZoneInfo(timezone_name)
        observations: list[ObservationVersion] = []
        for index, raw_date in enumerate(dates):
            raw_temp = temperatures[index]
            if raw_temp is None:
                continue
            observed = _parse_datetime(raw_date)
            if observed.astimezone(timezone).date() != observation_date:
                continue
            raw_record = {
                key: value[index]
                for key, value in payload.items()
                if isinstance(value, list) and index < len(value)
            }
            raw_record["station_id"] = station_id
            canonical = json.dumps(raw_record, sort_keys=True, separators=(",", ":"), default=str)
            revision_hash = hashlib.sha256(canonical.encode()).hexdigest()
            temperature = Decimal(str(raw_temp))
            awc = awc_by_time.get(observed)
            awc_temp = None if awc is None or awc.get("temp") is None else Decimal(str(awc["temp"]))
            receipt = None if awc is None else _parse_datetime_or_none(awc.get("receiptTime"))
            raw_metar = str(raw_record.get("metar_set_1") or "")
            observations.append(
                ObservationVersion(
                    station_id=station_id,
                    station_timezone=timezone_name,
                    observed_at_utc=observed,
                    first_seen_at_utc=fetched_at,
                    source="weather.gov-wrh-synoptic",
                    source_url=source_url,
                    temperature_c=temperature,
                    displayed_temperature_c=Decimal(math.floor(float(temperature) + 0.5)),
                    raw_payload=raw_record,
                    revision_hash=revision_hash,
                    corrected=bool(re.search(r"\bCOR\b", raw_metar)),
                    awc_receipt_time_utc=receipt,
                    awc_temperature_c=awc_temp,
                )
            )
        return sorted(observations, key=lambda item: item.observed_at_utc)


def station_id_from_source_url(source_url: str) -> str:
    parsed = urlparse(source_url)
    if parsed.scheme != "https" or parsed.hostname not in {"weather.gov", "www.weather.gov"}:
        raise ObservationError("resolution source is not an HTTPS weather.gov URL")
    if parsed.path.rstrip("/") != "/wrh/timeseries":
        raise ObservationError("resolution source is not the WRH timeseries page")
    values = parse_qs(parsed.query).get("site", [])
    if len(values) != 1:
        raise ObservationError("resolution source has no unique site parameter")
    station_id = values[0].upper()
    if _STATION_RE.fullmatch(station_id) is None:
        raise ObservationError(f"invalid station id: {station_id!r}")
    return station_id


def apply_observed_max(member_values: list[float], observed_max_c: Decimal | None) -> list[float]:
    if observed_max_c is None:
        return list(member_values)
    floor_value = float(observed_max_c)
    return [max(value, floor_value) for value in member_values]


def bracket_is_impossible(*, upper: float | None, observed_display_max_c: Decimal | None) -> bool:
    return bool(
        upper is not None
        and observed_display_max_c is not None
        and observed_display_max_c >= Decimal(str(upper))
    )


def _find_gaps(
    *,
    observations: list[ObservationVersion],
    local_start: datetime,
    cadence_minutes: int,
    now: datetime,
) -> list[str]:
    if not observations:
        return []
    threshold = timedelta(minutes=cadence_minutes * 2)
    gaps: list[str] = []
    first_local = observations[0].observed_at_utc.astimezone(local_start.tzinfo)
    if first_local - local_start > threshold:
        gaps.append(f"start->{first_local.isoformat()}")
    for left, right in zip(observations, observations[1:], strict=False):
        if right.observed_at_utc - left.observed_at_utc > threshold:
            gaps.append(f"{left.observed_at_utc.isoformat()}->{right.observed_at_utc.isoformat()}")
    return gaps


def _required_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ObservationError(f"missing station {key}")
    return value


def _parse_datetime(value: object) -> datetime:
    if not isinstance(value, str):
        raise ObservationError(f"invalid datetime: {value!r}")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ObservationError(f"datetime has no timezone: {value!r}")
    return parsed.astimezone(UTC)


def _parse_datetime_or_none(value: object) -> datetime | None:
    return None if value is None else _parse_datetime(value)
