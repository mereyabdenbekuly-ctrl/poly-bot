from datetime import UTC, date, datetime

from polybot.models import RuleInterpretation
from polybot.observations import ObservationHistory
from polybot.scanner import _runtime_rule_ambiguities


def history() -> ObservationHistory:
    return ObservationHistory(
        station_id="EDDM",
        station_name="Munich Airport",
        station_timezone="Europe/Berlin",
        source_url="https://www.weather.gov/wrh/timeseries?site=eddm",
        observation_date=date(2026, 9, 9),
        fetched_at_utc=datetime.now(UTC),
        day_started=True,
        day_finished=False,
        expected_cadence_minutes=30,
        observations=[],
        observed_max_c=None,
        displayed_max_c=None,
        latest_observed_at_utc=None,
        stale=False,
    )


def interpretation(*reasons: str) -> RuleInterpretation:
    return RuleInterpretation(
        event_type="daily_max_temperature",
        tradeable=False,
        location="Munich",
        observation_date=date(2026, 9, 9),
        unit="C",
        precision_decimal_places=0,
        station_or_authority="Munich Airport",
        resolution_source_url="https://www.weather.gov/wrh/timeseries?site=eddm",
        source_local_date=False,
        bucket_semantics_clear=True,
        ambiguity_reasons=list(reasons),
        summary="test",
        confidence=1,
    )


def test_station_timezone_evidence_resolves_that_specific_ambiguity() -> None:
    blockers, warnings = _runtime_rule_ambiguities(
        interpretation("The observation-day timezone is not specified."), history()
    )
    assert blockers == []
    assert warnings == ["RULE_TIMEZONE_VERIFIED_FROM_STATION_SOURCE"]


def test_unresolved_rule_ambiguity_blocks_entry() -> None:
    blockers, warnings = _runtime_rule_ambiguities(
        interpretation("The temperature unit is unclear."), history()
    )
    assert blockers == ["UNRESOLVED_RULE_AMBIGUITY"]
    assert warnings == []
