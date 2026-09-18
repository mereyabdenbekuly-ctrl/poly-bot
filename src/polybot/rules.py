from __future__ import annotations

import hashlib
import json
import re
from decimal import Decimal

from polybot.models import Bracket, EventDefinition, RuleAudit, RuleInterpretation

_TITLE_RE = re.compile(r"^Highest temperature in (?P<location>.+?) on .+?\??$", re.I)
_URL_RE = re.compile(r"https?://[^\s)]+")
_STATION_RE = re.compile(
    r"highest temperature recorded by (?P<station>.+?) in degrees", re.I | re.S
)
_PRECISION_RE = re.compile(
    r"(?:to|measures temperatures to)\s+"
    r"(?:(?P<word>one|two|three|\d+)\s+decimal places?|(?P<whole>whole degrees?))",
    re.I,
)
_LABEL_RE = re.compile(
    r"^\s*(?P<value>-?\d+(?:\.\d+)?)\s*°?\s*(?P<unit>[CF])?"
    r"(?:\s+(?P<tail>or below|or lower|or higher|or above))?\s*$",
    re.I,
)
_RANGE_RE = re.compile(
    r"^\s*(?P<value>-?\d+(?:\.\d+)?)\s*-\s*(?P<high>-?\d+(?:\.\d+)?)"
    r"\s*°?\s*(?P<unit>[CF])?\s*$",
    re.I,
)


def rules_hash(event: EventDefinition) -> str:
    payload = {
        "title": event.title,
        "description": event.description,
        "observation_date": None
        if event.observation_date is None
        else event.observation_date.isoformat(),
        "markets": [
            {"id": market.id, "label": market.group_item_title, "question": market.question}
            for market in event.markets
        ],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def deterministic_rule_audit(event: EventDefinition) -> RuleAudit:
    title_match = _TITLE_RE.match(event.title.strip())
    location = title_match.group("location").strip() if title_match else None

    lowered = event.description.lower()
    unit = (
        "C"
        if "degrees celsius" in lowered
        else "F"
        if "degrees fahrenheit" in lowered
        else "unknown"
    )
    precision_match = _PRECISION_RE.search(event.description)
    word_to_int = {"one": 1, "two": 2, "three": 3}
    precision: int | None = None
    if precision_match:
        raw = precision_match.group("word")
        if precision_match.group("whole"):
            precision = 0
        elif raw:
            raw = raw.lower()
            precision = word_to_int.get(raw, int(raw) if raw.isdigit() else 0)
    station_match = _STATION_RE.search(event.description)
    station = station_match.group("station").strip() if station_match else None
    urls = _URL_RE.findall(event.description)
    source_url = urls[0].rstrip(".,") if urls else None

    ambiguity: list[str] = []
    if title_match is None:
        ambiguity.append("unsupported event title template")
    if event.observation_date is None:
        ambiguity.append("observation date is missing")
    if unit == "unknown":
        ambiguity.append("temperature unit is missing")
    if precision is None:
        ambiguity.append("resolution precision is missing")
    if station is None:
        ambiguity.append("station or authority is missing")
    if source_url is None:
        ambiguity.append("resolution source URL is missing")

    try:
        brackets = build_brackets(event)
        bucket_semantics_clear = len(brackets) == len(event.markets)
    except ValueError as error:
        bucket_semantics_clear = False
        ambiguity.append(str(error))

    range_language = "temperature range that contains" in lowered
    if not range_language:
        bucket_semantics_clear = False
        ambiguity.append("rules do not explicitly describe temperature ranges")

    tradeable = not ambiguity and bucket_semantics_clear
    interpretation = RuleInterpretation(
        event_type="daily_max_temperature" if title_match else "unsupported",
        tradeable=tradeable,
        location=location,
        observation_date=event.observation_date,
        unit=unit,
        precision_decimal_places=precision,
        station_or_authority=station,
        resolution_source_url=source_url,
        source_local_date=bool(station and event.observation_date),
        bucket_semantics_clear=bucket_semantics_clear,
        ambiguity_reasons=ambiguity,
        summary=(
            f"Daily maximum temperature for {location} on {event.observation_date}; "
            f"source: {station}."
            if location and event.observation_date and station
            else "Rules could not be deterministically normalized."
        ),
        confidence=0.92 if tradeable else 0.35,
    )
    return RuleAudit(
        rules_hash=rules_hash(event),
        parser="deterministic-v1",
        interpretation=interpretation,
    )


def build_brackets(event: EventDefinition) -> dict[str, Bracket]:
    parsed: list[tuple[str, str, Decimal, str | None, Decimal | None]] = []
    for market in event.markets:
        match = _LABEL_RE.match(market.group_item_title)
        if match is None:
            match = _RANGE_RE.match(market.group_item_title)
        if match is None:
            raise ValueError(f"unsupported bracket label: {market.group_item_title!r}")
        groups = match.groupdict()
        parsed.append(
            (
                market.id,
                market.group_item_title,
                Decimal(groups["value"]),
                None if groups.get("tail") is None else str(groups["tail"]).lower(),
                None if groups.get("high") is None else Decimal(groups["high"]),
            )
        )

    endpoints = sorted(
        {item[2] for item in parsed} | {item[4] for item in parsed if item[4] is not None}
    )
    steps = [
        right - left for left, right in zip(endpoints, endpoints[1:], strict=False) if right > left
    ]
    width = min(steps) if steps else Decimal(1)
    if width <= 0:
        raise ValueError("bracket labels do not have a positive interval")

    brackets: dict[str, Bracket] = {}
    for market_id, label, value, tail, high in parsed:
        if tail in {"or below", "or lower"}:
            lower = None
            upper = float(value + width)
        elif tail in {"or higher", "or above"}:
            lower = float(value)
            upper = None
        elif high is not None:
            lower = float(value)
            upper = float(high + width)
        else:
            lower = float(value)
            upper = float(value + width)
        brackets[market_id] = Bracket(
            market_id=market_id,
            label=label,
            lower=lower,
            upper=upper,
        )
    return brackets
