import hashlib
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

from polybot.config import Settings
from polybot.models import (
    BookLevel,
    Bracket,
    DecisionAction,
    EventDefinition,
    MarketDecision,
    MarketDefinition,
    MarketSnapshot,
    OutcomeSide,
)
from polybot.scanner import Scanner
from polybot.storage import Storage
from polybot.weathernext_paper import (
    WEATHERNEXT_PAPER_STRATEGY_VERSION,
    paper_idempotency_key,
)


def _decision() -> MarketDecision:
    return MarketDecision(
        action=DecisionAction.PAPER_BUY,
        reason_codes=[],
        event_id="event-wn",
        market_id="market-wn",
        asset_id="asset-wn",
        token_id="token-wn",
        condition_id="condition-wn",
        probability=Decimal("0.80"),
        shares=Decimal("5"),
        executable_price=Decimal("0.20"),
        probability_edge=Decimal("0.60"),
        notional_usd=Decimal("1"),
        fee_usd=Decimal("0.01"),
        execution_buffer_usd=Decimal("0.01"),
        max_loss_usd=Decimal("1.02"),
        expected_profit_usd=Decimal("0.50"),
        strategy_version=WEATHERNEXT_PAPER_STRATEGY_VERSION,
        execution_model="WEATHERNEXT_CROSSING_LIMIT_SHARES",
        book_hash="book-wn",
        created_at=datetime.now(UTC),
    )


def test_weathernext_ledger_isolated_from_v1_portfolio(tmp_path) -> None:
    storage = Storage(tmp_path / "paper.sqlite3")
    run_id = storage.start_scan(query="weather", mode="paper")
    decision = _decision()

    storage.record_weathernext_paper_decision(run_id, decision)
    storage.open_weathernext_paper_order(
        decision,
        run_id=run_id,
        idempotency_key=paper_idempotency_key(decision),
        max_event_risk=Decimal("2"),
        max_total_risk=Decimal("6"),
    )

    weather_summary = storage.weathernext_paper_summary()
    assert weather_summary["strategy_version"] == WEATHERNEXT_PAPER_STRATEGY_VERSION
    assert weather_summary["open_orders"] == 1
    assert weather_summary["open_exposure_usd"] == Decimal("1.02")
    assert weather_summary["decisions_by_action"] == {"PAPER_BUY": 1}

    # v1 risk/P&L queries must not see the isolated research position.
    v1_summary = storage.portfolio_summary()
    assert v1_summary["open_orders"] == 0
    assert v1_summary["open_exposure_usd"] == Decimal(0)
    assert v1_summary["realized_pnl_usd"] == Decimal(0)


def test_weathernext_idempotency_key_is_strategy_scoped() -> None:
    decision = _decision()
    first = paper_idempotency_key(decision)
    legacy_payload = ":".join(
        (
            decision.event_id,
            decision.market_id,
            decision.asset_id,
            decision.book_hash,
            str(decision.probability),
            decision.strategy_version,
            decision.execution_model,
        )
    )
    assert first != hashlib.sha256(legacy_payload.encode()).hexdigest()


def test_scanner_records_missing_snapshot_as_explicit_filter(tmp_path) -> None:
    storage = Storage(tmp_path / "missing.sqlite3")
    run_id = storage.start_scan(query="weather", mode="paper")
    now = datetime.now(UTC)
    market = MarketSnapshot(
        event_id="event-wn",
        event_slug="event-wn",
        event_title="WeatherNext test",
        market_id="market-wn",
        market_slug="market-wn",
        market_question="Will it be 20C?",
        outcome_label="20C",
        asset_id="asset-wn",
        token_id="token-wn",
        condition_id="condition-wn",
        outcome=OutcomeSide.YES,
        end_date=None,
        accepting_orders=True,
        book_timestamp=now,
        book_hash="book-wn",
        bids=[],
        asks=[BookLevel(price=Decimal("0.20"), size=Decimal("5"))],
        min_order_size=Decimal("5"),
        tick_size=Decimal("0.01"),
        fee_rate=Decimal(0),
        fee_exponent=Decimal(0),
        fee_taker_only=False,
    )
    event = EventDefinition(
        id="event-wn",
        slug="event-wn",
        title="WeatherNext test",
        description="",
        observation_date=None,
        markets=[
            MarketDefinition(
                id="market-wn",
                slug="market-wn",
                question="Will it be 20C?",
                group_item_title="20C",
                asset_id="asset-wn",
                condition_id="condition-wn",
                end_date=None,
                accepting_orders=True,
                fee_rate=Decimal(0),
                fee_exponent=Decimal(0),
                fee_taker_only=False,
            )
        ],
    )
    scanner = Scanner.__new__(Scanner)
    scanner.settings = Settings(
        weathernext_paper_enabled=True,
        min_probability_edge=Decimal("0.08"),
        min_expected_profit_usd=Decimal("0.25"),
    )
    scanner.storage = storage
    opened, recorded, errors = scanner._run_weathernext_paper_strategy(  # noqa: SLF001
        run_id=run_id,
        event=event,
        comparison=None,
        brackets={"market-wn": Bracket(
            market_id="market-wn", label="20C", lower=20, upper=21
        )},
        probabilities={},
        market_snapshots={"market-wn": market},
        event_blockers=[],
        event_warnings=[],
        paper=True,
        allow_open=True,
    )
    assert opened == 0
    assert recorded == 1
    assert errors == []
    decision = storage.weathernext_paper_summary()["recent_decisions"][0]
    assert "SNAPSHOT_UNAVAILABLE" in decision["reason_codes"]
    assert decision["action"] == "SKIP"


def test_scanner_opens_only_isolated_weathernext_position_when_snapshot_exists(tmp_path) -> None:
    storage = Storage(tmp_path / "available.sqlite3")
    run_id = storage.start_scan(query="weather", mode="paper")
    now = datetime.now(UTC)
    market = MarketSnapshot(
        event_id="event-wn",
        event_slug="event-wn",
        event_title="WeatherNext test",
        market_id="market-wn",
        market_slug="market-wn",
        market_question="Will it be 20C?",
        outcome_label="20C",
        asset_id="asset-wn",
        token_id="token-wn",
        condition_id="condition-wn",
        outcome=OutcomeSide.YES,
        end_date=None,
        accepting_orders=True,
        book_timestamp=now,
        book_hash="book-wn",
        bids=[],
        asks=[BookLevel(price=Decimal("0.20"), size=Decimal("5"))],
        min_order_size=Decimal("5"),
        tick_size=Decimal("0.01"),
        fee_rate=Decimal(0),
        fee_exponent=Decimal(0),
        fee_taker_only=False,
    )
    event = EventDefinition(
        id="event-wn",
        slug="event-wn",
        title="WeatherNext test",
        description="",
        observation_date=None,
        markets=[
            MarketDefinition(
                id="market-wn",
                slug="market-wn",
                question="Will it be 20C?",
                group_item_title="20C",
                asset_id="asset-wn",
                condition_id="condition-wn",
                end_date=None,
                accepting_orders=True,
                fee_rate=Decimal(0),
                fee_exponent=Decimal(0),
                fee_taker_only=False,
            )
        ],
    )
    scanner = Scanner.__new__(Scanner)
    scanner.settings = Settings(weathernext_paper_enabled=True)
    scanner.storage = storage
    comparison = SimpleNamespace(
        init_time_utc=now,
        source_uri="gs://weathernext3_spatial/test/predictions.zarr",
        scenario_max_c=[20.0] * 64,
    )
    opened, recorded, errors = scanner._run_weathernext_paper_strategy(  # noqa: SLF001
        run_id=run_id,
        event=event,
        comparison=comparison,
        brackets={"market-wn": Bracket(
            market_id="market-wn", label="20C", lower=20, upper=21
        )},
        probabilities={"market-wn": Decimal("0.80")},
        market_snapshots={"market-wn": market},
        event_blockers=[],
        event_warnings=[],
        paper=True,
        allow_open=True,
    )
    assert errors == []
    assert opened == 1
    assert recorded == 1
    assert storage.weathernext_paper_summary()["open_orders"] == 1
    assert storage.portfolio_summary()["open_orders"] == 0
