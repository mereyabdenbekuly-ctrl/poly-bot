import pytest

from polybot.cli import _resolve_event_limit, build_parser
from polybot.config import Settings


def test_paper_mode_uses_broader_default_event_limit() -> None:
    settings = Settings(max_events=2, paper_max_events=8)

    assert _resolve_event_limit(settings, requested=None, paper=False) == 2
    assert _resolve_event_limit(settings, requested=None, paper=True) == 8


def test_explicit_event_limit_overrides_both_modes() -> None:
    settings = Settings(max_events=2, paper_max_events=8)

    assert _resolve_event_limit(settings, requested=5, paper=False) == 5
    assert _resolve_event_limit(settings, requested=5, paper=True) == 5


def test_diagnostics_command_is_available_without_runtime_side_effects() -> None:
    args = build_parser().parse_args(["diagnostics", "--json"])

    assert args.command == "diagnostics"
    assert args.as_json is True


def test_live_provision_defaults_to_supported_direct_signer_without_database_arg() -> None:
    args = build_parser().parse_args(["live-pilot", "provision"])

    assert args.command == "live-pilot"
    assert args.live_command == "provision"
    assert args.auth_mode == "direct_signer"
    assert args.wallet is None
    assert not hasattr(args, "database")


def test_live_auto_once_is_explicitly_bounded_and_opt_in() -> None:
    args = build_parser().parse_args(["live-pilot", "auto-once", "--json"])

    assert args.command == "live-pilot"
    assert args.live_command == "auto-once"
    assert args.timeout_seconds == 0.0
    assert args.poll_seconds == 5.0
    assert args.after_decision_id is None
    assert args.authorization.endswith("auto-once-authorization.json")
    assert args.as_json is True


def test_live_v2_status_and_run_are_explicitly_bounded_and_opt_in() -> None:
    status = build_parser().parse_args(["live-v2", "status", "--json"])
    assert status.command == "live-v2"
    assert status.live_v2_command == "status"
    assert status.as_json is True

    run = build_parser().parse_args(["live-v2", "run", "--json"])
    assert run.command == "live-v2"
    assert run.live_v2_command == "run"
    assert run.poll_seconds == 5.0
    assert run.timeout_seconds == 0.0
    assert run.credentials.endswith("credentials.json")
    assert run.authorization.endswith("live-v2-authorization.json")
    assert run.as_json is True


def test_weathernext_first_trial_and_existing_read_flags_are_explicit() -> None:
    planned = build_parser().parse_args(["weathernext", "first-full-trial-plan", "--json"])
    assert planned.weathernext_command == "first-full-trial-plan"
    assert planned.as_json is True

    read = build_parser().parse_args(
        [
            "weathernext",
            "autonomous-refresh",
            "--read-approved",
            "--require-strictly-future-targets",
        ]
    )
    assert read.read_approved is True
    assert read.require_strictly_future_targets is True


@pytest.mark.parametrize("requested", [0, 21])
def test_event_limit_stays_bounded(requested: int) -> None:
    with pytest.raises(ValueError, match="between 1 and 20"):
        _resolve_event_limit(Settings(), requested=requested, paper=True)
