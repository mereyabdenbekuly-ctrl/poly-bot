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


@pytest.mark.parametrize("requested", [0, 21])
def test_event_limit_stays_bounded(requested: int) -> None:
    with pytest.raises(ValueError, match="between 1 and 20"):
        _resolve_event_limit(Settings(), requested=requested, paper=True)
