from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import SecretStr

import polybot.astra as astra_module
from polybot.astra import AstraRuleAuditor
from polybot.config import Settings


def test_astra_uses_dedicated_long_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class FakeResponses:
        def parse(self, **kwargs: Any) -> Any:
            raise TimeoutError("test timeout")

    def fake_openai(**kwargs: Any) -> Any:
        captured.update(kwargs)
        return SimpleNamespace(responses=FakeResponses())

    settings = Settings().model_copy(
        update={
            "openai_api_key": SecretStr("test-key"),
            "openai_base_url": "https://api.openai.com/v1",
            "openai_fallback_api_key": None,
            "openai_fallback_base_url": None,
            "http_timeout_seconds": 20.0,
            "astra_timeout_seconds": 120.0,
        }
    )
    monkeypatch.setattr(astra_module, "OpenAI", fake_openai)
    auditor = AstraRuleAuditor(settings=settings, storage=SimpleNamespace())  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="all Astra endpoints failed"):
        auditor._request_with_failover({})  # noqa: SLF001

    assert captured["timeout"] == 120.0
    assert captured["max_retries"] == 0
