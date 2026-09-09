.PHONY: sync check test doctor scan run dashboard

sync:
	uv sync --all-groups

check:
	uv run ruff check .
	uv run ruff format --check .
	uv run pyright
	uv run pytest

test:
	uv run pytest --cov=polybot

doctor:
	uv run polybot doctor

scan:
	uv run polybot scan --max-events 2

run:
	uv run polybot run --interval 300 --max-events 2

dashboard:
	uv run polybot dashboard --host 127.0.0.1 --port 8787
