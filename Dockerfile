FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

RUN pip install --no-cache-dir uv==0.11.24 \
    && useradd --create-home --uid 10001 polybot

COPY pyproject.toml uv.lock README.md .python-version ./
COPY src ./src

RUN uv sync --frozen --no-dev \
    && mkdir -p /app/data \
    && chown -R polybot:polybot /app

USER polybot
ENV PATH="/app/.venv/bin:$PATH"

CMD ["polybot", "run", "--interval", "300", "--max-events", "2"]
