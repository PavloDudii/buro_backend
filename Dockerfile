FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    PATH="/src/.venv/bin:$PATH"

WORKDIR /src

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY pyproject.toml uv.lock ./
COPY . .

FROM base AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ghostscript \
        qpdf \
        tesseract-ocr \
        tesseract-ocr-eng \
        tesseract-ocr-ukr \
    && rm -rf /var/lib/apt/lists/*
RUN uv sync --frozen --no-dev

CMD ["uv", "run", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]

FROM base AS dev
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ghostscript \
        qpdf \
        tesseract-ocr \
        tesseract-ocr-eng \
        tesseract-ocr-ukr \
    && rm -rf /var/lib/apt/lists/*
RUN uv sync --frozen --all-extras

CMD ["uv", "run", "pytest", "-q"]
