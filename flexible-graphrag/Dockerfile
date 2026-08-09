# Base image: uv + Python 3.14 on Debian trixie-slim — the SAME base as Dockerfile.langflow and as
# Langflow's own image, so both flexible-graphrag images share one modern base with uv pre-installed.
# (Was python:3.12 through v0.6.x; Python 3.14 as of v0.7.0.)
#
# trixie-slim ships no compiler, so build tools are installed below — flexible-graphrag has C-extension
# deps (pystemmer, etc.) that must compile. Alternative bases if you need them:
#   - Full Debian with build tools baked in:  FROM python:3.14
#   - RedHat UBI (OpenRAG's choice):          FROM registry.access.redhat.com/ubi10/python-314-minimal
#   - Multi-stage (builder + a python:3.14-slim runtime that copies the venv) — a size optimization
#     for later; drops the build tools from the final image, as Langflow's own Dockerfile does.
FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

WORKDIR /app

# build-essential/gcc for the C-extension deps (trixie-slim has no compiler); curl for the healthcheck.
# uv is already in this image, so there is no `pip install uv` step.
RUN apt-get update && apt-get install --no-install-recommends -y \
    build-essential gcc curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy dependency files first for better layer caching.
# extras-overrides.txt must be present before install so --override can reference it.
# README.md + the force-included deploy files (Dockerfile, env-sample.txt) must be present before the
# editable install: hatchling reads `readme` and validates `[tool.hatch.build.targets.wheel.force-include]`
# during `build_editable`, so they have to exist at /app even though `COPY . .` re-copies them below.
COPY pyproject.toml uv.toml extras-overrides.txt README.md Dockerfile env-sample.txt ./

# Optional extras to install alongside base dependencies.
# extras-overrides.txt suppresses advisory-only transitive downgrades from langchain-extras
# (fsspec<2025, pgvector<0.4, requests/urllib3/rich pins from hugegraph-python, etc.)
# so langchain-extras is safe to include by default.
#
# Note: rapidocr is bundled with docling-slim[standard] (a base dependency), so basic
# OCR for scanned PDFs works out of the box without any docling-ocr-* extra.
#
# Override at build time:
#   docker build --build-arg EXTRAS="langchain,langchain-extras,observability" .   (add OTel tracing)
#   docker build --build-arg EXTRAS="" .   (base dependencies only)
#
# Excluded from default:
#   rdf                            — REDUNDANT: rdflib/pyoxigraph/requests are already base deps, so
#                                    RDF stores work without it (the extra adds nothing).
#   observability                  — optional OpenTelemetry/OpenInference tracing (add if you want it).
#   docling-ocr-easyocr/tesserocr  — alternative OCR engines needing extra system libs
#   docling-ocr-ocrmac              — macOS only
#   surrealdb-extras, age-extras, spanner-extras — niche integrations
ARG EXTRAS="langchain,langchain-extras"

RUN if [ -n "$EXTRAS" ]; then \
        uv pip install --override extras-overrides.txt --system -e ".[$EXTRAS]"; \
    else \
        uv pip install --system -e .; \
    fi

# Copy application code
COPY . .

# The editable install leaves a flexible_graphrag.egg-info in /app; with /app on PYTHONPATH it becomes
# a duplicate 'flexible-graphrag' importlib.metadata distribution alongside the site-packages dist-info.
# Harmless for the backend, but removed for consistency with Dockerfile.langflow (where it triggers a
# Langflow `duplicate-distribution` error).
RUN rm -rf /app/*.egg-info

# Put /app on the import path. The editable install above ran before this COPY, so setuptools'
# packages.find never saw the source packages (ingest/, process/, rdf/, …) to register them. uvicorn
# adds the cwd when importing main:app, but PYTHONPATH makes the whole source tree importable
# unconditionally — and keeps this image consistent with Dockerfile.langflow.
ENV PYTHONPATH=/app

# Create necessary directories
RUN mkdir -p uploads sample_docs

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the application with standard asyncio loop (uvloop conflicts with nest_asyncio)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--loop", "asyncio"]
