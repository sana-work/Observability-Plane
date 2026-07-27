FROM python:3.11-slim AS base

WORKDIR /app
COPY pyproject.toml README.md ./
COPY obs_enrichment ./obs_enrichment
RUN pip install --no-cache-dir .

# Production image with GLiNER baked in (uncomment for the prod build):
# RUN pip install --no-cache-dir ".[gliner]" \
#  && python -c "from gliner import GLiNER; GLiNER.from_pretrained('urchade/gliner_multi_pii-v1')"

USER 1000
EXPOSE 9108
ENTRYPOINT ["obs-enrichment"]
