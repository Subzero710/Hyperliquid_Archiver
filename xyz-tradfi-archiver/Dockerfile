FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1
ENV PYTHONPATH=/app

WORKDIR /app

RUN python -m pip install \
  --root-user-action=ignore \
  "boto3>=1.34.0" \
  "pyarrow>=16.0.0" \
  "requests>=2.32.0" \
  "websocket-client>=1.8.0" \
  "zstandard>=0.22.0"

COPY app ./app

ENTRYPOINT ["python", "-m", "app.cli"]