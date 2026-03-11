FROM python:3.10-slim AS base

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements-pipeline.txt .
RUN pip install --no-cache-dir -r requirements-pipeline.txt

COPY . .

RUN mkdir -p data/raw data/processed data/analytics logs

CMD ["python", "main.py"]
