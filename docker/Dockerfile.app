# Stage 1 — build the React frontend
FROM node:20-alpine AS frontend
WORKDIR /build
COPY dashboard/frontend/package.json dashboard/frontend/package-lock.json* ./
RUN npm install
COPY dashboard/frontend/ ./
RUN npm run build

# Stage 2 — Python API serving the built static files
FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements-dashboard.txt .
RUN pip install --no-cache-dir -r requirements-dashboard.txt

COPY . .
COPY --from=frontend /build/dist /app/dashboard/frontend/dist

EXPOSE 8501
CMD ["uvicorn", "dashboard.api.main:app", "--host", "0.0.0.0", "--port", "8501"]
