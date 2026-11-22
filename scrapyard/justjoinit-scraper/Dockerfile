FROM python:3.12-slim

RUN useradd -u 10001 -m app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

USER app

EXPOSE 8801
CMD ["sh", "-c", "exec ${OTEL_EXPORTER_OTLP_ENDPOINT:+opentelemetry-instrument} uvicorn main:app --host 0.0.0.0 --port 8801"]
