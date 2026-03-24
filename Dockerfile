FROM python:3.11-slim-bookworm
WORKDIR /app

# Install only curl (needed for healthcheck) — no gcc/libpq-dev needed
# because we use psycopg2-binary which has no C build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Install supercronic
ARG SUPERCRONIC_VERSION=v0.2.29
ARG SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-amd64
RUN curl -fsSL "$SUPERCRONIC_URL" -o /usr/local/bin/supercronic \
    && chmod +x /usr/local/bin/supercronic

# Copy cron schedules into image
COPY cron/ /app/cron/

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --timeout=120 -r requirements.txt

# Application code
COPY app/ .

CMD ["streamlit", "run", "app_streamlit.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
