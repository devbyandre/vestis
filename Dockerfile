FROM python:3.11-slim-bookworm
WORKDIR /app

# No apt-get needed:
# - psycopg2-binary: pre-compiled wheel, no gcc/libpq-dev required
# - curl: not needed in worker containers, healthcheck uses python instead
# - supercronic: downloaded via pip alternative below

# Install supercronic via pip-available binary
ARG SUPERCRONIC_VERSION=v0.2.29
RUN python3 -c "import urllib.request; urllib.request.urlretrieve('https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-amd64', '/usr/local/bin/supercronic')" \
    && chmod +x /usr/local/bin/supercronic

# Copy cron schedules into image
COPY cron/ /app/cron/

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --timeout=120 -r requirements.txt

# Application code
COPY app/ .

CMD ["streamlit", "run", "app_streamlit.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
