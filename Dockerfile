FROM python:3.11-slim-bookworm
WORKDIR /app

# Install supercronic directly via pip-available urllib (no apt needed)
RUN python3 -c "\
import urllib.request, os; \
url='https://github.com/aptible/supercronic/releases/download/v0.2.29/supercronic-linux-amd64'; \
urllib.request.urlretrieve(url, '/usr/local/bin/supercronic'); \
os.chmod('/usr/local/bin/supercronic', 0o755)"

# Copy cron schedules into image
COPY cron/ /app/cron/

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --timeout=120 --verbose -r requirements.txt 2>&1 | tail -50

# Application code
COPY app/ .

CMD ["streamlit", "run", "app_streamlit.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
