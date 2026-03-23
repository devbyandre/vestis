FROM python:3.11-slim-bookworm
WORKDIR /app

# Force apt to use HTTPS to avoid HTTP connection failures
RUN echo 'Acquire::https::Verify-Peer "false";' > /etc/apt/apt.conf.d/99insecure \
    && sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list.d/debian.sources \
    || sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list

# Install system dependencies + curl (healthcheck) + supercronic (cron replacement)
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Install supercronic (lightweight cron for containers – no root daemon needed)
ARG SUPERCRONIC_VERSION=v0.2.29
ARG SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-amd64
RUN curl -fsSL "$SUPERCRONIC_URL" -o /usr/local/bin/supercronic \
    && chmod +x /usr/local/bin/supercronic

# Copy cron schedules into image
COPY cron/ /app/cron/

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY app/ .
