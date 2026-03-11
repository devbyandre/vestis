#!/usr/bin/env python3
"""
webhook_server.py  —  Vestis auto-deploy webhook

Listens for GitHub push events on the 'main' branch and runs:
  git pull && docker compose up -d --build

Setup:
  1. Run this on your homeserver (it starts on port 9000)
  2. Add the webhook in GitHub:
       Repo → Settings → Webhooks → Add webhook
       Payload URL:  http://YOUR_HOMESERVER_IP:9000/deploy
       Content type: application/json
       Secret:       (same value as WEBHOOK_SECRET below)
       Events:       Just the push event
  3. Set WEBHOOK_SECRET in your .env (or export it in your shell)

Run with Docker Compose by adding the 'webhook' service (see docker-compose.yml comments),
or standalone:
  WEBHOOK_SECRET=your_secret python3 webhook_server.py
"""

import hashlib
import hmac
import json
import logging
import os
import subprocess
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "").encode()
DEPLOY_BRANCH  = os.environ.get("DEPLOY_BRANCH", "refs/heads/main")
PROJECT_DIR    = os.environ.get("PROJECT_DIR", "/opt/vestis")
PORT           = int(os.environ.get("WEBHOOK_PORT", "9000"))


def _verify_signature(body: bytes, sig_header: str) -> bool:
    """Verify the GitHub HMAC-SHA256 signature."""
    if not WEBHOOK_SECRET:
        logging.warning("WEBHOOK_SECRET not set — skipping signature verification")
        return True
    if not sig_header or not sig_header.startswith("sha256="):
        return False
    expected = "sha256=" + hmac.new(WEBHOOK_SECRET, body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig_header)


def _deploy():
    """Pull latest code and restart Docker Compose services."""
    logging.info("🚀 Deploy triggered — pulling and restarting...")
    try:
        subprocess.run(["git", "pull", "--ff-only"], cwd=PROJECT_DIR, check=True)
        subprocess.run(
            ["docker", "compose", "up", "-d", "--build"],
            cwd=PROJECT_DIR,
            check=True
        )
        logging.info("✅ Deploy complete")
        return True
    except subprocess.CalledProcessError as e:
        logging.error("❌ Deploy failed: %s", e)
        return False


class WebhookHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        logging.info(fmt, *args)

    def do_POST(self):
        if self.path != "/deploy":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)

        # Verify signature
        sig = self.headers.get("X-Hub-Signature-256", "")
        if not _verify_signature(body, sig):
            logging.warning("Invalid signature — ignoring request")
            self.send_response(401)
            self.end_headers()
            return

        # Parse event
        event = self.headers.get("X-GitHub-Event", "")
        if event != "push":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ignored")
            return

        payload = json.loads(body)
        ref = payload.get("ref", "")

        if ref != DEPLOY_BRANCH:
            logging.info("Push to %s — not deploying (watching %s)", ref, DEPLOY_BRANCH)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ignored")
            return

        # Deploy
        ok = _deploy()
        self.send_response(200 if ok else 500)
        self.end_headers()
        self.wfile.write(b"deployed" if ok else b"deploy_failed")


if __name__ == "__main__":
    if not WEBHOOK_SECRET:
        logging.error("WEBHOOK_SECRET environment variable is not set. Exiting.")
        sys.exit(1)

    logging.info("Vestis webhook server listening on port %d", PORT)
    logging.info("Watching branch: %s", DEPLOY_BRANCH)
    logging.info("Project dir:     %s", PROJECT_DIR)

    server = HTTPServer(("0.0.0.0", PORT), WebhookHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logging.info("Shutting down")
