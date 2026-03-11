# article_utils.py
"""
Utilities to fetch article HTML and extract main text using BeautifulSoup heuristics.
This is lightweight and won't be as robust as newspaper3k, but avoids heavy deps.
"""

import requests
from bs4 import BeautifulSoup
import logging
from typing import Optional

logging.getLogger("urllib3").setLevel(logging.WARNING)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"
}

def fetch_article_text(url: str, timeout: int = 8) -> Optional[str]:
    if not url or not url.startswith(("http://","https://")):
        return None
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        for s in soup(["script","style","noscript","header","footer","aside","svg","figure","iframe"]):
            s.decompose()
        paragraphs = [p.get_text(separator=" ", strip=True) for p in soup.find_all("p") if p.get_text(strip=True)]
        if paragraphs:
            joined = "\n\n".join(paragraphs)
            return joined[:20000]
        body = soup.body.get_text(separator=" ", strip=True) if soup.body else None
        return body.strip() if body else None
    except Exception:
        return None
