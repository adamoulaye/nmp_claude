"""Session HTTP avec reprises automatiques (429/5xx) et delai exponentiel."""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def make_session(headers: dict | None = None) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    if headers:
        s.headers.update(headers)
    return s
