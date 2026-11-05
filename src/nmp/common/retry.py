"""Reprise automatique : backoff exponentiel, respect de Retry-After, détection de quota."""

from __future__ import annotations

import logging

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

log = logging.getLogger(__name__)


class QuotaExceeded(RuntimeError):
    """Quota d'API épuisé : arrêt propre ; le point de contrôle permet la reprise."""


class SchemaDrift(RuntimeError):
    """Structure de la réponse modifiée de façon incompatible."""


def _retryable(exc: BaseException) -> bool:
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code in (408, 425, 429, 500, 502, 503, 504)
    return False


http_retry = retry(
    retry=retry_if_exception(_retryable),
    wait=wait_exponential_jitter(initial=1, max=60),
    stop=stop_after_attempt(6),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)


@http_retry
def get_json(session: requests.Session, url: str, *, params=None, headers=None, timeout: int = 30):
    r = session.get(url, params=params, headers=headers, timeout=timeout)
    if r.status_code == 429 and r.headers.get("X-RateLimit-Remaining") == "0" and "Retry-After" not in r.headers:
        raise QuotaExceeded(f"Quota épuisé pour {url}")
    r.raise_for_status()
    return r.json()
