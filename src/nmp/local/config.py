"""Configuration du mode local (scripts + Delta local). Les secrets viennent de .env (jamais dans Git)."""

import os
from pathlib import Path

try:  # python-dotenv est optionnel (extra [local]) : sans lui, on lit simplement l'environnement
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass

from nmp.transform.car_batch import BENCHMARK, CODE_VERSION  # noqa: E402,F401  (une seule source de vérité)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
ALPACA_KEY_ID = os.getenv("ALPACA_KEY_ID") or os.getenv("ALPACA_API_KEY_ID", "")
ALPACA_SECRET = os.getenv("ALPACA_SECRET") or os.getenv("ALPACA_API_SECRET_KEY", "")
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "")  # ex. : "Prenom Nom prenom@univ.ca"

PILOT_TICKERS = ["AAPL", "TSLA", "JPM", "XOM", "PFE"]  # tech, conso., finance, énergie, santé
