# NMP — News-Market Platform (version unifiée)
Corrélation entre actualités financières et dynamiques boursières — Médaillon · Data Vault 2.0 · étude d'événements.

Ce dépôt réunit deux projets : la **plateforme** (Databricks, streaming Kafka, Data Vault, MLOps) et un **mode local léger**
(scripts, cron, Delta local) qui sert à valider la méthode en une semaine. Les deux partagent le même cœur de calcul, testé.

## 1. Deux modes, un seul cœur

```
                         ┌──────────────── CŒUR PARTAGÉ (pandas pur, testé, sans Spark) ────────────────┐
                         │ common/timeutil   séances, UTC, heure d'été, 'pendant/avant/apres/ferme'     │
                         │ transform/dedup   MinHash + URL canonique : 1re publication = signal         │
                         │ transform/adjust  splits / dividendes (prix bruts conservés)                 │
                         │ transform/event_study  rendement anormal, CAR, t, p, test de fuite (leak)    │
                         │ transform/car_batch    événements -> lignes bv_event_car                     │
                         └──────────────────────────────────────────────────────────────────────────────┘
        MODE LOCAL (semaines 1-4)                                   MODE PLATEFORME (semaines 5+)
 scripts/*.py + nmp/local/ : APIs -> archives brutes             Sources batch ─► jobs Python ─────────────┐
 -> Delta local, cron, SLO, poc_event.py                         Sources flux  ─► streamer ─► Redpanda/Event Hubs ─► DLT
                                                                                                           ▼
                BRONZE ─► SILVER ─► RAW VAULT ─► BUSINESS VAULT (bv_article_enriched, bv_event_car) ─► GOLD ─► Power BI
```

**Traçabilité** : `gold.fact_news_impact.hk_article → vault.sat_article_content.bronze_ref → bronze.raw_events.payload` (JSON d'origine) + `url`.

## 2. Démarrage

```bash
cp .env.example .env              # renseigner les clés (jamais commitées)
make install && make lint && make test     # 45 tests, couverture 83 % de la logique testable
# Semaine 1 — mode local : un événement AAPL de bout en bout (article -> prix -> CAR -> Delta)
make poc DAY=2026-10-13
# Plateforme
docker compose up -d --build      # Redpanda + MLflow + flux Alpaca (titres + SPY)
databricks bundle deploy -t dev --var="warehouse_id=<id>"
databricks bundle run batch_ingest -t dev --var="warehouse_id=<id>"   # ... enrich -> car -> health
```

## 3. Décisions anti-biais (à défendre à l'oral)
1. `available_utc = max(published, first_seen)` : jamais la date de mise à jour.
2. Modèle de marché estimé **avant** la fenêtre d'événement ; la minute de publication (k = 0) est **exclue** ; la fuite d'information
   avant la nouvelle est testée (`leak_flag`, CAR sur [-15, -1]).
3. Rendements calculés seulement entre minutes **consécutives** (aucun saut de nuit dans l'estimation).
4. Terciles de la cible calculés sur le **train** ; `TimeSeriesSplit(gap)` + hold-out chronologique.
5. Prix **bruts** stockés, ajustés dans Gold ; volume ajusté par les **splits seulement**.
6. Seule la **1re publication** (`is_first`, hors reprises) alimente le fait d'impact ; CAR calculé pour les nouvelles publiées **en séance**.
7. Tout événement non calculable est journalisé (jamais fatal) ; chaque ligne garde sa version de code (`model_version`).

## 4. Plan de réalisation (12 semaines à 3 personnes) — voir `docs/FUSION.md`
| Phase | Sem. | Livrable | Critère d'acceptation |
|---|---|---|---|
| 1. Fondations + POC | 1 | `make poc` : un CAR sur une vraie nouvelle | tests verts, graphique prix/volume |
| 2. Tranche verticale | 2-3 | AAPL + SPY, Alpaca News, `car_batch` | CAR, t, p-value, `leak_flag` sur 50+ événements |
| 3. Qualité | 4-5 | autres sources, dédup, SLO | rejeu = 0 doublon ; quota → statut `QUOTA` |
| 4. Plateforme | 6-8 | Databricks, Data Vault, `car` dans Gold | un chiffre de Gold remonte au JSON d'origine |
| 5. Intelligence | 8-10 | modèles 1-3 + baselines, MLflow | FinBERT vs Finnhub sur ≥ 500 titres annotés |
| 6. Production | 10-12 | streaming, CI/CD, Power BI, alertes | latence news→Silver < 2 min ; CI verte |

## 5. Limites connues (honnêtes)
- Les jobs Spark/Databricks (`jobs.py`, `pipelines/`, SQL du Vault) **n'ont pas été exécutés** dans cet environnement : la logique
  pandas et la vue d'ajustement (via DuckDB) sont testées, pas le déploiement.
- Le CAR n'est calculé que pour les nouvelles publiées **en séance** ; pour une nouvelle hors séance, la réaction inclut le saut de
  nuit, volontairement exclu de l'estimation (à traiter à part si le sujet l'exige).
- Fenêtre en **barres** de 1 min consécutives ; test t sous hypothèse de variance constante (la volatilité monte après une nouvelle :
  citer le test de Boehmer comme amélioration).
- Les quotas gratuits limitent l'univers : commencer par 10 à 50 titres.
