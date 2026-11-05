# Fusion des deux dépôts : ce qui vient d'où, ce qui a été corrigé

## Provenance
| Brique | Source retenue | Pourquoi |
|---|---|---|
| Plateforme (Databricks, Vault, DLT, Docker, MLflow, CI/CD) | `nmp` | seul à couvrir Kafka/streaming, Data Vault complet, MLOps |
| Étude d'événements (`event_study.py`) | `news-market-project` | exclut la minute k=0, test de fuite, rendements sur minutes consécutives, 2 fenêtres, volume relatif |
| Lot CAR (`car_batch.py`) | `news-market-project`, généralisé | rend `run_batch` utilisable par Spark (`id_col`, `keep`) + sortie `bv_event_car` |
| Dédup | `nmp` (MinHash) + règle URL de l'autre dépôt | passage à l'échelle + reprises de même URL |
| Mode local (`nmp/local/`, `scripts/`) | `news-market-project` | POC en une semaine, sans Azure |
| Catégories | règles de l'autre dépôt, **vocabulaire de nmp** | un seul vocabulaire jusqu'à Gold |
| Sessions | `nmp` (vectorisé) + `classify_session` | 8 cas supplémentaires (fin d'heure d'été, fermeture anticipée, Action de grâce) |

## Défauts corrigés (un test chacun : `tests/test_regressions.py`, `tests/test_sql_adjustment.py`)
1. Articles **sans texte** regroupés ensemble (le 1er « signal », les autres « bruit ») → jamais comparés.
2. Nouvelle publiée pendant la **dernière séance** du calendrier : date de réaction vide → conservée.
3. **Volume** annoncé « ajusté » mais laissé brut ; en SQL il était divisé par le facteur *dividendes inclus* → splits seulement.
4. Vue SQL `v_adjustment_factor` : **deux lignes les jours d'événement** (barres dupliquées dans Gold) → une ligne ; égalité avec la référence pandas vérifiée (DuckDB).
5. **Le CAR n'était jamais calculé** dans le pipeline (`bv_event_car` jamais alimentée) → job `car` + fenêtre `window_end_min = 60` dans Gold.
6. **SPY absent** des barres suivies (le modèle de marché en a besoin) → ajouté automatiquement (`Settings.bar_tickers`).
7. Titre vide / NaN dans la dédup de l'autre dépôt → `_shingles` tolère les valeurs nulles.
8. Le CI de `nmp` ne passait pas son propre lint/format (68 erreurs, 23 fichiers) → ruff + mypy + couverture au vert.

## Répartition à 3 (voir aussi le plan)
- **P1 Data Engineering** : `ingestion/`, `streaming/`, `local/sources`, SQL Vault, job `car`.
- **P2 Data Science** : `event_study`, `dedup`, `ml/` (modèles 1-2), annotation.
- **P3 BI / DevOps** : CI/CD, `databricks.yml`, `ml/anomaly` + Granger, Power BI, alertes.
