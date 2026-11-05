# Modèle sémantique et rapports Power BI

**Connexion** : connecteur *Azure Databricks* → SQL Warehouse (Serverless), schéma `gold`.
Mode **DirectQuery** pour `fact_realtime_feed`, `fact_bar_1m`, `fact_platform_health` (avec *Auto page refresh* 30 s) ; mode **Import** (rafraîchissement horaire) pour `dim_*`, `fact_model_metrics`, `fact_drift`.

## Tables et relations (schéma en étoile)
| Table | Clé | Relation |
|---|---|---|
| `dim_security` | hk_security | 1—* `fact_news_impact`, `fact_bar_1m`, `fact_sector_heatmap` |
| `dim_category` | category | 1—* `fact_news_impact` |
| `dim_date` | date | 1—* `fact_news_impact[session_date]` |
| `fact_news_impact` | hk_article + hk_security | grain : 1re publication × ticker |

## Mesures DAX
```DAX
CAR moyen (%)        = AVERAGE ( fact_news_impact[car] ) * 100
Part significative   = DIVIDE ( CALCULATE ( COUNTROWS ( fact_news_impact ), fact_news_impact[significant] = TRUE ), COUNTROWS ( fact_news_impact ) )
Sentiment moyen      = AVERAGE ( fact_news_impact[sentiment_score] )
Dernière donnée UTC  = MAX ( fact_bar_1m[bar_ts] )
Retard (min)         = DATEDIFF ( [Dernière donnée UTC], UTCNOW (), MINUTE )
Taux de rejet        = DIVIDE ( SUM ( fact_platform_health[rejected] ), SUM ( fact_platform_health[received] ) )
Fraîcheur OK         = IF ( [Retard (min)] <= 5, "OK", "ALERTE" )
F1 macro champion    = CALCULATE ( MAX ( fact_model_metrics[value] ), fact_model_metrics[alias] = "champion", fact_model_metrics[metric] = "holdout_macro_f1" )
```

## Rapports
1. **Fil en temps réel** — chandeliers (visuel *Candlestick*, `fact_bar_1m`) + marqueurs de nouvelles ; infobulle de page : `headline`, `sentiment_label`, lien `url` ; carte « réaction prédite » (`predicted_magnitude`) ; carte `Dernière donnée UTC` / `Fraîcheur OK` ; alertes : *Data alert* Power BI sur `Retard (min)` > 5.
2. **Analyse d'impact** — histogramme CAR moyen par `category` ; nuage `sentiment_score` × `car` (couleur = `significant`) ; matrice thermique `sector` × `day` sur `fact_sector_heatmap`.
3. **Performance des modèles** — table par version (alias champion / challenger / baseline), importance des variables (table `fact_model_metrics` métrique `feature_importance:*`), courbe PSI par variable avec seuils 0,1 / 0,25.
4. **État de la plateforme** — fraîcheur par source (`ops.freshness`), volumes reçus/retenus/rejetés par heure, quotas (`ops.api_quota`), incidents (`ops.incidents`).

Documentation du modèle : activer les descriptions de colonnes/mesures dans Power BI et exporter le dictionnaire (Tabular Editor) dans le dépôt.

## Alertes d'exploitation (Databricks SQL Alerts → Teams/e-mail)
- `SELECT max(lag_minutes) FROM ops.freshness WHERE checked_utc > now() - INTERVAL 15 MINUTES` > 10
- `SELECT count(*) FROM ops.run_log WHERE status <> 'SUCCESS' AND started_utc > now() - INTERVAL 1 HOUR` > 0
- `SELECT max(psi) FROM gold.fact_drift WHERE checked_utc > now() - INTERVAL 1 DAY` > 0.25
