USE CATALOG IDENTIFIER(:catalog);

-- Index inversé (mot -> article) pour la recherche plein texte
CREATE OR REPLACE TABLE gold.search_index_news CLUSTER BY (token) AS
WITH docs AS (
  SELECT c.hk_article, h.record_source AS source,
         lower(concat_ws(' ', c.headline, c.summary)) AS txt
  FROM (
    SELECT * FROM vault.sat_article_content
    QUALIFY row_number() OVER (PARTITION BY hk_article ORDER BY load_ts DESC) = 1
  ) c
  JOIN vault.hub_article h ON h.hk_article = c.hk_article
),
tokens AS (
  SELECT hk_article, source,
         explode(split(regexp_replace(txt, '[^a-z0-9]+', ' '), ' ')) AS token
  FROM docs
)
SELECT token, hk_article, source, count(*) AS tf
FROM tokens
WHERE length(token) >= 3
  AND token NOT IN ('the', 'and', 'for', 'that', 'with', 'this', 'from', 'are', 'was', 'has',
                    'have', 'its', 'but', 'not', 'you', 'will', 'can', 'after', 'over', 'new', 'says', 'said')
GROUP BY token, hk_article, source;
