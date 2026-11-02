CREATE OR REFRESH MATERIALIZED VIEW transportation.gold.fact_trips_kochi
AS (
SELECT *
FROM transportation.gold.facts_trips
WHERE city_id = 'KL01'
);