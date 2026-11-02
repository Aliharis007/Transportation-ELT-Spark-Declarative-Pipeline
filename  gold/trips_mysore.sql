CREATE OR REFRESH MATERIALIZED VIEW transportation.gold.fact_trips_mysore
AS (
SELECT *
FROM transportation.gold.facts_trips
WHERE city_id = 'KA01'
);