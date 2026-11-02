CREATE OR REFRESH MATERIALIZED VIEW transportation.gold.fact_trips_lucknow
AS (
SELECT *
FROM transportation.gold.facts_trips
WHERE city_id = 'UP01'
);