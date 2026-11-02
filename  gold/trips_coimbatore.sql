CREATE OR REFRESH MATERIALIZED VIEW transportation.gold.fact_trips_coimbatore
AS (
SELECT *
FROM transportation.gold.facts_trips
WHERE city_id = 'TN01'
);