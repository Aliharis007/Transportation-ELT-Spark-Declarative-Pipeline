CREATE OR REFRESH MATERIALIZED VIEW transportation.gold.fact_trips_surat
AS (
SELECT *
FROM transportation.gold.facts_trips
WHERE city_id = 'GJ01'
);