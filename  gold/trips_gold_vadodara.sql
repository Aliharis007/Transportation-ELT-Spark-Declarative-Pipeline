CREATE OR REFRESH MATERIALIZED VIEW transportation.gold.facts_trips_vadodara
AS (
    SELECT *
    FROM transportation.gold.facts_trips
    WHERE city_id='GJ02'
);