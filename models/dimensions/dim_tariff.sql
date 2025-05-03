{{ config(materialized="table") }}

select tariff_id, tariff_name, cost_per_hour, currency, duration_minutes
from {{ ref("stg_station_tariffs") }}