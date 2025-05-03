-- models/staging/stg_station_tariffs.sql
{{ config(materialized="view") }}

with
    src as (
        select ingest_datetime, json_extract(data_json, '$.tariffs') as tariffs_json
        from {{ source("gbfs", "raw_station_information") }}
    ),

    exploded as (
        select ingest_datetime, t as tariff_json
        from src, unnest(cast(json_extract_array(tariffs_json) as array<json>)) as t
    ),

    parsed as (
        select
            ingest_datetime,
            json_extract_scalar(tariff_json, '$.tariff_id') as tariff_id,
            json_extract_scalar(tariff_json, '$.name') as tariff_name,
            cast(
                json_extract_scalar(tariff_json, '$.cost_per_hour') as float64
            ) as cost_per_hour,
            json_extract_scalar(tariff_json, '$.currency') as currency,
            cast(
                json_extract_scalar(tariff_json, '$.duration_minutes') as int64
            ) as duration_minutes
        from exploded
    )

select *
from parsed