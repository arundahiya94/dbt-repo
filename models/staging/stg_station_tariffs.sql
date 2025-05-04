-- models/staging/stg_station_tariffs.sql
{{ config(materialized="view") }}

with
    raw as (
        select ingest_datetime, json_extract(data_json, '$.tariffs') as tariffs_json
        from {{ source("gbfs", "raw_station_information") }}
    ),

    exploded as (
        select ingest_datetime, tariff_json
        from raw, unnest(json_extract_array(tariffs_json)) as tariff_json  -- tariff_json is STRING
    ),

    parsed as (
        select
            ingest_datetime,
            json_extract_scalar(tariff_json, '$.tariff_id') as tariff_id,
            json_extract_scalar(tariff_json, '$.name') as tariff_name,
            safe_cast(
                json_extract_scalar(tariff_json, '$.cost_per_hour') as float64
            ) as cost_per_hour,
            json_extract_scalar(tariff_json, '$.currency') as currency,
            safe_cast(
                json_extract_scalar(tariff_json, '$.duration_minutes') as int64
            ) as duration_minutes
        from exploded
    )

select *
from parsed
