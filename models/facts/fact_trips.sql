-- models/facts/fact_trips.sql
{{ config(materialized="table") }}

with
    src as (
        select
            *,
            -- optional: build a surrogate PK for each trip
            md5(
                concat(
                    cast(started_at as string),
                    cast(ended_at as string),
                    cast(start_station_id as string),
                    cast(end_station_id as string)
                )
            ) as trip_id
        from {{ ref("stg_historic_trips") }}
    )

select
    trip_id,
    started_at,
    ended_at,
    duration_s as raw_duration_s,
    computed_duration_s,
    trip_date,
    trip_hour,
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name,
    start_lat,
    start_lon,
    end_lat,
    end_lon
from src
