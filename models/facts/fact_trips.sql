{{ config(materialized="incremental", unique_key="trip_id") }}

with
    src as (
        select
            *,
            -- synthetic primary key for dedup/incremental
            md5(
                concat(
                    cast(started_at as string),
                    cast(ended_at as string),
                    start_station_id,
                    end_station_id,
                    cast(duration_s as string)
                )
            ) as trip_id
        from {{ ref("stg_historic_trips") }}
    )

select
    trip_id,
    started_at,
    ended_at,
    duration_s,
    computed_duration_s,
    date(started_at) as trip_date,
    timestamp_trunc(started_at, hour) as trip_hour,
    start_station_id,
    end_station_id
from src

{% if is_incremental() %}
    where trip_id not in (select trip_id from {{ this }})
{% endif %}
