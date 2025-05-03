-- models/facts/fact_station_status_latest.sql
{{ config(materialized="view") }}

with
    src as (
        select
            *, md5(concat(cast(feed_updated_at as string), station_id)) as snapshot_id
        from {{ ref("stg_station_status") }}
    ),

    ranked as (
        select
            *,
            row_number() over (
                partition by station_id order by feed_updated_at desc
            ) as rn
        from src
    )

select
    snapshot_id,
    feed_updated_at as status_timestamp,
    date(feed_updated_at) as status_date,
    timestamp_trunc(feed_updated_at, hour) as status_hour,
    station_id,
    bikes_available,
    docks_available,
    is_installed,
    is_renting,
    is_returning
from ranked
where rn = 1