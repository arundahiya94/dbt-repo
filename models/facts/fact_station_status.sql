-- models/facts/fact_station_status.sql
{{ config(materialized="incremental", unique_key="snapshot_id") }}

with
    status_src as (
        select
            *,
            -- stable PK per station‐timestamp
            md5(concat(cast(feed_updated_at as string), station_id)) as snapshot_id
        from {{ ref("stg_station_status") }}
    ),

    info_src as (
        select station_id, is_virtual_station from {{ ref("stg_station_information") }}
    )

select
    s.snapshot_id,
    s.feed_updated_at as status_timestamp,
    date(s.feed_updated_at) as status_date,
    timestamp_trunc(s.feed_updated_at, hour) as status_hour,
    s.station_id,
    s.bikes_available,
    s.docks_available,
    coalesce(i.is_virtual_station, false) as is_virtual_station
from status_src s

left join info_src i using (station_id)

{% if is_incremental() %}
    where s.snapshot_id not in (select snapshot_id from {{ this }})
{% endif %}
