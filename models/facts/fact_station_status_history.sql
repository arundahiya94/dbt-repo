-- models/facts/fact_station_status_history.sql
{{ config(materialized="incremental", unique_key="snapshot_id") }}

with
    src as (
        select
            *, md5(concat(cast(feed_updated_at as string), station_id)) as snapshot_id
        from {{ ref("stg_station_status") }}
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
from src

{% if is_incremental() %}
    where snapshot_id not in (select snapshot_id from {{ this }})
{% endif %}
