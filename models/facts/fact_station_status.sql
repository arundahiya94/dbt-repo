{{ config(
    materialized = 'incremental',
    unique_key    = 'snapshot_id'
) }}

with src as (
  select
    *,
    -- one snapshot per station‐timestamp
    md5(concat(cast(feed_timestamp as string), station_id)) as snapshot_id
  from {{ ref('stg_station_status') }}
)

select
  snapshot_id,
  feed_timestamp,
  date(feed_timestamp)                      as status_date,
  timestamp_trunc(feed_timestamp, HOUR)     as status_hour,
  station_id,
  num_bikes_available,
  num_docks_available,
  is_virtual_station
from src

{% if is_incremental() %}
  where snapshot_id not in (select snapshot_id from {{ this }})
{% endif %}
