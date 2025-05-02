-- models/staging/stg_station_status.sql

with source as (
  select
    ingest_datetime,
    last_updated    as feed_last_updated,
    ttl,
    version,
    s.*  -- expands all fields under data.stations
  from {{ source('gbfs','raw_station_status') }},
  unnest(data.stations) as s
)

select
  station_id,
  to_timestamp(last_reported)      as reported_at,
  num_bikes_available             as bikes_available,
  num_docks_available             as docks_available,
  num_vehicles_disabled           as vehicles_disabled,
  num_docks_disabled              as docks_disabled,
  is_installed,
  is_renting,
  is_returning,
  cast(ingest_datetime as timestamp) as ingest_at,
  to_timestamp(last_updated)        as feed_updated_at,
  version,
  ttl
from source