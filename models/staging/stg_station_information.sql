-- models/staging/stg_station_information.sql

with source as (
  select
    ingest_datetime,
    last_updated    as feed_last_updated,
    ttl,
    version,
    s.*
  from {{ source('gbfs','raw_station_information') }},
  unnest(data.stations) as s
)

select
  station_id,
  name[OFFSET(0)].text        as station_name,
  lat,
  lon,
  address,
  cross_street,
  capacity,
  is_virtual_station,
  rental_uris.android         as uri_android,
  rental_uris.ios             as uri_ios,
  rental_uris.web             as uri_web,
  cast(ingest_datetime as timestamp) as ingest_at,
  to_timestamp(last_updated)        as feed_updated_at,
  version,
  ttl
from source