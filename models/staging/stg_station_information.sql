-- models/staging/stg_station_information.sql

with src as (
  select
    ingest_datetime,
    last_updated  as feed_last_updated,
    ttl,
    version,

    -- extract the JSON array of stations
    json_extract(data_json, '$.stations')   as stations_json
  from {{ source('gbfs','raw_station_information') }}
),

exploded as (
  select
    ingest_datetime,
    feed_last_updated,
    ttl,
    version,

    -- each station as JSON
    station_json
  from src,
  unnest( cast(json_extract_array(stations_json) as array<json>) ) as station_json
),

parsed as (
  select
    ingest_datetime,
    timestamp_seconds(feed_last_updated)  as feed_updated_at,
    ttl,
    version,

    -- drill into each station JSON object
    station_json:station_id                as station_id,
    station_json:name[0].text              as station_name,
    station_json:lat                       as lat,
    station_json:lon                       as lon,
    station_json:address                   as address,
    station_json:cross_street              as cross_street,
    station_json:capacity                  as capacity,
    station_json:is_virtual_station        as is_virtual_station,
    station_json:rental_uris.android       as uri_android,
    station_json:rental_uris.ios           as uri_ios,
    station_json:rental_uris.web           as uri_web

  from exploded
)

select * from parsed
