-- models/staging/stg_station_information.sql

with src as (
  select
    ingest_datetime,
    last_updated      as feed_last_updated,
    ttl,
    version,
    -- get an ARRAY<JSON> of station objects
    JSON_EXTRACT_ARRAY(data_json, '$.stations')  as stations_json
  from `data-management-2-arun`.`data_management_2_arun`.`raw_station_information`
),

exploded as (
  select
    ingest_datetime,
    feed_last_updated,
    ttl,
    version,
    station_json
  from src,
  unnest(stations_json) as station_json   -- station_json is now JSON
),

parsed as (
  select
    ingest_datetime,
    timestamp_seconds(feed_last_updated)    as feed_updated_at,
    ttl,
    version,

    JSON_EXTRACT_SCALAR(station_json, '$.station_id')                    as station_id,
    JSON_EXTRACT_SCALAR(station_json, '$.name')                          as station_name,
    cast(JSON_EXTRACT_SCALAR(station_json, '$.lat')  as float64)       as lat,
    cast(JSON_EXTRACT_SCALAR(station_json, '$.lon')  as float64)       as lon,
    JSON_EXTRACT_SCALAR(station_json, '$.address')                      as address,
    JSON_EXTRACT_SCALAR(station_json, '$.cross_street')                 as cross_street,
    cast(JSON_EXTRACT_SCALAR(station_json, '$.capacity') as int64)     as capacity,
    JSON_EXTRACT_SCALAR(station_json, '$.is_virtual_station') = 'true' as is_virtual_station,
    JSON_EXTRACT_SCALAR(station_json, '$.rental_uris.android')         as uri_android,
    JSON_EXTRACT_SCALAR(station_json, '$.rental_uris.ios')             as uri_ios,
    JSON_EXTRACT_SCALAR(station_json, '$.rental_uris.web')             as uri_web

  from exploded
)

select * from parsed
