-- models/staging/stg_station_information.sql
with
    source as (
        select
            ingest_datetime,
            last_updated as feed_last_updated,
            ttl,
            version,
            -- get an ARRAY<JSON> of station objects
            json_extract_array(data_json, '$.stations') as stations_json
        from {{ source("gbfs", "raw_station_information") }}
    ),

    exploded as (
        select ingest_datetime, feed_last_updated, ttl, version, station_json
        from source, unnest(stations_json) as station_json  -- station_json is now JSON
    ),

    parsed as (
        select
            ingest_datetime,
            timestamp_seconds(feed_last_updated) as feed_updated_at,
            ttl,
            version,

            json_extract_scalar(station_json, '$.station_id') as station_id,
            json_extract_scalar(station_json, '$.name') as station_name,
            cast(json_extract_scalar(station_json, '$.lat') as float64) as lat,
            cast(json_extract_scalar(station_json, '$.lon') as float64) as lon,
            json_extract_scalar(station_json, '$.address') as address,
            json_extract_scalar(station_json, '$.cross_street') as cross_street,
            cast(json_extract_scalar(station_json, '$.capacity') as int64) as capacity,
            json_extract_scalar(station_json, '$.is_virtual_station')
            = 'true' as is_virtual_station,
            json_extract_scalar(station_json, '$.rental_uris.android') as uri_android,
            json_extract_scalar(station_json, '$.rental_uris.ios') as uri_ios,
            json_extract_scalar(station_json, '$.rental_uris.web') as uri_web

        from exploded
    )

select *
from parsed
