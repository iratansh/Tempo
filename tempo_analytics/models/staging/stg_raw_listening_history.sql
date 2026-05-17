{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Parse raw JSON into a flat structure for easier transformation
WITH raw_data AS (
    SELECT
        id as raw_id,
        extraction_timestamp,
        raw_data,
        ingestion_timestamp
    FROM {{ source('raw_data', 'listening_history') }}
),

parsed AS (
    SELECT
        raw_id,
        extraction_timestamp,
        ingestion_timestamp,
        jsonb_array_elements(raw_data -> 'data') as event
    FROM raw_data
)

SELECT
    raw_id,
    extraction_timestamp,
    ingestion_timestamp,
    
    -- Listening event details
    (event ->> 'played_at')::timestamp as played_at,
    event ->> 'context_type' as context_type,
    event ->> 'context_uri' as context_uri,
    
    -- Track details
    event -> 'track' ->> 'track_id' as track_id,
    event -> 'track' ->> 'track_name' as track_name,
    (event -> 'track' ->> 'track_popularity')::int as track_popularity,
    (event -> 'track' ->> 'track_explicit')::boolean as track_explicit,
    (event -> 'track' ->> 'track_duration_ms')::int as track_duration_ms,
    event -> 'track' ->> 'track_uri' as track_uri,
    
    -- Artist details
    event -> 'artist' ->> 'artist_id' as artist_id,
    event -> 'artist' ->> 'artist_name' as artist_name,
    event -> 'artist' ->> 'artist_uri' as artist_uri,
    
    -- Album details
    event -> 'album' ->> 'album_id' as album_id,
    event -> 'album' ->> 'album_name' as album_name,
    event -> 'album' ->> 'album_type' as album_type,
    event -> 'album' ->> 'album_release_date' as album_release_date,
    (event -> 'album' ->> 'album_total_tracks')::int as album_total_tracks,
    event -> 'album' ->> 'album_image_url' as album_image_url,
    event -> 'album' ->> 'album_uri' as album_uri

FROM parsed
