{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

-- Dimension table for tracks (deduplicated)
SELECT DISTINCT
    track_id,
    track_name,
    track_popularity,
    track_explicit,
    track_duration_ms,
    track_uri,
    album_id,
    album_name,
    album_release_date,
    -- Use the most recent data for each track
    MAX(extraction_timestamp) as last_updated
FROM {{ ref('stg_raw_listening_history') }}
GROUP BY 
    track_id,
    track_name,
    track_popularity,
    track_explicit,
    track_duration_ms,
    track_uri,
    album_id,
    album_name,
    album_release_date
