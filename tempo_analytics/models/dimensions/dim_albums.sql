{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

-- Dimension table for albums (deduplicated)
SELECT DISTINCT
    album_id,
    album_name,
    album_type,
    album_release_date,
    album_total_tracks,
    album_image_url,
    album_uri,
    MAX(extraction_timestamp) as last_updated
FROM {{ ref('stg_raw_listening_history') }}
GROUP BY 
    album_id,
    album_name,
    album_type,
    album_release_date,
    album_total_tracks,
    album_image_url,
    album_uri
