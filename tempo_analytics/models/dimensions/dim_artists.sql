{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

-- Dimension table for artists (deduplicated)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_uri,
    MAX(extraction_timestamp) as last_updated
FROM {{ ref('stg_raw_listening_history') }}
GROUP BY 
    artist_id,
    artist_name,
    artist_uri
