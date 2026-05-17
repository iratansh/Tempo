{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

-- Fact table: listening events with foreign keys to dimensions
WITH staging AS (
    SELECT * FROM {{ ref('stg_raw_listening_history') }}
),

-- Deduplicate by keeping the most recent extraction for each play event
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY played_at, track_id
            ORDER BY extraction_timestamp DESC
        ) as rn
    FROM staging
)

SELECT
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['played_at', 'track_id']) }} as listening_event_id,

    -- Foreign keys
    track_id,
    artist_id,
    album_id,

    -- Timestamps
    played_at,
    DATE(played_at) as played_date,
    EXTRACT(HOUR FROM played_at) as played_hour,
    EXTRACT(DOW FROM played_at) as day_of_week,

    -- Context
    context_type,
    context_uri,

    -- Metadata
    extraction_timestamp,
    ingestion_timestamp

FROM deduped
WHERE rn = 1
