import os
from dotenv import load_dotenv
from modules.Extract import SpotifyExtractor
from datetime import datetime


def _parse_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


if __name__ == "__main__":
    load_dotenv()
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')
    redirect_url = os.getenv('REDIRECT_URL')

    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
    minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
    minio_bucket = os.getenv('MINIO_BUCKET', 'tempo-data')
    minio_secure = _parse_bool(os.getenv('MINIO_SECURE'), default=False)

    # Initialize extractor
    extractor = SpotifyExtractor(client_id, client_secret, redirect_url)

    # Extract recently played tracks
    print("\n" + "="*60)
    print("EXTRACTING RECENTLY PLAYED TRACKS")
    print("="*60)

    listening_history = extractor.get_recently_played(limit=10)

    # Display summary
    print(f"\nExtracted {listening_history['total_tracks']} tracks")
    print(f"Extraction timestamp: {listening_history['extraction_timestamp']}")
    print("\nSample tracks:")
    for i, event in enumerate(listening_history['data'][:5], 1):
        print(f"{i}. {event['track']['track_name']} - {event['artist']['artist_name']}")
        print(f"   Played at: {event['played_at']}")
        print(f"   Context: {event['context_type'] or 'Unknown'}")

    # Save to MinIO (raw data lake)
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    filename = f"raw_listening_history_{timestamp}.json"
    object_name = f"listening_history/{filename}"
    extractor.save_to_minio(
        listening_history,
        bucket=minio_bucket,
        object_name=object_name,
        endpoint=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=minio_secure
    )

    print(f"   - Data extracted from Spotify API")
    print(f"   - Saved to MinIO: {minio_bucket}/{object_name}")
    print(f"   - Ready for Airflow ingestion")
