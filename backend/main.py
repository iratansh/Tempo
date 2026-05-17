import os
from dotenv import load_dotenv
from modules.Extract import SpotifyExtractor
from datetime import datetime


if __name__ == "__main__":
    load_dotenv()
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')
    redirect_url = os.getenv('REDIRECT_URL')

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

    # Save to JSON (raw data lake)
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    filename = f"raw_listening_history_{timestamp}.json"
    extractor.save_to_json(listening_history, filename)

    print(f"\n✅ Milestone 1 Complete!")
    print(f"   - Data extracted from Spotify API")
    print(f"   - Saved to: backend/data/{filename}")
    print(f"   - Ready for Airflow ingestion (Milestone 2)")
