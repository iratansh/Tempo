import spotipy
from spotipy.oauth2 import SpotifyOAuth
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('backend/logs/extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SpotifyExtractor:
    """
    Spotify data extraction client.
    Handles OAuth authentication and extracts listening history data.
    """

    def __init__(self, client_id: str, client_secret: str, redirect_uri: str):
        self.client_id = client_id
        self.client_secret = client_secret
        scope = "user-read-recently-played playlist-read-private playlist-read-collaborative"

        try:
            self.sp = spotipy.Spotify(auth_manager=spotipy.SpotifyOAuth(
                client_id=client_id,
                client_secret=client_secret,
                redirect_uri=redirect_uri,
                scope=scope
            ))
            logger.info("Successfully authenticated with Spotify API")
        except Exception as e:
            logger.error(f"Failed to authenticate with Spotify: {str(e)}")
            raise

    def get_recently_played(self, limit: int = 50) -> Dict:
        """
        Extract recently played tracks with all relevant fields for the data warehouse.

        Args:
            limit: Number of recently played tracks to retrieve (max 50)

        Returns:
            Dictionary containing structured listening history data
        """
        try:
            logger.info(f"Fetching {limit} recently played tracks")
            results = self.sp.current_user_recently_played(limit=limit)

            # Transform raw API response into structured data
            listening_history = {
                'extraction_timestamp': datetime.utcnow().isoformat(),
                'total_tracks': len(results['items']),
                'data': []
            }

            for item in results['items']:
                track = item['track']

                # Extract fact table fields
                listening_event = {
                    'played_at': item['played_at'],
                    'context_type': item.get('context', {}).get('type') if item.get('context') else None,
                    'context_uri': item.get('context', {}).get('uri') if item.get('context') else None,

                    # Track information
                    'track': {
                        'track_id': track['id'],
                        'track_name': track['name'],
                        'track_popularity': track.get('popularity', 0),
                        'track_explicit': track.get('explicit', False),
                        'track_duration_ms': track['duration_ms'],
                        'track_uri': track['uri']
                    },

                    # Artist information (primary artist)
                    'artist': {
                        'artist_id': track['artists'][0]['id'],
                        'artist_name': track['artists'][0]['name'],
                        'artist_uri': track['artists'][0]['uri']
                    },

                    # Album information
                    'album': {
                        'album_id': track['album']['id'],
                        'album_name': track['album']['name'],
                        'album_type': track['album']['album_type'],
                        'album_release_date': track['album']['release_date'],
                        'album_total_tracks': track['album']['total_tracks'],
                        'album_image_url': track['album']['images'][0]['url'] if track['album']['images'] else None,
                        'album_uri': track['album']['uri']
                    }
                }

                listening_history['data'].append(listening_event)

            logger.info(f"Successfully extracted {len(listening_history['data'])} listening events")
            return listening_history

        except Exception as e:
            logger.error(f"Failed to fetch recently played tracks: {str(e)}")
            raise

    def get_playlist_tracks(self, playlist_id: str) -> Dict:
        """
        Extract tracks from a specific playlist.

        Args:
            playlist_id: Spotify playlist ID

        Returns:
            Dictionary containing playlist tracks data
        """
        try:
            logger.info(f"Fetching tracks from playlist: {playlist_id}")
            results = self.sp.playlist_tracks(playlist_id)

            playlist_data = {
                'extraction_timestamp': datetime.utcnow().isoformat(),
                'playlist_id': playlist_id,
                'total_tracks': len(results['items']),
                'tracks': []
            }

            for item in results['items']:
                # Handle both 'track' and 'item' keys for different API responses
                track = item.get('track') or item.get('item')

                if track and track is not None:
                    track_info = {
                        'track_id': track['id'],
                        'track_name': track['name'],
                        'artist_name': track['artists'][0]['name'],
                        'album_name': track['album']['name'],
                        'added_at': item.get('added_at')
                    }
                    playlist_data['tracks'].append(track_info)

            logger.info(f"Successfully extracted {len(playlist_data['tracks'])} tracks from playlist")
            return playlist_data

        except Exception as e:
            logger.error(f"Failed to fetch playlist tracks: {str(e)}")
            raise

    def save_to_json(self, data: Dict, filename: str) -> None:
        """
        Save extracted data to JSON file (raw data lake layer).

        Args:
            data: Dictionary to save
            filename: Output filename
        """
        try:
            with open(f'backend/data/{filename}', 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Successfully saved data to backend/data/{filename}")
        except Exception as e:
            logger.error(f"Failed to save JSON file: {str(e)}")
            raise
