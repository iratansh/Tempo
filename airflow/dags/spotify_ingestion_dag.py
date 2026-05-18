"""
Spotify Listening History Ingestion DAG
Runs every 3 hours to extract listening data and load into PostgreSQL
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import sys
import os
import json
import logging
import io
from minio import Minio
from minio.error import S3Error

# Add backend directory to Python path
sys.path.insert(0, '/opt/airflow/backend')

from modules.Extract import SpotifyExtractor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'tempo',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def extract_spotify_data(**context):
    """
    Extract recently played tracks from Spotify API
    """
    logger.info("Starting Spotify data extraction...")

    # Get credentials from environment
    client_id = os.getenv('SPOTIFY_CLIENT_ID')
    client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
    redirect_uri = os.getenv('SPOTIFY_REDIRECT_URI')

    if not all([client_id, client_secret, redirect_uri]):
        raise ValueError("Missing Spotify credentials in environment variables")

    # Initialize extractor
    extractor = SpotifyExtractor(client_id, client_secret, redirect_uri)

    # Extract data
    listening_history = extractor.get_recently_played(limit=50)

    logger.info(f"Successfully extracted {listening_history['total_tracks']} tracks")

    # Save to MinIO (data lake)
    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'minio:9000')
    minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
    minio_bucket = os.getenv('MINIO_BUCKET', 'tempo-data')
    minio_secure = os.getenv('MINIO_SECURE', 'false').lower() in {"1", "true", "yes"}

    object_name = f"listening_history/raw_listening_history_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"

    try:
        client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=minio_secure
        )

        if not client.bucket_exists(minio_bucket):
            client.make_bucket(minio_bucket)

        payload = json.dumps(listening_history, indent=2).encode("utf-8")
        client.put_object(
            minio_bucket,
            object_name,
            io.BytesIO(payload),
            length=len(payload),
            content_type="application/json"
        )

        logger.info(f"Saved to MinIO: {minio_bucket}/{object_name}")

        context['task_instance'].xcom_push(
            key='minio_object_name',
            value=object_name
        )

        return listening_history['total_tracks']
    except S3Error as e:
        logger.error(f"MinIO S3 error: {str(e)}")
        raise


def load_to_postgres(**context):
    """
    Load extracted data into PostgreSQL raw_data table
    """
    logger.info("Loading data to PostgreSQL...")

    # Pull MinIO object name from XCom
    object_name = context['task_instance'].xcom_pull(
        task_ids='extract_spotify_data',
        key='minio_object_name'
    )

    if not object_name:
        logger.warning("No MinIO object to load")
        return 0

    # Fetch from MinIO
    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'minio:9000')
    minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
    minio_bucket = os.getenv('MINIO_BUCKET', 'tempo-data')
    minio_secure = os.getenv('MINIO_SECURE', 'false').lower() in {"1", "true", "yes"}

    client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=minio_secure
    )

    response = client.get_object(minio_bucket, object_name)
    try:
        payload = response.read()
    finally:
        response.close()
        response.release_conn()

    listening_history = json.loads(payload.decode("utf-8"))

    if not listening_history or not listening_history.get('data'):
        logger.warning("No data to load")
        return 0

    # Connect to PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='tempo_postgres')

    # Insert data
    extraction_timestamp = listening_history['extraction_timestamp']
    raw_data_json = json.dumps(listening_history)

    insert_query = """
        INSERT INTO raw_data.listening_history (extraction_timestamp, raw_data)
        VALUES (%s, %s)
        ON CONFLICT (extraction_timestamp) DO NOTHING
        RETURNING id;
    """

    try:
        result = postgres_hook.run(
            insert_query,
            parameters=(extraction_timestamp, raw_data_json),
            autocommit=True
        )

        if result:
            logger.info(f"Successfully loaded data with ID: {result[0][0]}")
        else:
            logger.info("Data already exists (duplicate extraction_timestamp)")

        return len(listening_history['data'])

    except Exception as e:
        logger.error(f"Failed to load data: {str(e)}")
        raise


def verify_data_quality(**context):
    """
    Verify data was loaded correctly
    """
    logger.info("Verifying data quality...")

    postgres_hook = PostgresHook(postgres_conn_id='tempo_postgres')

    # Check row count
    count_query = "SELECT COUNT(*) FROM raw_data.listening_history;"
    count_result = postgres_hook.get_first(count_query)
    total_rows = count_result[0] if count_result else 0

    # Check latest record
    latest_query = """
        SELECT
            extraction_timestamp,
            jsonb_array_length(raw_data->'data') as track_count
        FROM raw_data.listening_history
        ORDER BY extraction_timestamp DESC
        LIMIT 1;
    """
    latest_result = postgres_hook.get_first(latest_query)

    if latest_result:
        latest_ts, track_count = latest_result
        logger.info(f" Data Quality Check Passed:")
        logger.info(f"   Total records in database: {total_rows}")
        logger.info(f"   Latest extraction: {latest_ts}")
        logger.info(f"   Tracks in latest extraction: {track_count}")
    else:
        logger.warning("No data found in database")

    return total_rows


# Define the DAG
with DAG(
    'spotify_listening_history_ingestion',
    default_args=default_args,
    description='Extract Spotify listening history and load to PostgreSQL every 3 hours',
    schedule_interval='0 */3 * * *',  # Every 3 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spotify', 'extraction', 'etl'],
) as dag:

    # Task 1: Extract data from Spotify
    extract_task = PythonOperator(
        task_id='extract_spotify_data',
        python_callable=extract_spotify_data,
        provide_context=True,
    )

    # Task 2: Load data to PostgreSQL
    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True,
    )

    # Task 3: Verify data quality
    verify_task = PythonOperator(
        task_id='verify_data_quality',
        python_callable=verify_data_quality,
        provide_context=True,
    )

    # Define task dependencies
    extract_task >> load_task >> verify_task
