# Tempo Setup Guide

## Prerequisites

- Docker Desktop installed and running
- Python 3.11+ with venv
- Spotify Developer Account (with Client ID and Secret)

## Quick Start

### 1. Environment Setup

Create `.env` file in the root directory:
```bash
CLIENT_ID=your_spotify_client_id
CLIENT_SECRET=your_spotify_client_secret
REDIRECT_URL=http://127.0.0.1:8888/callback
```

### 2. Start Docker Services

```bash
# Set Airflow UID (required on Linux/Mac)
echo -e "AIRFLOW_UID=$(id -u)" > .env.local

# Start all services (PostgreSQL + Airflow)
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f
```

### 3. Access Services

- **Airflow UI**: http://localhost:8080
  - Username: `admin`
  - Password: `admin`

- **PostgreSQL**:
  - Host: `localhost`
  - Port: `5432`
  - Database: `tempo_warehouse`
  - Username: `postgres`
  - Password: `postgres`

### 4. Configure Airflow Connection

1. Go to Airflow UI → Admin → Connections
2. Click "+" to add new connection
3. Configure:
   - **Connection Id**: `tempo_postgres`
   - **Connection Type**: `Postgres`
   - **Host**: `postgres`
   - **Database**: `tempo_warehouse`
   - **Login**: `postgres`
   - **Password**: `postgres`
   - **Port**: `5432`
4. Click "Save"

### 5. Run the Pipeline

1. In Airflow UI, find `spotify_listening_history_ingestion` DAG
2. Toggle it ON (unpause)
3. Click "Trigger DAG" to run immediately
4. Watch the task execution in the Graph/Tree view

## Verify Data

Connect to PostgreSQL and check data:

```bash
# Enter PostgreSQL container
docker exec -it tempo_postgres psql -U postgres -d tempo_warehouse

# Check data
SELECT COUNT(*) FROM raw_data.listening_history;
SELECT extraction_timestamp, jsonb_array_length(raw_data->'data') as tracks
FROM raw_data.listening_history
ORDER BY extraction_timestamp DESC
LIMIT 5;
```

## Troubleshooting

### Spotify Authentication Issues
- Delete `.cache` file and re-run
- Check `.env` credentials are correct

### Docker Issues
```bash
# Stop all services
docker-compose down

# Remove volumes (WARNING: deletes all data)
docker-compose down -v

# Rebuild and restart
docker-compose up -d --build
```

### Airflow DAG not showing
- Check DAG file syntax: `docker exec tempo_airflow_scheduler airflow dags list`
- View scheduler logs: `docker-compose logs airflow-scheduler`

## Next Steps

- **Milestone 3**: Set up dbt for data transformation
- **Milestone 4**: Create Superset dashboards
