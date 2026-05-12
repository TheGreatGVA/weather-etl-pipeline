# Weather ETL Pipeline

## Overview
An automated hourly ETL pipeline that ingests real-time weather data 
for 5 Indian cities, transforms it through Bronze/Silver/Gold Medallion 
layers, and surfaces aggregated analytics — fully orchestrated with 
Apache Airflow and containerized with Docker.

## Architecture
OpenWeatherMap API → Airflow DAG → PostgreSQL (Bronze) 
→ dbt Staging Models (Silver) → dbt Analytics Models (Gold)

## Tech Stack
- **Orchestration:** Apache Airflow 3.2
- **Transformation:** dbt Core 1.11
- **Database:** PostgreSQL 13
- **Language:** Python 3.13
- **Containerization:** Docker + Docker Compose

## Pipeline Layers
- **Bronze** — Raw API response stored as-is in `raw.weather_raw`
- **Silver** — Cleaned and type-cast data in `analytics.stg_weather`
- **Gold** — City-level daily aggregations in `analytics.weather_summary`

## dbt Tests
Schema tests on `stg_weather` validate:
- `city` — not null
- `temp_c` — not null  
- `humidity` — not null

## How to Run
1. Clone the repo
2. Create a `.env` file with your credentials (see `.env.example`)
3. Start all containers:
```bash
docker compose -f docker-compose.yaml up -d
```
4. Open Airflow UI at `http://localhost:8080`
5. Trigger the `weather_etl` DAG manually or wait for hourly schedule

## Project Structure
weather-pipeline/
│
├── dags/
│   └── weather_etl.py        # Airflow DAG — extract, load, dbt trigger
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   ├── stg_weather.sql
│       │   └── schema.yml
│       └── analytics/
│           └── weather_summary.sql
│
├── docker-compose.yaml
├── .env.example
└── README.md

## Screenshots
