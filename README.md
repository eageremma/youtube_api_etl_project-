YouTube ETL Pipeline with Apache Airflow & Docker
🚀 Project Overview

This project demonstrates an end-to-end ETL (Extract, Transform, Load) data pipeline built using:

Apache Airflow for orchestration and scheduling

Docker & Docker Compose for reproducible infrastructure

Python for API ingestion and data transformation

YouTube Data API v3 as the data source

The pipeline automatically:

Extracts video data from YouTube

Enriches it with engagement metrics

Transforms raw JSON into clean, analytics-ready CSV files

This simulates a real-world data engineering workflow similar to what is used in production systems.

🧱 Architecture
YouTube API
     |
     v
[Extract Tasks]
     |
 Raw JSON Files
     |
     v
[Transform Task]
     |
 Clean CSV Output
     |
     v
Analytics / BI / ML Ready


All tasks are orchestrated through Airflow DAGs running inside Docker containers.

🛠️ Tech Stack

Python 3

Apache Airflow (Dockerized)

Docker Compose

YouTube Data API v3

Requests, CSV, JSON libraries

📁 Project Structure
youtube-etl/
│
├── dags/
│   └── youtube_simple_pipeline.py
│
├── data/
│   ├── raw_search.json
│   ├── raw_videos.json
│   └── youtube_videos.csv
│
├── logs/
├── plugins/
├── config/
│
├── docker-compose.yaml
└── .env

⚙️ Pipeline Workflow
✅ Task 1 — Extract Search Results

Calls YouTube Search API

Retrieves:

Video IDs

Titles

Channel names

Publish dates

Saves output as:

👉 see the in the repository for - data/raw_search.json

✅ Task 2 — Extract Video Details

Uses video IDs from Task 1

Calls YouTube Videos API

Pulls richer metrics:

Views

Likes

Comment counts

Duration

Thumbnails

👉 see in the repository for -  data/raw_videos.json

✅ Task 3 — Transform to Clean CSV

The transformation step:

Parses nested JSON

Converts ISO-8601 duration to seconds

Normalizes numeric fields
👉 see in the repository for - youtube.videos.csv

Selects analytics-friendly columns

Final dataset includes:
