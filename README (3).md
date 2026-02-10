# 📊 YouTube ETL Pipeline with Apache Airflow & Docker

This repository contains an end-to-end ETL pipeline that pulls YouTube
data, transforms it, and outputs analytics-ready datasets.

## 📈 Results

-   [Final CSV](data/youtube_videos.csv)
-   [Raw Search JSON](data/raw_search.json)
-   [Raw Videos JSON](data/raw_videos.json)

## 🚀 Overview

The pipeline: 1. Extracts video IDs via search 2. Pulls analytics
metrics 3. Transforms to clean CSV

## 🛠 Tech Stack

Airflow • Docker • Python • YouTube API

## ▶️ Run Instructions

1.  Add your API key to `.env`
2.  `docker compose up -d`
3.  Trigger DAG in Airflow UI
