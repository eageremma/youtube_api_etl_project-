from datetime import datetime
import os
import json
import csv
import urllib.parse
import urllib.request

from airflow import DAG
from airflow.operators.python import PythonOperator

RAW_SEARCH_PATH = "/opt/airflow/data/search_raw.json"
RAW_VIDEOS_PATH = "/opt/airflow/data/videos_raw.json"
CSV_PATH = "/opt/airflow/data/youtube_videos.csv"


def _get_json(url: str) -> dict:
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _iso8601_duration_to_seconds(duration: str) -> int:
    """
    Converts ISO 8601 duration like 'PT1H2M10S' to seconds.
    Handles hours/minutes/seconds.
    """
    if not duration or not duration.startswith("PT"):
        return 0

    duration = duration[2:]  # remove 'PT'
    num = ""
    hours = minutes = seconds = 0

    for ch in duration:
        if ch.isdigit():
            num += ch
        else:
            if num == "":
                continue
            if ch == "H":
                hours = int(num)
            elif ch == "M":
                minutes = int(num)
            elif ch == "S":
                seconds = int(num)
            num = ""

    return hours * 3600 + minutes * 60 + seconds


def extract_search():
    api_key = os.getenv("YT_API_KEY")
    if not api_key:
        raise RuntimeError("Missing YT_API_KEY in environment")

    query = os.getenv("YT_QUERY", "cats")
    max_results = int(os.getenv("YT_MAX_RESULTS", "10"))

    base_url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part": "snippet",
        "q": query,
        "type": "video",
        "maxResults": max_results,
        "key": api_key,
    }
    url = base_url + "?" + urllib.parse.urlencode(params)

    os.makedirs(os.path.dirname(RAW_SEARCH_PATH), exist_ok=True)
    data = _get_json(url)

    with open(RAW_SEARCH_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def extract_video_details():
    api_key = os.getenv("YT_API_KEY")
    if not api_key:
        raise RuntimeError("Missing YT_API_KEY in environment")

    with open(RAW_SEARCH_PATH, "r", encoding="utf-8") as f:
        search_payload = json.load(f)

    video_ids = []
    for item in search_payload.get("items", []):
        vid = item.get("id", {}).get("videoId")
        if vid:
            video_ids.append(vid)

    if not video_ids:
        raise RuntimeError("No video IDs found from search step")

    base_url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "part": "snippet,contentDetails,statistics",
        "id": ",".join(video_ids),
        "key": api_key,
    }
    url = base_url + "?" + urllib.parse.urlencode(params)

    data = _get_json(url)

    os.makedirs(os.path.dirname(RAW_VIDEOS_PATH), exist_ok=True)
    with open(RAW_VIDEOS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def transform_to_csv():
    with open(RAW_VIDEOS_PATH, "r", encoding="utf-8") as f:
        payload = json.load(f)

    rows = []
    for item in payload.get("items", []):
        vid = item.get("id", "")
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        content = item.get("contentDetails", {})

        title = snippet.get("title", "")
        channel_title = snippet.get("channelTitle", "")
        published_at = snippet.get("publishedAt", "")

        # Stats (may be missing for some videos)
        views = int(stats.get("viewCount", 0) or 0)
        likes = int(stats.get("likeCount", 0) or 0)
        comments = int(stats.get("commentCount", 0) or 0)

        # Duration
        duration_iso = content.get("duration", "")
        duration_seconds = _iso8601_duration_to_seconds(duration_iso)

        # Thumbnail (choose best available)
        thumbs = snippet.get("thumbnails", {})
        thumb_url = (
            (thumbs.get("maxres") or {}).get("url")
            or (thumbs.get("standard") or {}).get("url")
            or (thumbs.get("high") or {}).get("url")
            or (thumbs.get("medium") or {}).get("url")
            or (thumbs.get("default") or {}).get("url")
            or ""
        )

        rows.append([
            vid,
            title,
            channel_title,
            published_at,
            views,
            likes,
            comments,
            duration_iso,
            duration_seconds,
            thumb_url,
        ])

    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "video_id",
            "title",
            "channel_title",
            "published_at",
            "views",
            "likes",
            "comments",
            "duration_iso",
            "duration_seconds",
            "thumbnail_url",
        ])
        writer.writerows(rows)


with DAG(
    dag_id="youtube_richer_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="extract_search", python_callable=extract_search)
    t2 = PythonOperator(task_id="extract_video_details", python_callable=extract_video_details)
    t3 = PythonOperator(task_id="transform_to_csv", python_callable=transform_to_csv)

    t1 >> t2 >> t3
