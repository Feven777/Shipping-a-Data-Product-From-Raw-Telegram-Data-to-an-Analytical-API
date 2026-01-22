from dotenv import load_dotenv
from pathlib import Path
import os
import psycopg2

dotenv_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path)

print("DEBUG PASSWORD =", repr(os.getenv("POSTGRES_PASSWORD")))
import csv
# -----------------------------
# Configuration
# -----------------------------
CSV_PATH = Path("data/yolo/detections.csv")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

# -----------------------------
# Connect to Postgres
# -----------------------------
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# -----------------------------
# Create raw table
# -----------------------------
cur.execute("""
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.yolo_detections (
        message_id TEXT,
        channel_name TEXT,
        object_name TEXT,
        confidence FLOAT
    );
""")

# Optional: truncate for clean reload
cur.execute("TRUNCATE TABLE raw.yolo_detections;")

# -----------------------------
# Load CSV
# -----------------------------
with open(CSV_PATH, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    rows = [
        (
            r["message_id"],
            r["channel_name"],
            r["object_name"],
            r["confidence"] if r["confidence"] else None,
        )
        for r in reader
    ]

cur.executemany(
    """
    INSERT INTO raw.yolo_detections
    (message_id, channel_name, object_name, confidence)
    VALUES (%s, %s, %s, %s)
    """,
    rows,
)

conn.commit()
cur.close()
conn.close()

print(f"Loaded {len(rows)} YOLO detection rows into raw.yolo_detections")
