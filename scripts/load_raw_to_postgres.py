import json
from glob import glob
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_batch

from src.config import Config
from src.logger import get_logger

logger = get_logger()


def get_connection():
    return psycopg2.connect(Config.POSTGRES_DSN)


def create_schema_and_table(conn):
    sql = """
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.telegram_messages (
        message_id      BIGINT,
        channel         TEXT,
        message_date    TIMESTAMPTZ,
        message_text    TEXT,
        views           INTEGER,
        forwards        INTEGER,
        has_media       BOOLEAN,
        media_file      TEXT,
        raw_json        JSONB,
        PRIMARY KEY (message_id, channel)
    );
    """

    with conn.cursor() as cur:
        cur.execute(sql)

    conn.commit()
    logger.info("Ensured raw.telegram_messages table exists.")


def find_json_files():
    base = Path("data/raw/telegram_messages")
    files = glob(str(base / "**" / "*.json"), recursive=True)
    logger.info(f"Found {len(files)} JSON files.")
    return files


def load_messages(files):
    messages = []

    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)
            messages.extend(data)

    logger.info(f"Loaded {len(messages)} messages from JSON files.")
    return messages


def insert_messages(conn, messages):
    sql = """
    INSERT INTO raw.telegram_messages (
        message_id,
        channel,
        message_date,
        message_text,
        views,
        forwards,
        has_media,
        media_file,
        raw_json
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (message_id, channel) DO NOTHING;
    """

    values = []

    for m in messages:
        values.append((
            m.get("message_id"),
            m.get("channel"),
            m.get("message_date"),
            m.get("message_text"),
            m.get("views"),
            m.get("forwards"),
            m.get("has_media"),
            m.get("media_file"),
            json.dumps(m),
        ))

    with conn.cursor() as cur:
        execute_batch(cur, sql, values, page_size=500)

    conn.commit()
    logger.info(f"Inserted {len(values)} records (duplicates ignored).")


def main():
    logger.info("Connecting to PostgreSQL...")
    conn = get_connection()

    logger.info("Creating schema and table if needed...")
    create_schema_and_table(conn)

    logger.info("Finding JSON files...")
    files = find_json_files()

    if not files:
        logger.warning("No JSON files found. Nothing to load.")
        return

    logger.info("Loading messages...")
    messages = load_messages(files)

    logger.info("Inserting messages into database...")
    insert_messages(conn, messages)

    conn.close()
    logger.info("Raw data load completed successfully.")


if __name__ == "__main__":
    main()
