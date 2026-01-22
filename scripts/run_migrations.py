import psycopg2
from pathlib import Path
from src.config import Config
from src.logger import get_logger

logger = get_logger()

def run_migration(file_path: Path):
    logger.info(f"Running migration: {file_path.name}")

    with open(file_path, "r", encoding="utf-8") as f:
        sql = f.read()

    conn = psycopg2.connect(Config.POSTGRES_DSN)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
        logger.info(f"Migration {file_path.name} applied successfully.")
    finally:
        conn.close()

def main():
    migrations_dir = Path("migrations/raw")
    for migration in sorted(migrations_dir.glob("*.sql")):
        run_migration(migration)

if __name__ == "__main__":
    main()
