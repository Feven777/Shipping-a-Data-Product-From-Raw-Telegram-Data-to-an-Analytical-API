import os
from pathlib import Path
from dotenv import load_dotenv

# Force load the .env from project root
dotenv_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path)

class Config:
    TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID"))
    TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")
    TELEGRAM_PHONE = os.getenv("TELEGRAM_PHONE")
    BOOTSTRAP_MESSAGE_LIMIT = 300

    # PostgreSQL
    POSTGRES_HOST = os.getenv("POSTGRES_HOST") or "127.0.0.1"
    POSTGRES_PORT = os.getenv("POSTGRES_PORT") or "5432"
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

    POSTGRES_DSN = (
        f"host={POSTGRES_HOST} "
        f"port={POSTGRES_PORT} "
        f"dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} "
        f"password={POSTGRES_PASSWORD}"
    )

    # Debug prints to check if values are loaded
    print("Loaded PostgreSQL config:")
    print(f"HOST={POSTGRES_HOST}, PORT={POSTGRES_PORT}, DB={POSTGRES_DB}, USER={POSTGRES_USER}, PASSWORD={'***' if POSTGRES_PASSWORD else 'None'}")
