from loguru import logger
import sys
from pathlib import Path

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(
    LOG_DIR / "scraper.log",
    level="INFO",
    rotation="10 MB",
    retention="7 days",
    format="{time} | {level} | {message}"
)

def get_logger():
    return logger
