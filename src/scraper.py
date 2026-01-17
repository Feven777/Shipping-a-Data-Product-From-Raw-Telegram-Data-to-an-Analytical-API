from telethon import TelegramClient
from telethon.sessions import StringSession
from src.config import Config
from src.logger import get_logger

logger = get_logger()

class TelegramScraper:
    def __init__(self):
        self.client = TelegramClient(
            "telegram_session",
            Config.TELEGRAM_API_ID,
            Config.TELEGRAM_API_HASH
        )

    async def connect(self):
        logger.info("Connecting to Telegram...")
        await self.client.start(phone=Config.TELEGRAM_PHONE)
        logger.info("Connected successfully!")

    async def disconnect(self):
        logger.info("Disconnecting from Telegram...")
        await self.client.disconnect()
        logger.info("Disconnected.")
