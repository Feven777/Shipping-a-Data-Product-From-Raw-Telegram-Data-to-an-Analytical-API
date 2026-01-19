import asyncio
import json
from pathlib import Path
from datetime import datetime

from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto

from src.config import Config
from src.logger import get_logger
from src.checkpoint import CheckpointManager
from src.channels import CHANNELS

logger = get_logger()


class TelegramScraper:
    def __init__(self):
        self.client = TelegramClient(
            "telegram_session",
            Config.TELEGRAM_API_ID,
            Config.TELEGRAM_API_HASH,
        )
        self.checkpoints = CheckpointManager()

    async def connect(self):
        logger.info("Connecting to Telegram...")
        await self.client.start(phone=Config.TELEGRAM_PHONE)
        logger.info("Connected successfully!")

    async def disconnect(self):
        logger.info("Disconnecting from Telegram...")
        await self.client.disconnect()
        logger.info("Disconnected.")

    async def scrape_channel(self, channel_name, channel_url):
        logger.info(f"Scraping channel: {channel_name}")

        # 🔹 Get last scraped message id
        last_id = self.checkpoints.get_last_id(channel_name)
        logger.info(f"Last message id for {channel_name}: {last_id}")

        messages = []
        max_id = last_id

        # 🔹 Detect bootstrap vs incremental mode
        is_bootstrap = last_id == 0

        if is_bootstrap:
            logger.info(
                f"Bootstrap mode detected for {channel_name}. "
                f"Fetching last {Config.BOOTSTRAP_MESSAGE_LIMIT} messages only."
            )
            message_iterator = self.client.iter_messages(
                channel_url,
                limit=Config.BOOTSTRAP_MESSAGE_LIMIT,
            )
        else:
            logger.info(
                f"Incremental mode for {channel_name}. Fetching new messages only."
            )
            message_iterator = self.client.iter_messages(
                channel_url,
                min_id=last_id,
            )

        async for message in message_iterator:
            try:
                msg_id = message.id
                msg_date = message.date.isoformat()

                # Track highest message id seen
                if msg_id > max_id:
                    max_id = msg_id

                text = message.text or ""
                views = getattr(message, "views", 0)
                forwards = getattr(message, "forwards", 0)

                has_media = False
                media_path = None

                # 🔹 Media handling
                if message.media:
                    has_media = True
                    if isinstance(message.media, MessageMediaPhoto):
                        img_folder = Path("data/raw/images") / channel_name
                        img_folder.mkdir(parents=True, exist_ok=True)

                        file_path = img_folder / f"{msg_id}.jpg"
                        await message.download_media(file=file_path)
                        media_path = str(file_path)

                msg_record = {
                    "message_id": msg_id,
                    "channel": channel_name,
                    "message_date": msg_date,
                    "message_text": text,
                    "views": views,
                    "forwards": forwards,
                    "has_media": has_media,
                    "media_file": media_path,
                }

                messages.append(msg_record)

            except Exception as e:
                logger.error(f"Error processing message {message.id}: {e}")

        # 🔹 Write messages to data lake
        if messages:
            self._write_json(channel_name, messages)

        # 🔹 Update checkpoint
        self.checkpoints.update(channel_name, max_id)
        logger.info(f"Updated checkpoint for {channel_name}: {max_id}")

    def _write_json(self, channel_name, messages):
        # Partition by current UTC date
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        out_folder = Path("data/raw/telegram_messages") / date_str
        out_folder.mkdir(parents=True, exist_ok=True)

        out_file = out_folder / f"{channel_name}.json"

        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(messages, f, indent=2, ensure_ascii=False)

        logger.info(f"Wrote {len(messages)} messages to {out_file}")

    async def run(self):
        await self.connect()

        for channel_name, channel_url in CHANNELS.items():
            try:
                await self.scrape_channel(channel_name, channel_url)
            except Exception as e:
                logger.error(f"Error scraping {channel_name}: {e}")

        await self.disconnect()


if __name__ == "__main__":
    scraper = TelegramScraper()
    asyncio.run(scraper.run())
