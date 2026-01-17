import asyncio
from src.scraper import TelegramScraper

async def main():
    scraper = TelegramScraper()
    await scraper.connect()
    await scraper.disconnect()

if __name__ == "__main__":
    asyncio.run(main())