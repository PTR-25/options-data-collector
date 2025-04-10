import asyncio
from store_options import OptionsDataStore
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def main():
    store = OptionsDataStore()  # Uses BASE_SAVE_PATH from your .env
    try:
        await store.run()  # This runs continuous 5-minute cycles
    finally:
        await store.close()

if __name__ == "__main__":
    asyncio.run(main())
