import asyncio
from datetime import datetime, timezone, timedelta
import logging
from store_options import OptionsDataStore

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def main():
    store = OptionsDataStore()
    await store.initialize()
    last_synced_day = None  # Changed from last_synced_hour to last_synced_day
    try:
        while True:
            await store.run_cycle()
    
            # Check if a new day is reached (at UTC midnight)
            now = datetime.now(timezone.utc)
            current_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
            if last_synced_day is None or current_day > last_synced_day:
                logger.info("New day reached: rotating data folder for S3 sync.")
                rotated_folder = store.rotate_data_directory()
                if rotated_folder:
                    logger.info("Initiating S3 sync and cleanup on rotated folder.")
                    store.s3_sync_and_cleanup(folder=rotated_folder)
                    logger.info("S3 sync and cleanup complete.")
                last_synced_day = current_day
    finally:
        await store.close()

if __name__ == "__main__":
    asyncio.run(main())