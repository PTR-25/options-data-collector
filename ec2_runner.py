import asyncio
from datetime import datetime, timezone, timedelta
import logging
from store_options import OptionsDataStore

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def main():
    store = OptionsDataStore()  # Uses BASE_SAVE_PATH from your .env
    await store.initialize()
    last_synced_hour = None  # To track the last full-hour sync
    try:
        while True:
            await store.run_cycle()
    
            # Check if a new full hour is reached
            now = datetime.now(timezone.utc)
            current_full_hour = now.replace(minute=0, second=0, microsecond=0)
            if last_synced_hour is None or current_full_hour > last_synced_hour:
                logger.info("Full hour reached: rotating data folder for S3 sync.")
                rotated_folder = store.rotate_data_directory()  # Rotate current data folder
                if rotated_folder:
                    logger.info("Initiating S3 sync and cleanup on rotated folder.")
                    store.s3_sync_and_cleanup(folder=rotated_folder)
                    logger.info("S3 sync and cleanup complete.")
                last_synced_hour = current_full_hour
    finally:
        await store.close()

if __name__ == "__main__":
    asyncio.run(main())
