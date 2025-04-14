import asyncio
from datetime import datetime, timezone, timedelta
import logging
from store_options import OptionsDataStore
import os
from pathlib import Path
import shutil

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def is_maintenance_window():
    now = datetime.now(timezone.utc)
    # Example maintenance window: April 15, 2025 9:00 AM UTC for 30 minutes
    maintenance_start = datetime(2025, 4, 15, 9, 0, tzinfo=timezone.utc)
    maintenance_end = maintenance_start + timedelta(minutes=30)
    return maintenance_start <= now <= maintenance_end

class ConnectionManager:
    def __init__(self):
        self.base_delay = 60  # Start with 1 minute delay
        self.max_delay = 1800  # Max 30 minutes delay
        self.current_delay = self.base_delay
        self.consecutive_failures = 0
        self.last_success = None
        
    def connection_succeeded(self):
        self.current_delay = self.base_delay
        self.consecutive_failures = 0
        self.last_success = datetime.now(timezone.utc)
        
    def connection_failed(self):
        self.consecutive_failures += 1
        self.current_delay = min(self.current_delay * 2, self.max_delay)
        return self.current_delay
        
    def should_reset_connection(self) -> bool:
        if self.last_success is None:
            return True
        # Reset connection if no successful operations for 30 minutes
        return (datetime.now(timezone.utc) - self.last_success).total_seconds() > 1800

def cleanup_old_folders(base_dir: str, max_age_hours: int = 48):
    """Clean up old data folders that have already been synced to S3"""
    base_path = Path(base_dir)
    current_time = datetime.now(timezone.utc)
    deleted_count = 0
    skipped_count = 0
    
    # Get all folders that match the pattern 'data_YYYYMMDD_HHMMSS'
    for folder in base_path.parent.glob('data_*'):
        if not folder.is_dir() or folder.name == 'data' or folder.name.startswith('data_new_'):
            continue
            
        try:
            # Parse folder timestamp from name
            timestamp_str = folder.name.replace('data_', '')
            folder_time = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
            folder_time = folder_time.replace(tzinfo=timezone.utc)
            
            # Delete if older than max_age_hours
            if (current_time - folder_time) > timedelta(hours=max_age_hours):
                # Check if folder is empty or has a sync marker
                is_empty = True
                has_data = False
                for p in folder.glob('**/*'):
                    if p.is_file() and not p.name.startswith('.'):
                        has_data = True
                        is_empty = False
                        break
                
                if is_empty or not has_data:
                    shutil.rmtree(folder)
                    deleted_count += 1
                    logger.info(f"Deleted old folder: {folder.name}")
                else:
                    skipped_count += 1
                    logger.warning(f"Skipped non-empty folder: {folder.name} (might contain unsynced data)")
            
        except (ValueError, OSError) as e:
            logger.error(f"Error processing folder {folder.name}: {e}")
            
    if deleted_count > 0 or skipped_count > 0:
        logger.info(f"Folder cleanup: deleted {deleted_count}, skipped {skipped_count} folders")

async def main():
    store = OptionsDataStore()
    conn_manager = ConnectionManager()
    last_synced_day = None
    
    while True:
        try:
            if conn_manager.should_reset_connection():
                logger.info("Reinitializing connection...")
                await store.close()
                await asyncio.sleep(5)  # Brief pause before reconnecting
                store = OptionsDataStore()
                await store.initialize()

            if await is_maintenance_window():
                logger.warning("Scheduled maintenance window detected. Pausing data collection for 5 minutes.")
                await asyncio.sleep(300)
                continue

            try:
                await store.run_cycle()
                conn_manager.connection_succeeded()
            except Exception as e:
                logger.error(f"Error during run cycle: {e}")
                delay = conn_manager.connection_failed()
                logger.warning(f"Waiting {delay} seconds before retry (attempt {conn_manager.consecutive_failures})")
                await asyncio.sleep(delay)
                continue
            
            # Check if a new day is reached (at UTC midnight)
            now = datetime.now(timezone.utc)
            # Using consistent second=0 for comparison
            current_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
            if last_synced_day is None or current_day > last_synced_day:
                logger.info("New day reached: rotating data folder for S3 sync.")
                rotated_folder = store.rotate_data_directory()
                if rotated_folder:
                    logger.info("Initiating S3 sync and cleanup on rotated folder.")
                    sync_success = store.s3_sync_and_cleanup(folder=rotated_folder)
                    if sync_success:
                        logger.info("S3 sync completed successfully. Running folder cleanup.")
                        cleanup_old_folders(store.base_path)
                    else:
                        logger.warning("S3 sync had issues. Skipping folder cleanup to prevent data loss.")
                last_synced_day = current_day

        except Exception as e:
            logger.error(f"Critical error in main loop: {e}")
            delay = conn_manager.connection_failed()
            await asyncio.sleep(delay)
        finally:
            if not conn_manager.should_reset_connection():
                await store.close()

if __name__ == "__main__":
    asyncio.run(main())