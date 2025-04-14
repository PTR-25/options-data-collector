import asyncio
import logging
import os
from pathlib import Path
from datetime import datetime, timezone
from store_options import OptionsDataStore

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def validate_data_directory(base_path: Path) -> dict:
    """Validate that data is being saved correctly"""
    stats = {"total_files": 0, "total_size": 0, "instruments": set()}
    
    if not base_path.exists():
        logger.error(f"Data directory does not exist: {base_path}")
        return stats
        
    for currency_folder in base_path.glob("*"):
        if not currency_folder.is_dir():
            continue
            
        for expiry_type in currency_folder.glob("*"):
            if not expiry_type.is_dir():
                continue
                
            for expiry_folder in expiry_type.glob("*"):
                if not expiry_folder.is_dir():
                    continue
                    
                for csv_file in expiry_folder.glob("*.csv"):
                    stats["total_files"] += 1
                    stats["total_size"] += csv_file.stat().st_size
                    stats["instruments"].add(csv_file.stem)
                    
    
    return stats

async def main():
    # Set up local data path if not already defined in .env
    local_data_path = Path("./data").absolute()
    os.environ['BASE_SAVE_PATH'] = str(local_data_path)
    logger.info(f"Using local data path: {local_data_path}")
    
    store = OptionsDataStore()
    
    logger.info("Starting data collection in local mode")
    logger.info("Press Ctrl+C to stop the collection process")
    
    try:
        start_time = datetime.now(timezone.utc)
        await store.initialize()
        cycles_completed = 0
        last_validation_time = start_time
        
        while True:
            cycles_completed += 1
            logger.info(f"Starting collection cycle #{cycles_completed}")
            
            # Run the cycle
            await store.run_cycle()
            
            # Log buffer statistics
            buffer_stats = {curr: len(buff) for curr, buff in store.data_buffer.buffer.items()}
            logger.info(f"Buffer state: {buffer_stats}")
            
            # Validate data saving every 5 minutes
            now = datetime.now(timezone.utc)
            if (now - last_validation_time).total_seconds() >= 300:  # 5 minutes
                logger.info("Performing data directory validation...")
                stats = validate_data_directory(local_data_path)
                logger.info(f"Data directory stats: {stats['total_files']} files, "
                          f"{stats['total_size']/1024:.2f} KB total, "
                          f"{len(stats['instruments'])} unique instruments")
                last_validation_time = now
            
            runtime = now - start_time
            logger.info(f"Completed cycle #{cycles_completed}, Total runtime: {runtime}")
            
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt detected. Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Error in main loop: {e}", exc_info=True)
    finally:
        logger.info("Performing final data validation before shutdown...")
        final_stats = validate_data_directory(local_data_path)
        logger.info(f"Final data directory stats: {final_stats['total_files']} files, "
                   f"{final_stats['total_size']/1024:.2f} KB total, "
                   f"{len(final_stats['instruments'])} unique instruments")
        
        logger.info("Closing data store and saving remaining data...")
        await store.close()
        
        # Validate one last time after close
        post_close_stats = validate_data_directory(local_data_path)
        logger.info(f"Post-close data directory stats: {post_close_stats['total_files']} files, "
                   f"{post_close_stats['total_size']/1024:.2f} KB total, "
                   f"{len(post_close_stats['instruments'])} unique instruments")
        
        logger.info("Data collection completed")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process terminated by user")
