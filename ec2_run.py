from dotenv import load_dotenv

# Load environment variables first, before any other imports or configuration
load_dotenv()

import asyncio
import logging
import os
from datetime import datetime, time, timezone, timedelta
import yaml
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import glob

from main import OptionsDataCollector
from instrument_fetcher import fetch_option_instruments

# Configure logging after env vars are loaded
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=os.getenv('EC2_LOG_PATH', '/var/log/options-collector/collector.log')  # Log to file on EC2
)
logger = logging.getLogger(__name__)

# Add diagnostic prints
logger.info(f"S3_BUCKET from environment: {os.getenv('S3_BUCKET')}")
logger.info(f"Current working directory: {os.getcwd()}")
logger.info(f".env file location: {os.path.abspath('.env')}")

class EC2OptionsDataCollector(OptionsDataCollector):
    def __init__(self):  # Remove config_path parameter
        super().__init__()  # Remove config_path parameter
        self.last_refresh_date = None
        self.last_aggregation_hour = None

    async def check_instrument_refresh(self):
        """Check if we need to refresh instruments."""
        now = datetime.now(timezone.utc)
        refresh_hour = int(os.getenv('DAILY_REFRESH_HOUR', 8))
        target_time = time(hour=refresh_hour, minute=0)  # Refresh time from environment variable
        
        # Check if it's past the refresh time and we haven't refreshed today
        if (now.time() >= target_time and 
            (self.last_refresh_date is None or 
             now.date() > self.last_refresh_date)):
            
            logger.info("Starting daily instrument refresh...")
            try:
                # Stop current manager
                if self.manager:
                    self.manager.stop()
                
                # Fetch fresh instruments and reinitialize
                await self.initialize()
                self.last_refresh_date = now.date()
                logger.info("Daily instrument refresh completed successfully")
            except Exception as e:
                logger.exception("Failed to refresh instruments: %s", e)

    async def aggregate_and_upload_hourly(self, hour_datetime: datetime) -> bool:
        """Aggregate the last hour's minute-level files into hourly files and upload to S3."""
        try:
            # Base path and hour string
            base_path = self.config['temp_data_path']
            hour_str = hour_datetime.strftime('%Y-%m-%d/%H')  # For S3 path
            
            logger.info(f"Starting hourly aggregation for {hour_str}")
            
            # First level: coin directories
            success = True
            coin_dirs = glob.glob(os.path.join(base_path, "coin=*"))
            
            if not coin_dirs:
                logger.error(f"No coin directories found in {base_path}")
                return False
            
            logger.info(f"Found {len(coin_dirs)} coins to process")
            
            for coin_dir in coin_dirs:
                try:
                    coin = os.path.basename(coin_dir).replace('coin=', '')
                    logger.info(f"Processing {coin}")
                    
                    # Find all parquet files for this coin and hour
                    date_path = os.path.join(coin_dir, f"date={hour_datetime.strftime('%Y-%m-%d')}")
                    if not os.path.exists(date_path):
                        logger.warning(f"No data for {coin} on {hour_datetime.strftime('%Y-%m-%d')}")
                        continue
                    
                    # Collect all files from all expiry dates for this hour
                    all_parquet_files = []
                    for expiry_dir in glob.glob(os.path.join(date_path, "expiry=*")):
                        hour_path = os.path.join(expiry_dir, f"hour={hour_datetime.strftime('%H')}")
                        if os.path.exists(hour_path):
                            parquet_files = glob.glob(os.path.join(hour_path, "*.parquet"))
                            all_parquet_files.extend(parquet_files)
                    
                    if not all_parquet_files:
                        logger.warning(f"No parquet files found for {coin} in hour {hour_str}")
                        continue
                    
                    logger.info(f"Found {len(all_parquet_files)} files for {coin}")
                    
                    # Read and concatenate all files
                    dfs = []
                    for pf in all_parquet_files:
                        try:
                            df = pd.read_parquet(pf)
                            dfs.append(df)
                        except Exception as e:
                            logger.error(f"Error reading {pf}: {e}")
                    
                    if not dfs:
                        continue
                    
                    # Combine all snapshots
                    hourly_df = pd.concat(dfs, ignore_index=True)
                    
                    # Write hourly file preserving the structure
                    hourly_base = os.path.join(base_path, 'hourly')
                    hourly_path = os.path.join(
                        hourly_base,
                        f"coin={coin}",
                        f"date={hour_datetime.strftime('%Y-%m-%d')}",
                        f"hour={hour_datetime.strftime('%H')}"
                    )
                    os.makedirs(hourly_path, exist_ok=True)
                    
                    hourly_file = os.path.join(hourly_path, f"{coin}_hourly.parquet")
                    hourly_df.to_parquet(hourly_file, compression='snappy')
                    
                    # Upload to S3 with consistent structure
                    s3_key_prefix = os.path.join(
                        self.config['s3_prefix'],
                        'hourly',
                        f"coin={coin}",
                        f"date={hour_datetime.strftime('%Y-%m-%d')}",
                        f"hour={hour_datetime.strftime('%H')}"
                    ).replace('\\', '/')  # Ensure forward slashes for S3
                    
                    upload_success, _ = self.s3_uploader.upload_to_s3(
                        local_path=hourly_file,  # Changed from hourly_path to hourly_file
                        s3_bucket=self.config['s3_bucket'],
                        s3_key_prefix=s3_key_prefix,
                        delete_local=True
                    )
                    
                    if upload_success:
                        logger.info(f"Successfully uploaded hourly data for {coin}")
                        # Clean up minute files only after successful upload
                        for pf in all_parquet_files:
                            try:
                                os.remove(pf)
                                # Remove empty directories
                                dir_path = os.path.dirname(pf)
                                while dir_path > base_path:
                                    if not os.listdir(dir_path):
                                        os.rmdir(dir_path)
                                        dir_path = os.path.dirname(dir_path)
                                    else:
                                        break
                            except OSError as e:
                                logger.warning(f"Error cleaning up {pf}: {e}")
                    else:
                        logger.error(f"Failed to upload hourly data for {coin}")
                        success = False
                        
                except Exception as e:
                    logger.exception(f"Error processing {coin}: {e}")
                    success = False
            
            return success
            
        except Exception as e:
            logger.exception(f"Error during hourly aggregation: {e}")
            return False

    async def run(self):
        """Override run to include hourly aggregation."""
        self.running = True
        
        if not await self.initialize():
            logger.error("Initialization failed, exiting")
            return

        logger.info("Starting main collection loop")
        while self.running:
            try:
                now = datetime.now(timezone.utc)
                next_minute = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
                
                # Check if we're at the start of an hour
                if now.minute == 0 and now.second < 30:
                    # Always aggregate the previous hour at XX:00
                    prev_hour = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
                    logger.info(f"Starting hourly aggregation at {now} for hour {prev_hour}")
                    await self.aggregate_and_upload_hourly(prev_hour)
                
                # Wait for next minute
                wait_time = (next_minute - now).total_seconds()
                logger.debug(f"Waiting {wait_time:.3f} seconds until {next_minute.strftime('%H:%M:%S')}")
                await asyncio.sleep(wait_time)
                
                # Take snapshot
                start_time = datetime.now(timezone.utc)
                success = await self.process_snapshot()
                
                if success:
                    process_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                    logger.debug(f"Snapshot processing took {process_time:.3f} seconds")
                
                await self.check_instrument_refresh()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Error in main loop: %s", e)
                await asyncio.sleep(5)

        # Cleanup
        if self.manager:
            self.manager.stop()
        logger.info("Collector stopped")

def setup_ec2_environment():
    """Ensure EC2 environment is properly set up."""
    # Create necessary directories
    os.makedirs(os.getenv('EC2_LOG_PATH', '/var/log/options-collector'), exist_ok=True)
    os.makedirs(os.getenv('EC2_TEMP_PATH', '/tmp/options-collector'), exist_ok=True)

if __name__ == "__main__":
    setup_ec2_environment()
    collector = EC2OptionsDataCollector()
    try:
        asyncio.run(collector.run())
    except KeyboardInterrupt:
        logger.info("Collector stopped by user")
