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
    def __init__(self, config_path: str = os.getenv('EC2_CONFIG_PATH', '/etc/options-collector/config.yaml')):
        super().__init__(config_path)
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
            # Calculate the hour we're aggregating
            hour_str = hour_datetime.strftime('%Y/%m/%d/%H')
            base_path = os.path.join(self.config['temp_data_path'], hour_str)
            
            logger.info(f"Starting hourly aggregation for {hour_str}")
            
            # Find all coin directories
            coin_dirs = [d for d in glob.glob(os.path.join(base_path, "coin=*")) if os.path.isdir(d)]
            
            for coin_dir in coin_dirs:
                coin = os.path.basename(coin_dir).replace('coin=', '')
                logger.info(f"Aggregating data for {coin}")
                
                # Read all parquet files for this coin-hour
                parquet_files = glob.glob(os.path.join(coin_dir, "*.parquet"))
                if not parquet_files:
                    logger.warning(f"No parquet files found for {coin} in {hour_str}")
                    continue
                
                # Read and concatenate all files
                dfs = []
                for pf in parquet_files:
                    try:
                        df = pd.read_parquet(pf)
                        dfs.append(df)
                    except Exception as e:
                        logger.error(f"Error reading {pf}: {e}")
                
                if not dfs:
                    continue
                
                # Combine all minute snapshots
                hourly_df = pd.concat(dfs, ignore_index=True)
                
                # Create hourly aggregated file
                hourly_path = os.path.join(
                    self.config['temp_data_path'],
                    'hourly',
                    hour_str,
                    f"coin={coin}"
                )
                os.makedirs(hourly_path, exist_ok=True)
                
                # Write hourly parquet file
                hourly_file = os.path.join(hourly_path, "hourly_data.parquet")
                hourly_df.to_parquet(hourly_file, compression='snappy')
                
                # Upload to S3
                s3_key_prefix = os.path.join(
                    self.config['s3_prefix'],
                    'hourly',
                    hour_str
                )
                
                success, _ = self.s3_uploader.upload_to_s3(
                    local_path=hourly_path,
                    s3_bucket=self.config['s3_bucket'],
                    s3_key_prefix=s3_key_prefix,
                    delete_local=True
                )
                
                if success:
                    logger.info(f"Successfully uploaded hourly data for {coin} - {hour_str}")
                    # Clean up minute-level files
                    for pf in parquet_files:
                        try:
                            os.remove(pf)
                        except OSError as e:
                            logger.warning(f"Error removing minute file {pf}: {e}")
                else:
                    logger.error(f"Failed to upload hourly data for {coin} - {hour_str}")
            
            return True
            
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
                # 1. Calculate target time for next minute
                now = datetime.now(timezone.utc)
                next_minute = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
                wait_time = (next_minute - now).total_seconds()
                
                # 2. Wait precisely until next minute
                logger.debug(f"Waiting {wait_time:.3f} seconds until {next_minute.strftime('%H:%M:%S')}")
                await asyncio.sleep(wait_time)
                
                # 3. Take snapshot immediately at minute start
                start_time = datetime.now(timezone.utc)
                success = await self.process_snapshot()
                
                if success:
                    process_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                    logger.debug(f"Snapshot processing took {process_time:.3f} seconds")
                    
                    # 4. After successful snapshot, check if we need to aggregate
                    # (we're at the start of an hour)
                    if (start_time.minute == 0 and 
                        (self.last_aggregation_hour is None or 
                         start_time.replace(minute=0, second=0, microsecond=0) > self.last_aggregation_hour)):
                        # Aggregate previous hour
                        prev_hour = start_time.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
                        await self.aggregate_and_upload_hourly(prev_hour)
                        self.last_aggregation_hour = start_time.replace(minute=0, second=0, microsecond=0)
                
                # 5. Check for instrument refresh (if needed)
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

    config_path = os.getenv('EC2_CONFIG_PATH', '/etc/options-collector/config.yaml')
    if not os.path.exists(config_path):
        default_config = {
            'snapshot_interval': int(os.getenv('SNAPSHOT_INTERVAL', 3600)),  # 1 hour in production
            'temp_data_path': os.getenv('EC2_TEMP_PATH', '/tmp/options-collector'),
            's3_bucket': os.getenv('S3_BUCKET', 'your-production-bucket'),
            's3_prefix': os.getenv('S3_PREFIX', 'options-data'),
            'max_channels_per_conn': int(os.getenv('MAX_CHANNELS_PER_CONN', 500)),
            'heartbeat_interval': int(os.getenv('HEARTBEAT_INTERVAL', 30)),
        }
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        with open(config_path, 'w') as f:
            yaml.dump(default_config, f)

if __name__ == "__main__":
    setup_ec2_environment()
    collector = EC2OptionsDataCollector()
    try:
        asyncio.run(collector.run())
    except KeyboardInterrupt:
        logger.info("Collector stopped by user")
