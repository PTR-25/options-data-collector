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
import shutil
from pathlib import Path

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
        self.last_aggregation_hour = None

    async def check_instrument_refresh(self):
        """
        Check if we need to refresh instruments at the configured daily hour (e.g., 08:00 UTC).
        This method will attempt to refresh instruments indefinitely until successful if it's
        the designated refresh window.
        """
        now = datetime.now(timezone.utc)
        refresh_hour = int(os.getenv('DAILY_REFRESH_HOUR', 8))

        logger.info(f"Checking for instrument refresh at {now.isoformat()} UTC. Target refresh hour: {refresh_hour} UTC.")

        # Determine if it's the refresh window (top of the configured hour)
        is_refresh_trigger_time = (now.hour == refresh_hour and now.minute == 0)

        if is_refresh_trigger_time:
            logger.info(f"Daily instrument refresh window detected at {now.isoformat()} UTC. Starting refresh process (will retry until successful).")
            
            retry_delay_seconds = int(os.getenv('INSTRUMENT_REFRESH_RETRY_DELAY_SECONDS', 60))
            attempt = 0

            while True:  # Loop indefinitely until success
                attempt += 1
                logger.info(f"Instrument Refresh: Attempt {attempt}...")
                
                current_utc_time_for_log = datetime.now(timezone.utc).isoformat()
                try:
                    # 1) stop existing websockets cleanly
                    if self.manager:
                        logger.info(f"Instrument Refresh (Attempt {attempt}): Stopping existing WsManager...")
                        await self.manager.stop()
                        logger.info(f"Instrument Refresh (Attempt {attempt}): Existing WsManager stopped successfully.")
                    else:
                        logger.info(f"Instrument Refresh (Attempt {attempt}): No existing WsManager to stop.")
                    
                    # 2) fetch new channels list
                    logger.info(f"Instrument Refresh (Attempt {attempt}): Fetching new instruments list…")
                    new_channels = await fetch_option_instruments()

                    # 3) diff + in-place subscribe/unsubscribe
                    logger.info(f"Instrument Refresh (Attempt {attempt}): Resubscribing channels…")
                    await self.manager.resubscribe(new_channels)
                
                    logger.info(f"Instrument Refresh: Attempt {attempt} SUCCEEDED at {datetime.now(timezone.utc).isoformat()} UTC. Instruments are now up-to-date.")
                    return True
                except Exception as e:
                    # This catches exceptions from self.manager.stop() or self.initialize()
                    logger.exception(f"Instrument Refresh: Exception during attempt {attempt} at {current_utc_time_for_log}: {e}. Retrying in {retry_delay_seconds} seconds...")

                await asyncio.sleep(retry_delay_seconds)
                # Loop continues for the next attempt
        else:
            # Log if it's not the refresh time, but only if it's close to the top of an hour to avoid spamming logs
            if now.minute == 0 and now.second < 10: # Log once at the start of other hours
                 logger.info(f"Not the designated instrument refresh hour ({refresh_hour} UTC). Current time: {now.isoformat()} UTC. No refresh attempted.")
            elif now.hour == refresh_hour and now.minute != 0 : # Log if it's the refresh hour but not minute 0
                 logger.info(f"Inside refresh hour ({refresh_hour} UTC) but not minute 0. Current time: {now.isoformat()} UTC. No refresh attempted.")


        return False # Not the refresh window, or refresh process somehow exited (should not happen with indefinite retry)

    async def aggregate_and_upload_hourly(self, hour_datetime: datetime) -> bool:
        """Aggregate the last hour's minute-level files into hourly files and upload to S3."""
        try:
            base_path = self.config['temp_data_path']
            hour_str = hour_datetime.strftime('%Y-%m-%d/%H')
            
            logger.info(f"Starting hourly aggregation for {hour_str}")
            
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
                    
                    date_path = os.path.join(coin_dir, f"date={hour_datetime.strftime('%Y-%m-%d')}")
                    if not os.path.exists(date_path):
                        logger.warning(f"No data for {coin} on {hour_datetime.strftime('%Y-%m-%d')}")
                        continue
                    
                    # Process each expiry separately
                    for expiry_dir in glob.glob(os.path.join(date_path, "expiry=*")):
                        expiry = os.path.basename(expiry_dir).replace('expiry=', '')
                        logger.info(f"Processing expiry {expiry} for {coin}")
                        
                        hour_path = os.path.join(expiry_dir, f"hour={hour_datetime.strftime('%H')}")
                        if not os.path.exists(hour_path):
                            continue
                            
                        parquet_files = glob.glob(os.path.join(hour_path, "*.parquet"))
                        if not parquet_files:
                            continue
                        
                        # Read and concatenate files for this specific expiry
                        dfs = []
                        for pf in parquet_files:
                            try:
                                df = pd.read_parquet(pf)
                                dfs.append(df)
                            except Exception as e:
                                logger.error(f"Error reading {pf}: {e}")
                        
                        if not dfs:
                            continue
                        
                        # Combine snapshots for this expiry
                        hourly_df = pd.concat(dfs, ignore_index=True)
                        
                        # --- Ensure chronological order ---
                        # Sort by 'snapshot_timestamp' to ensure data is in correct time order
                        # This is crucial as glob.glob doesn't guarantee file order
                        if 'snapshot_timestamp' in hourly_df.columns:
                            hourly_df = hourly_df.sort_values(by='snapshot_timestamp').reset_index(drop=True)
                            logger.debug(f"Hourly DataFrame for {coin} expiry {expiry} sorted by snapshot_timestamp.")
                        else:
                            logger.warning(f"'snapshot_timestamp' column not found in hourly DataFrame for {coin} expiry {expiry}. Cannot sort.")
                        # --- End chronological order ---
                        
                        # Write hourly file preserving full structure
                        hourly_base = os.path.join(base_path, 'hourly')
                        hourly_path = os.path.join(
                            hourly_base,
                            f"coin={coin}",
                            f"date={hour_datetime.strftime('%Y-%m-%d')}",
                            f"expiry={expiry}",
                            f"hour={hour_datetime.strftime('%H')}"
                        )
                        os.makedirs(hourly_path, exist_ok=True)
                        
                        hourly_file = os.path.join(hourly_path, f"{coin}_{hour_datetime.strftime('%Y-%m-%d')}_{expiry}_{hour_datetime.strftime('%H')}.parquet")
                        hourly_df.to_parquet(hourly_file, compression='snappy')
                        
                        # Upload to S3 maintaining full structure
                        s3_key_prefix = os.path.join(
                            self.config['s3_prefix'],
                            'hourly',
                            f"coin={coin}",
                            f"date={hour_datetime.strftime('%Y-%m-%d')}",
                            f"expiry={expiry}",
                            f"hour={hour_datetime.strftime('%H')}"
                        ).replace('\\', '/')
                        
                        upload_success, _ = self.s3_uploader.upload_to_s3(
                            local_path=hourly_file,
                            s3_bucket=self.config['s3_bucket'],
                            s3_key_prefix=s3_key_prefix,
                            delete_local=True
                        )
                        
                        if upload_success:
                            logger.info(f"Successfully uploaded hourly data for {coin} expiry {expiry}")
                            # Clean up minute files for this expiry
                            for pf in parquet_files:
                                try:
                                    os.remove(pf)
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
                            logger.error(f"Failed to upload hourly data for {coin} expiry {expiry}")
                            success = False
                        
                except Exception as e:
                    logger.exception(f"Error processing {coin}: {e}")
                    success = False
            
            return success
            
        except Exception as e:
            logger.exception(f"Error during hourly aggregation: {e}")
            return False

    async def cleanup_old_data(self, max_age_days: int = 3) -> None:
        """Remove local data older than specified days."""
        try:
            base_path = Path(self.config['temp_data_path'])
            now = datetime.now(timezone.utc)
            cutoff_date = now - timedelta(days=max_age_days)
            
            logger.info(f"Cleaning up data older than {max_age_days} days ({cutoff_date.date()})")
            
            # Check both regular and hourly data directories
            data_dirs = [base_path, base_path / 'hourly']
            
            for data_dir in data_dirs:
                if not data_dir.exists():
                    continue
                    
                # Walk through coin directories
                for coin_dir in data_dir.glob('coin=*'):
                    # Check date directories
                    for date_dir in coin_dir.glob('date=*'):
                        try:
                            # Extract date from directory name
                            date_str = date_dir.name.replace('date=', '')
                            dir_date = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                            
                            # Remove if older than cutoff
                            if dir_date.date() < cutoff_date.date():
                                logger.info(f"Removing old data: {date_dir}")
                                shutil.rmtree(date_dir)
                                
                        except (ValueError, OSError) as e:
                            logger.error(f"Error processing directory {date_dir}: {e}")
                    
                    # Clean up empty coin directories
                    if not any(coin_dir.iterdir()):
                        coin_dir.rmdir()
                        
            logger.info("Data cleanup completed")
            
        except Exception as e:
            logger.exception(f"Error during data cleanup: {e}")

    async def run(self):
        """Override run to include data cleanup."""
        self.running = True
        
        if not await self.initialize():
            logger.error("Initialization failed, exiting")
            return

        logger.info("Starting main collection loop")
        while self.running:
            try:
                # --- Wait for next minute ---
                now = datetime.now(timezone.utc)
                next_minute = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
                wait_time = (next_minute - now).total_seconds()
                logger.debug(f"Waiting {wait_time:.3f} seconds until {next_minute.strftime('%H:%M:%S')}")
                await asyncio.sleep(wait_time)
                # --- End Wait ---
                
                # --- Take snapshot ---
                # This now happens accurately at the start of the minute (e.g., XX:01:00)
                start_time = datetime.now(timezone.utc)
                success = await self.process_snapshot()
                
                if success:
                    process_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                    logger.debug(f"Snapshot processing took {process_time:.3f} seconds")
                # --- End Snapshot ---
                
                now = datetime.now(timezone.utc)
                # Daily cleanup at a specific hour (e.g., 07:00 UTC, before instrument refresh)
                if now.hour == 7 and now.minute == 0 and now.second < 50:
                    await self.cleanup_old_data()
                
                # Check if we're at the start of an hour for aggregation
                if now.minute == 00 and now.second > 30:
                    # Always aggregate the previous hour at HH+1:00:30
                    prev_hour = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
                    logger.info(f"Starting hourly aggregation at {now} for hour {prev_hour}")
                    await self.aggregate_and_upload_hourly(prev_hour)
                                
                # --- Check for refresh ---
                # Special handling for 8 AM UTC - wait until seconds = 50 before refresh
                now = datetime.now(timezone.utc)  # Get fresh timestamp
                if now.hour == 8 and now.minute == 0:
                    if now.second < 50:
                        wait_seconds = 50 - now.second
                        logger.info(f"It's 8 AM UTC, waiting {wait_seconds} seconds before instrument refresh...")
                        await asyncio.sleep(wait_seconds)
                    logger.info("Now checking for instrument refresh at 8 AM with seconds=50")
                    await self.check_instrument_refresh()
                else:
                    # Normal refresh check for other hours
                    await self.check_instrument_refresh()
                # --- End Refresh Check ---
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Error in main loop: %s", e)
                # On error, wait a few seconds then align with next minute
                await asyncio.sleep(5)

        # Cleanup
        if self.manager:
            await self.manager.stop()  # Change to await the stop call
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
