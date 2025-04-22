from dotenv import load_dotenv

# Load environment variables first
load_dotenv()

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
import math
import signal
from typing import Optional

from instrument_fetcher import fetch_option_instruments
from snapshot_manager import process_and_write_snapshot
from s3_uploader import S3Uploader
from ws import WsManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OptionsDataCollector:
    """Main orchestrator for the options data collection pipeline."""
    
    def __init__(self):
        self.config = self._load_config()
        self.manager: Optional[WsManager] = None
        self.s3_uploader = S3Uploader()
        self.running = False
        self._setup_signal_handlers()

    def _load_config(self) -> dict:
        """Load configuration from environment variables."""
        try:
            # Clean and parse environment variables
            snapshot_interval = os.getenv('SNAPSHOT_INTERVAL', '3600')
            if '#' in snapshot_interval:  # Handle comments in env vars
                snapshot_interval = snapshot_interval.split('#')[0].strip()
            
            config = {
                'snapshot_interval': int(snapshot_interval),
                'temp_data_path': os.getenv('LOCAL_DATA_PATH', './temp_data'),
                's3_bucket': os.getenv('S3_BUCKET'),
                's3_prefix': os.getenv('S3_PREFIX', 'options-data'),
                'max_channels_per_conn': int(os.getenv('MAX_CHANNELS_PER_CONN', '500')),
                'heartbeat_interval': int(os.getenv('HEARTBEAT_INTERVAL', '30')),
            }
            
            # Validate required environment variables
            if not config['s3_bucket']:
                raise ValueError("S3_BUCKET environment variable must be set")
                
            return config
            
        except ValueError as e:
            logger.error(f"Error parsing environment variables: {e}")
            logger.error("Please check your .env file for correct numeric values without comments")
            raise

    def _setup_signal_handlers(self):
        """Setup handlers for graceful shutdown."""
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals."""
        logger.info("Shutdown signal received, stopping collector...")
        self.running = False

    async def initialize(self):
        """Initialize WebSocket manager with fetched instruments."""
        try:
            logger.info("Fetching option instruments...")
            channels = await fetch_option_instruments()
            
            if not channels:
                raise RuntimeError("No option instruments found")
            
            logger.info(f"Initializing WebSocket manager with {len(channels)} channels")
            self.manager = WsManager(
                all_channels=channels,
                max_channels_per_conn=self.config['max_channels_per_conn'],
                heartbeat_interval=self.config['heartbeat_interval']
            )
            self.manager.start()
            logger.info("WebSocket manager started successfully")
            return True
            
        except Exception as e:
            logger.exception("Failed to initialize: %s", e)
            return False

    async def process_snapshot(self):
        """Process a snapshot and save locally."""
        try:
            snapshot = self.manager.pop_snapshot()
            if not snapshot:
                logger.warning("Empty snapshot, skipping processing")
                return False

            # Process and write to local Parquet
            timestamp = datetime.now(timezone.utc)
            # Base path only, partitioning will handle the rest
            local_path = self.config['temp_data_path']
            
            result_path = process_and_write_snapshot(
                snapshot_data=snapshot,
                timestamp=timestamp,
                base_path=local_path
            )
            
            if not result_path:
                logger.error("Failed to process and write snapshot")
                return False

            return True

        except Exception as e:
            logger.exception("Error processing snapshot: %s", e)
            return False

    async def run(self):
        """Main run loop."""
        self.running = True
        
        if not await self.initialize():
            logger.error("Initialization failed, exiting")
            return

        logger.info("Starting main collection loop")
        while self.running:
            try:
                # First, calculate the target time for the next minute
                now = datetime.now(timezone.utc)
                next_minute = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
                
                # Calculate and wait the exact time until next minute
                wait_time = (next_minute - now).total_seconds()
                logger.debug(f"Waiting {wait_time:.3f} seconds until {next_minute.strftime('%H:%M:%S')}")
                await asyncio.sleep(wait_time)
                
                # After wait, immediately start the snapshot process
                start_time = datetime.now(timezone.utc)
                success = await self.process_snapshot()
                
                if not success:
                    logger.warning("Failed to process snapshot, will retry next minute")
                else:
                    process_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                    logger.debug(f"Snapshot processing took {process_time:.3f} seconds")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Error in main loop: %s", e)
                # On error, wait a few seconds then align with next minute
                await asyncio.sleep(5)

        # Cleanup
        if self.manager:
            self.manager.stop()
        logger.info("Collector stopped")

if __name__ == "__main__":
    collector = OptionsDataCollector()
    try:
        asyncio.run(collector.run())
    except KeyboardInterrupt:
        pass
