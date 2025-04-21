import asyncio
import logging
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

from main import OptionsDataCollector
from snapshot_manager import process_and_write_snapshot  # Add this import

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class LocalOptionsDataCollector(OptionsDataCollector):
    def __init__(self):
        # Override config with local settings from env
        local_config = {
            'snapshot_interval': int(os.getenv('LOCAL_SNAPSHOT_INTERVAL', 60)),
            'temp_data_path': os.getenv('LOCAL_DATA_PATH', './local_data'),
            'max_channels_per_conn': int(os.getenv('MAX_CHANNELS_PER_CONN', 500)),
            'heartbeat_interval': int(os.getenv('HEARTBEAT_INTERVAL', 30)),
        }
        self.config = local_config
        self.manager = None
        self.running = False
        self._setup_signal_handlers()

    async def process_snapshot(self):
        """Override to skip S3 upload."""
        try:
            # Get snapshot
            snapshot = self.manager.pop_snapshot()
            if not snapshot:
                logger.warning("Empty snapshot, skipping processing")
                return False

            # Process and write to local Parquet
            timestamp = datetime.now(timezone.utc)
            local_path = os.path.join(
                self.config['temp_data_path']
            )
            
            result_path = process_and_write_snapshot(
                snapshot_data=snapshot,
                timestamp=timestamp,
                base_path=local_path
            )
            
            if not result_path:
                logger.error("Failed to process and write snapshot")
                return False

            logger.info(f"Successfully saved snapshot to {result_path}")
            return True

        except Exception as e:
            logger.exception("Error processing snapshot: %s", e)
            return False

if __name__ == "__main__":
    # Create data directory
    os.makedirs('./local_data', exist_ok=True)
    
    collector = LocalOptionsDataCollector()
    try:
        asyncio.run(collector.run())
    except KeyboardInterrupt:
        logger.info("Local collection stopped by user")
