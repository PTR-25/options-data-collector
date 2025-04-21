import asyncio
import json
import math
import sys
import pytest
import websockets
import logging

from instrument_fetcher import fetch_option_instruments  # Import the fetcher
from ws import WsManager, WS_URL

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(threadName)s %(message)s")
logger = logging.getLogger(__name__)

# Test configuration
TEST_COINS = ["SOL"]  # Use subset for faster testing
MONITOR_DURATION = 20
MONITOR_INTERVAL = 5

async def monitor_manager(manager, duration=MONITOR_DURATION, interval=MONITOR_INTERVAL):
    """For a fixed duration, every interval seconds, print and verify stats."""
    for elapsed in range(interval, duration + 1, interval):
        await asyncio.sleep(interval)
        latest = manager.get_latest()
        count = len(latest)
        logger.info(f"[{elapsed}s] Fetched ticks for {count} options")
        if latest:
            ch, payload = next(iter(latest.items()))
            logger.info(f"Sample from {ch}: {payload.get('mark_price', 'N/A')}")
    logger.info("Monitoring complete.")

@pytest.mark.asyncio
async def test_ws_manager_integration():
    """Test WsManager with multiple coins using instrument_fetcher."""
    
    # Fetch instruments for all test coins
    logger.info(f"Fetching instruments for coins: {TEST_COINS}")
    channels = await fetch_option_instruments(TEST_COINS)
    
    if not channels:
        pytest.skip("No instruments fetched from API")
    
    total = len(channels)
    logger.info(f"Fetched {total} channels across {len(TEST_COINS)} coins")
    
    # Initialize manager
    manager = WsManager(all_channels=channels, max_channels_per_conn=500)
    expected_clients = math.ceil(total / manager.batch_size)

    # Start all client threads
    logger.info(f"Starting WsManager with {total} channels across {expected_clients} clients...")
    manager.start()

    # Verify correct number of clients spawned
    assert len(manager.clients) == expected_clients, (
        f"Expected {expected_clients} clients, got {len(manager.clients)}"
    )

    # Verify that channels are fully distributed
    distributed = sum(len(c.channels) for c in manager.clients)
    assert distributed == total, (
        f"Channels distributed mismatch: {distributed} vs {total}"
    )

    try:
        # Monitor cache population
        await monitor_manager(manager)

        # Verify cache contains *some* data (exact matches might be flaky in live test)
        latest = manager.get_latest()
        logger.info(f"Final check: Cache contains {len(latest)} entries.")
        
        # Test pop_snapshot functionality
        snapshot = manager.pop_snapshot()
        assert isinstance(snapshot, dict), "pop_snapshot should return a dict"
        assert len(snapshot) > 0, "Snapshot should contain some data"
        assert len(manager.get_latest()) == 0, "Cache should be empty after pop_snapshot"
        
        # Log coverage statistics
        coverage = (len(latest) / total) * 100
        logger.info(f"Data coverage: {coverage:.1f}% ({len(latest)}/{total} channels)")

    finally:
        # Ensure clean shutdown
        logger.info("Stopping WsManager...")
        manager.stop()
        # Give threads time to shut down gracefully
        await asyncio.sleep(5)
        logger.info("WsManager stopped.")

if __name__ == "__main__":
    logger.info(f"Running WsManager Integration Test for {TEST_COINS}")
    try:
        asyncio.run(test_ws_manager_integration())
        logger.info("Integration test completed successfully.")
    except pytest.skip.Exception as e:
        logger.warning(f"Test skipped: {e}")
        sys.exit(0)
    except AssertionError as e:
        logger.error(f"Assertion failed: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        sys.exit(1)
