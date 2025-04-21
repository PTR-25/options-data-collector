import asyncio
import json
import logging
import sys
import time
from collections import defaultdict # Keep for potential future use if needed

import pytest

# We will use the actual ws module and its configured WS_URL
import ws as ws_mod
from ws import WsManager

# Configure logging for the test
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(threadName)s %(message)s")
logger = logging.getLogger(__name__)

# --- Configuration for Live Test ---
# Ensure your .env file has valid CLIENT_ID_1 and CLIENT_SECRET_1
# These intervals should be reasonable for a live test
HEARTBEAT_INTERVAL_SEC = 15 # How often the client asks the server to send heartbeats
RUN_TIME_SEC = HEARTBEAT_INTERVAL_SEC * 4 + 5 # Run long enough for at least two heartbeats

@pytest.mark.asyncio
# @pytest.mark.skip(reason="Requires live Deribit connection and credentials") # Optional: uncomment to skip by default
async def test_ws_heartbeat_integration():
    """
    Verify WsManager connects to Deribit mainnet, authenticates,
    and receives server heartbeats.
    """
    # ---------- Setup ----------
    # Use a small number of real channels (e.g., BTC perpetual and one option)
    # Using too many might slow down the test or hit limits if run frequently
    # Ensure these instruments exist on Deribit mainnet
    channels = [
        #"ticker.BTC-PERPETUAL.100ms",
        # Add a specific, currently active BTC option channel if desired
        "ticker.BTC-25APR25-80000-C.raw" # Example - check Deribit for a valid one
    ]
    if not channels:
        pytest.skip("No channels defined for live test.")

    expected_clients = 1 # Since we have few channels

    # Use a flag or counter to track if the heartbeat callback was triggered
    heartbeat_received = asyncio.Event() # Use an Event for cleaner async waiting/checking

    def on_hb(params):
        logger.info(f"Heartbeat received by callback: {params}")
        # Signal that at least one heartbeat was received
        if not heartbeat_received.is_set():
             heartbeat_received.set() # Set the event the first time

    manager = WsManager(
        all_channels=channels,
        max_channels_per_conn=500, # Standard Deribit limit
        heartbeat_interval=HEARTBEAT_INTERVAL_SEC,
        reconnect_interval=5.0, # Longer reconnect for live test
    )
    manager.register_heartbeat(on_hb)

    logger.info(f"Starting WsManager to connect to {ws_mod.WS_URL}...")
    manager.start() # This starts clients in their own threads

    # Allow time for connection, auth, subscription, and heartbeats
    logger.info(f"Running test for {RUN_TIME_SEC} seconds, waiting for heartbeat...")

    try:
        # Wait for the heartbeat event to be set, with a timeout slightly longer
        # than the expected run time to catch potential issues.
        await asyncio.wait_for(heartbeat_received.wait(), timeout=RUN_TIME_SEC + 5)
        logger.info("Heartbeat successfully received.")
    except asyncio.TimeoutError:
        logger.error("Timeout waiting for heartbeat callback.")
        # Optionally fail the test immediately if no heartbeat is received
        pytest.fail(f"Did not receive any heartbeat within {RUN_TIME_SEC + 5} seconds.")
    finally:
        logger.info(f"Ran for test duration. Stopping WsManager...")
        manager.stop()
        logger.info("WsManager stop signaled.")
        # Give threads time to shut down gracefully - may need adjustment
        await asyncio.sleep(5)
        logger.info("WsManager likely stopped.")


    # ---------- Assertions ----------
    assert len(manager.clients) == expected_clients, \
        f"Expected {expected_clients} WsManager clients, found {len(manager.clients)}"

    # The primary check is whether the heartbeat_received event was set,
    # which is handled by the try/except asyncio.TimeoutError block above.
    # We can add an explicit assertion for clarity if preferred:
    assert heartbeat_received.is_set(), "Heartbeat callback was not triggered."

    # Optional: Check cache - less reliable in short live test, depends on market activity
    # latest_data = manager.get_latest()
    # logger.info(f"Final cache size: {len(latest_data)}")
    # assert len(latest_data) > 0, "Cache should contain some data after running."


if __name__ == "__main__":
    # NOTE: Ensure you have a .env file with valid CLIENT_ID_1 and CLIENT_SECRET_1
    # in the same directory or accessible in your environment.
    print("--- Running Deribit WebSocket Heartbeat Integration Test ---")
    print(f"--- Connecting to: {ws_mod.WS_URL} ---")
    print(f"--- Test Duration: ~{RUN_TIME_SEC} seconds ---")
    print("--- Ensure CLIENT_ID_1 and CLIENT_SECRET_1 are set in .env ---")

    try:
        logger.info("Running heartbeat integration test...")
        asyncio.run(test_ws_heartbeat_integration())
        logger.info("Heartbeat integration test passed.")
        print("\n--- Heartbeat integration test passed. ---")
    except pytest.fail.Exception as e: # Catch pytest.fail exceptions
         logger.error(f"Test failed: {e}")
         print(f"\n--- Test failed: {e} ---")
         sys.exit(1)
    except AssertionError as e:
        logger.error(f"Assertion failed: {e}", exc_info=True)
        print(f"\n--- Assertion failed: {e} ---")
        sys.exit(1)
    except Exception as e:
        # Catch potential errors during connection, auth, etc.
        logger.exception("An unexpected error occurred during the test.")
        print(f"\n--- Error during execution: {e} ---")
        sys.exit(1)
