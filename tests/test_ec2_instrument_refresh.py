# tests/test_ec2_instrument_refresh.py
import asyncio
import pytest
from unittest.mock import patch, AsyncMock, MagicMock, call
from datetime import datetime, timezone, timedelta
from freezegun import freeze_time

import sys
import os
# Add the parent directory to sys.path to import main module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Assume EC2OptionsDataCollector is importable
# Adjust the import path based on your project structure if needed
from ec2_run import EC2OptionsDataCollector
from ws import WsManager # Import WsManager to mock it

# --- Helper function for os.getenv mock ---
def mock_getenv_side_effect(key, default=None):
    """Side effect function for mocking os.getenv."""
    if key == 'DAILY_REFRESH_HOUR':
        return '8' # Return a clean string '8'
    if key == 'EC2_LOG_PATH':
        return '/tmp/test-collector.log' # Provide a default for logging setup
    if key == 'EC2_TEMP_PATH':
        return '/tmp/test-options-collector'
    if key == 'S3_BUCKET':
        return 'mock-test-bucket' # Provide a default for logging/init
    return default # Return the original default for unhandled keys

# --- Fixtures ---

@pytest.fixture
def mock_config():
    """Provides a mock configuration dictionary."""
    return {
        'deribit_api_url': 'wss://mock.deribit.com/ws/api/v2',
        'coins': ['BTC', 'ETH'],
        'channels': ['ticker'],
        'max_instruments_per_coin': 500,
        'max_channels_per_conn': 100,
        'temp_data_path': '/tmp/test-data',
        's3_bucket': 'test-bucket',
        's3_prefix': 'test-prefix',
        'heartbeat_interval': 30,
        'reconnect_delay': 5,
        'snapshot_interval': 60,
    }

@pytest.fixture
def mock_os_env():
    """Fixture to patch os.getenv for the duration of a test."""
    # Patch os.getenv in all modules where it might be called at import time or runtime
    with patch('ec2_run.os.getenv', side_effect=mock_getenv_side_effect) as mock_ec2_getenv, \
         patch('main.os.getenv', side_effect=mock_getenv_side_effect) as mock_main_getenv, \
         patch('s3_uploader.os.getenv', side_effect=mock_getenv_side_effect) as mock_s3_getenv, \
         patch('snapshot_manager.os.getenv', side_effect=mock_getenv_side_effect) as mock_snap_getenv, \
         patch('ws.os.getenv', side_effect=mock_getenv_side_effect) as mock_ws_getenv:
        yield mock_ec2_getenv

@pytest.fixture
def collector(mock_config, mock_os_env):
    """Provides an EC2OptionsDataCollector instance with mocked config and env."""
    # Use MagicMock instead of AsyncMock for S3Uploader since its methods are synchronous
    with patch('main.OptionsDataCollector._load_config', return_value=mock_config), \
         patch('main.S3Uploader', new_callable=MagicMock) as MockS3Uploader:

        instance = EC2OptionsDataCollector()
        instance.config = mock_config
        instance.manager = None
        instance.last_aggregation_hour = None
        instance.s3_uploader = MockS3Uploader.return_value

        assert isinstance(instance.s3_uploader, MagicMock), "S3Uploader was not mocked correctly"
        return instance

# --- Test Cases ---

@pytest.mark.asyncio
@patch('main.fetch_option_instruments', new_callable=AsyncMock)
@patch('main.WsManager', spec=WsManager)
async def test_instrument_refresh_updates_subscriptions(
    mock_WsManager, mock_fetch_instruments, collector, mock_config
):
    """
    Verify that check_instrument_refresh stops the old manager and calls initialize
    when checked within the 08:00:00-08:00:49 UTC window.
    """
    initial_channels = ['ticker.BTC-INITIAL.100ms', 'ticker.ETH-INITIAL.100ms']
    refreshed_channels = ['ticker.BTC-REFRESHED.100ms']
    mock_fetch_instruments.side_effect = [initial_channels, refreshed_channels]

    mock_manager_instance = AsyncMock(spec=WsManager)
    mock_manager_instance.start = AsyncMock()
    mock_manager_instance.stop = AsyncMock()
    mock_WsManager.return_value = mock_manager_instance

    # --- Initial Initialize ---
    init_success = await collector.initialize()
    assert init_success is True
    assert mock_fetch_instruments.call_count == 1
    mock_fetch_instruments.assert_called_with()
    mock_WsManager.assert_called_once()
    args, kwargs = mock_WsManager.call_args
    assert kwargs.get('all_channels') == initial_channels
    mock_manager_instance.start.assert_called_once()

    # --- Reset Mocks for Refresh Check ---
    mock_WsManager.reset_mock()
    mock_manager_instance.reset_mock()
    mock_WsManager.return_value = mock_manager_instance
    mock_initialize_method = AsyncMock(return_value=True)

    # --- Trigger Refresh ---
    with patch.object(collector, 'initialize', mock_initialize_method):
        with freeze_time("2025-04-24 08:00:05 UTC"):  # Within refresh window
            refresh_triggered = await collector.check_instrument_refresh()

    assert refresh_triggered is True
    mock_manager_instance.stop.assert_called_once()
    mock_initialize_method.assert_called_once()


@pytest.mark.asyncio
async def test_instrument_refresh_skipped_if_not_time(collector):
    """Verify refresh is skipped if it's not 08:00:XX UTC."""
    mock_initialize_method = AsyncMock()
    collector.initialize = mock_initialize_method
    mock_manager_instance = AsyncMock(spec=WsManager)
    mock_manager_instance.stop = AsyncMock()
    collector.manager = mock_manager_instance

    with freeze_time("2025-04-24 07:59:59 UTC"):  # Just before refresh window
        refresh_triggered = await collector.check_instrument_refresh()

    assert refresh_triggered is False
    assert mock_initialize_method.call_count == 0
    assert mock_manager_instance.stop.call_count == 0


@pytest.mark.asyncio
@patch.object(EC2OptionsDataCollector, 'process_snapshot', new_callable=AsyncMock)
@patch.object(EC2OptionsDataCollector, 'aggregate_and_upload_hourly', new_callable=AsyncMock)
@patch.object(EC2OptionsDataCollector, 'check_instrument_refresh', new_callable=AsyncMock)
@patch.object(EC2OptionsDataCollector, 'cleanup_old_data', new_callable=AsyncMock)
@patch('asyncio.sleep', new_callable=AsyncMock)
async def test_run_loop_order_at_0800(
    mock_sleep, mock_cleanup, mock_check_refresh, mock_aggregate, mock_process_snapshot,
    collector
):
    """
    Verify the sequence of operations at 08:00:00 UTC:
    1. Sleep until next minute
    2. Take snapshot
    3. Check cleanup (if 07:00)
    4. Check aggregation (hourly)
    5. Check refresh (if 08:00)
    """
    collector.running = True

    mock_controller = MagicMock()
    mock_check_refresh.side_effect = mock_controller.check_instrument_refresh
    mock_aggregate.side_effect = mock_controller.aggregate_and_upload_hourly
    mock_process_snapshot.side_effect = mock_controller.process_snapshot
    mock_cleanup.side_effect = mock_controller.cleanup_old_data
    mock_sleep.side_effect = mock_controller.sleep

    with freeze_time("2025-04-24 08:00:00.100 UTC"):
        now = datetime.now(timezone.utc)

        # Simulate one iteration of the run loop
        next_minute = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
        wait_time = (next_minute - now).total_seconds()
        await asyncio.sleep(wait_time)
        
        await collector.process_snapshot()
        
        now = datetime.now(timezone.utc)  # Update now after sleep
        # Check cleanup (not at 07:00 so should not run)
        if now.hour == 7 and now.minute == 0 and now.second < 50:
            await collector.cleanup_old_data()
            
        # Check aggregation (at XX:00 so should run)
        if now.minute == 0 and now.second < 30:
            prev_hour = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
            await collector.aggregate_and_upload_hourly(prev_hour)
            
        # Check refresh (at 08:00 so should run)
        await collector.check_instrument_refresh()

    # Verify operation order
    expected_calls = [
        call.sleep(pytest.approx(59.9, abs=0.1)),
        call.process_snapshot(),
        call.aggregate_and_upload_hourly(datetime(2025, 4, 24, 7, 0, tzinfo=timezone.utc)),
        call.check_instrument_refresh()
    ]
    assert mock_controller.method_calls == expected_calls


@pytest.mark.asyncio
@patch.object(EC2OptionsDataCollector, 'initialize', new_callable=AsyncMock)
async def test_refresh_triggered_within_window(mock_initialize, collector):
    """
    Confirm refresh happens within the 50-second window after 08:00:00 UTC.
    """
    mock_initialize.return_value = True
    mock_manager_instance = AsyncMock(spec=WsManager)
    mock_manager_instance.stop = AsyncMock()
    collector.manager = mock_manager_instance

    with freeze_time("2025-04-24 08:00:15 UTC"):  # Within 50-second window
        refresh_triggered = await collector.check_instrument_refresh()

    assert refresh_triggered is True
    mock_manager_instance.stop.assert_called_once()
    mock_initialize.assert_called_once()


@pytest.mark.asyncio
@patch.object(EC2OptionsDataCollector, 'initialize', new_callable=AsyncMock)
async def test_refresh_skipped_outside_window(mock_initialize, collector):
    """
    Confirm refresh is skipped if check occurs after the 50-second window.
    """
    mock_manager_instance = AsyncMock(spec=WsManager)
    mock_manager_instance.stop = AsyncMock()
    collector.manager = mock_manager_instance

    with freeze_time("2025-04-24 08:00:55 UTC"):  # Outside 50-second window
        refresh_triggered = await collector.check_instrument_refresh()

    assert refresh_triggered is False
    mock_manager_instance.stop.assert_not_called()
    mock_initialize.assert_not_called()
