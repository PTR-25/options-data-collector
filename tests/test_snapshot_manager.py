import logging
import os
from datetime import datetime
import pytest
import pandas as pd
from snapshot_manager import process_and_write_snapshot, parse_instrument_name

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_real_data_sample(n_samples=2):
    """Get sample data from existing Parquet files."""
    try:
        # Read the real Parquet dataset
        parquet_path = "./local_data/2025/04/21/14"
        df = pd.read_parquet(parquet_path)
        
        # Sample n random instruments
        sample_df = df.sample(n=n_samples)
        
        # Convert to the expected snapshot format
        snapshot_data = {}
        for _, row in sample_df.iterrows():
            channel = f"ticker.{row['instrument_name']}.100ms"
            # Extract all relevant fields from the row
            data = {
                "timestamp": int(pd.Timestamp(row['timestamp']).timestamp() * 1000),
                "state": row['state'],
                "stats": row['stats'],
                "greeks": row['greeks'],
                "index_price": row['index_price'],
                "last_price": row['last_price'],
                "settlement_price": row['settlement_price'],
                "min_price": row['min_price'],
                "max_price": row['max_price'],
                "open_interest": row['open_interest'],
                "mark_price": row['mark_price'],
                "best_ask_price": row['best_ask_price'],
                "best_bid_price": row['best_bid_price'],
                "interest_rate": row['interest_rate'],
                "mark_iv": row['mark_iv'],
                "bid_iv": row['bid_iv'],
                "ask_iv": row['ask_iv'],
                "underlying_price": row['underlying_price'],
                "underlying_index": row['underlying_index'],
                "estimated_delivery_price": row['estimated_delivery_price'],
                "best_ask_amount": row['best_ask_amount'],
                "best_bid_amount": row['best_bid_amount']
            }
            snapshot_data[channel] = data
        
        return snapshot_data
    except Exception as e:
        logger.warning(f"Could not read real data: {e}. Using fallback test data.")
        return None

@pytest.fixture
def sample_snapshot_data():
    # Try to get real data first
    real_data = get_real_data_sample()
    if real_data:
        return real_data
        
    # Fallback to synthetic test data if real data isn't available
    return {
        "ticker.BTC-26SEP25-80000-C.100ms": {
            "timestamp": 1713715200123,
            "mark_price": 0.1234,
            "underlying_price": 50000,
            "volume": 10.5
        },
        "ticker.XRP_USDC-26SEP25-0d625-C.100ms": {
            "timestamp": 1713715200456,
            "mark_price": 0.05,
            "underlying_price": 0.55,
            "volume": 1000
        }
    }

def test_parse_instrument_name():
    # Test standard BTC option
    assert parse_instrument_name("BTC-26SEP25-80000-C") == ("BTC", "2025-09-26", 80000.0, "C")
    
    # Test USDC option with decimal strike
    assert parse_instrument_name("XRP_USDC-26SEP25-0d625-C") == ("XRP", "2025-09-26", 0.625, "C")
    
    # Test invalid formats
    assert parse_instrument_name("invalid-format") is None
    assert parse_instrument_name("BTC-26SEP25-invalid-C") is None

def test_process_and_write_snapshot(sample_snapshot_data, tmp_path):
    timestamp = datetime.now()
    result = process_and_write_snapshot(
        sample_snapshot_data,
        timestamp=timestamp,
        base_path=str(tmp_path)
    )
    
    assert result is not None
    assert os.path.exists(result)
    
    # Verify the data was written correctly
    found_files = []
    for root, _, files in os.walk(result):
        for file in files:
            if file.endswith('.parquet'):
                found_files.append(os.path.join(root, file))
                df = pd.read_parquet(os.path.join(root, file))
                print(f"Found columns: {df.columns.tolist()}")  # Debug print
                # Verify essential columns exist
                expected_cols = [
                    'instrument_name', 'snapshot_timestamp', 'coin',
                    'expiry', 'strike', 'type', 'timestamp', 'state',
                    'stats', 'greeks', 'index_price', 'last_price',
                    'settlement_price', 'min_price', 'max_price',
                    'open_interest', 'mark_price', 'best_ask_price',
                    'best_bid_price', 'interest_rate', 'mark_iv',
                    'bid_iv', 'ask_iv', 'underlying_price',
                    'underlying_index', 'estimated_delivery_price',
                    'best_ask_amount', 'best_bid_amount', 'date', 'hour'
                ]
                missing_cols = [col for col in expected_cols if col not in df.columns]
                assert not missing_cols, f"Missing columns: {missing_cols}"
    
    assert len(found_files) > 0, "No Parquet files were created"

def test_empty_snapshot():
    result = process_and_write_snapshot({}, datetime.now())
    assert result is None

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
