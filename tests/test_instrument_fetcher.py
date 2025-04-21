import logging
import sys
import pytest
from instrument_fetcher import fetch_option_instruments, classify_instrument

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_classify_instrument():
    # Test standard formats
    assert classify_instrument("BTC-26SEP25-80000-C") == "BTC"
    assert classify_instrument("ETH-26SEP25-2000-P") == "ETH"
    
    # Test USDC formats
    assert classify_instrument("SOL_USDC-26SEP25-80-C") == "SOL"
    assert classify_instrument("XRP_USDC-26SEP25-0d625-C") == "XRP"
    
    # Test invalid/unsupported
    assert classify_instrument("UNKNOWN-26SEP25-100-C") is None

@pytest.mark.asyncio
async def test_fetch_instruments():
    channels = await fetch_option_instruments()
    assert isinstance(channels, list)
    assert len(channels) > 0, "Should fetch at least some instruments"
    
    # Verify channel format
    for channel in channels:
        assert channel.startswith("ticker."), f"Invalid channel format: {channel}"
        assert channel.endswith(".100ms"), f"Invalid channel format: {channel}"
        
        # Extract instrument name
        instrument = channel.split('.')[1]
        assert classify_instrument(instrument) is not None, f"Unrecognized instrument: {instrument}"

if __name__ == "__main__":
    print("Running instrument fetcher tests...")
    pytest.main([__file__, "-v"])
