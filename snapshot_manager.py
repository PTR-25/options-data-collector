import logging
import os
import re
from datetime import datetime
from typing import Dict, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# Regex for parsing instrument names with support for decimal strikes
INSTRUMENT_REGEX = re.compile(r"""
    ^(                    # Start of string
    [A-Z]+               # Coin prefix (e.g., BTC, ETH)
    (?:_USDC)?          # Optional _USDC suffix
    )-                   # Separator
    (\d{1,2}[A-Z]{3}\d{2}) # Expiry (e.g., 26SEP25)
    -                    # Separator
    (\d+(?:d\d+)?)      # Strike price with optional decimal (e.g., 80000 or 0d625)
    -                    # Separator
    ([CP])              # Option type (C or P)
    $                    # End of string
""", re.VERBOSE)

def parse_instrument_name(name: str) -> Optional[Tuple[str, str, float, str]]:
    """
    Parse a Deribit instrument name into components.
    
    Args:
        name: Instrument name (e.g., "BTC-26SEP25-80000-C" or "XRP_USDC-26SEP25-0d625-C")
    
    Returns:
        Tuple of (coin, expiry_date, strike, option_type) or None if parsing fails
        Example: ("BTC", "2025-09-26", 80000.0, "C")
    """
    match = INSTRUMENT_REGEX.match(name)
    if not match:
        logger.warning(f"Could not parse instrument name: {name}")
        return None

    coin_raw, expiry_raw, strike_raw, option_type = match.groups()
    
    # Standardize coin name (remove _USDC if present)
    coin = coin_raw.replace('_USDC', '')
    
    # Convert expiry to ISO format
    try:
        expiry_dt = datetime.strptime(expiry_raw, "%d%b%y")
        expiry = expiry_dt.strftime("%Y-%m-%d")
    except ValueError:
        logger.error(f"Failed to parse expiry date: {expiry_raw}")
        return None

    # Parse strike, handling decimal notation
    try:
        if 'd' in strike_raw:
            integer, decimal = strike_raw.split('d')
            strike = float(f"{integer}.{decimal}")
        else:
            strike = float(strike_raw)
    except ValueError:
        logger.error(f"Failed to parse strike price: {strike_raw}")
        return None

    return coin, expiry, strike, option_type

def process_and_write_snapshot(
    snapshot_data: Dict[str, dict],
    timestamp: datetime,
    base_path: str = "./data",
    compression: str = "snappy"
) -> Optional[str]:
    """Process a snapshot dictionary into a pandas DataFrame and write to partitioned Parquet."""
    if not snapshot_data:
        logger.warning("Received empty snapshot data")
        return None

    try:
        records = []
        for channel, data in snapshot_data.items():
            # Extract instrument name from channel
            parts = channel.split('.')
            if len(parts) < 2:
                logger.warning(f"Invalid channel format: {channel}")
                continue
                
            instrument_name = parts[1]
            parsed = parse_instrument_name(instrument_name)
            if not parsed:
                continue
                
            coin, expiry, strike, option_type = parsed
            
            # Create record with explicit string formatting
            record = {
                "instrument_name": str(instrument_name),
                "snapshot_timestamp": timestamp.isoformat(),
                "coin": str(coin),
                "expiry": str(expiry),
                "strike": float(strike),
                "option_type": str(option_type),
                **data
            }
            records.append(record)

        if not records:
            logger.error("No valid records could be processed from the snapshot")
            return None

        # Create DataFrame and ensure column order
        df = pd.DataFrame(records)
        
        # Add partitioning columns with correct string formats
        df["date"] = timestamp.strftime("%Y-%m-%d")
        df["hour"] = timestamp.strftime("%H")

        # Ensure the output directory exists
        os.makedirs(base_path, exist_ok=True)

        # Write partitioned dataset without schema restriction
        logger.info(f"Writing {len(df)} records to Parquet dataset at: {base_path}")
        logger.debug(f"DataFrame columns: {df.columns.tolist()}")
        
        # Convert to arrow table without explicit schema (preserve all columns)
        table = pa.Table.from_pandas(df)
        
        pq.write_to_dataset(
            table,
            root_path=base_path,
            partition_cols=["coin", "date", "expiry", "hour"],
            compression=compression,
            existing_data_behavior="overwrite_or_ignore"  
        )

        logger.info(f"Successfully wrote snapshot to {base_path}")
        return base_path

    except Exception as e:
        logger.exception(f"Failed to process and write snapshot: {e}")
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("Snapshot manager module loaded.")
