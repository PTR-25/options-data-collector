import os
import re
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from typing import Optional, Tuple
import logging
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_filename(filename: str) -> Optional[Tuple[str, str, str]]:
    """Parse hourly Parquet filename to extract metadata."""
    pattern = r"([A-Z]+)_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2}_\d{2})\.parquet$"
    match = re.match(pattern, os.path.basename(filename))
    if not match:
        logger.error(f"Invalid filename format: {filename}")
        return None
    return match.groups()  # (coin, expiry_date, snapshot_datetime)

def analyze_hourly_file(file_path: str) -> Optional[pd.DataFrame]:
    """Analyze hourly aggregated Parquet file."""
    try:
        # Parse filename first
        parsed = parse_filename(os.path.basename(file_path))
        if not parsed:
            return None
        coin, expiry_date, snapshot_datetime = parsed
        
        logger.info(f"\nAnalyzing hourly data for {coin}")
        logger.info(f"Expiry date: {expiry_date}")
        logger.info(f"Snapshot time: {snapshot_datetime}")
        
        # Read the data
        df = pd.read_parquet(file_path)
        
        # Basic information
        logger.info("\n=== Data Overview ===")
        logger.info(f"Total records: {len(df)}")
        logger.info(f"Unique instruments: {df['instrument_name'].nunique()}")
        
        # Perform sanity checks
        run_sanity_checks(df)
        
        # Plot volatility smiles instead of Greeks evolution
        plot_volatility_smiles(df, coin, snapshot_datetime)
        
        return df
        
    except Exception as e:
        logger.error(f"Error analyzing file: {e}")
        return None

def run_sanity_checks(df: pd.DataFrame) -> None:
    """Run sanity checks on hourly data."""
    logger.info("\n=== Sanity Checks ===")
    
    # 1. Count unique timestamps (should be 60)
    timestamps = pd.to_datetime(df['snapshot_timestamp'])
    n_timestamps = timestamps.nunique()
    logger.info(f"Number of timestamps: {n_timestamps} (expected: 60)")
    
    # 2. Count strikes and option types
    n_strikes = df['strike'].nunique()
    logger.info(f"Number of unique strikes: {n_strikes}")
    
    # 3. Verify puts and calls are balanced
    n_puts = len(df[df['option_type'] == 'P'])
    n_calls = len(df[df['option_type'] == 'C'])
    logger.info(f"Puts: {n_puts}, Calls: {n_calls}")
    
    # 4. Verify total records
    expected_records = n_timestamps * n_strikes * 2  # times 2 for puts and calls
    actual_records = len(df)
    logger.info(f"Total records: {actual_records}")
    logger.info(f"Expected records (timestamps × strikes × 2): {expected_records}")
    
    if actual_records != expected_records:
        logger.warning("Record count mismatch!")
        # Detailed analysis of missing data
        for strike in df['strike'].unique():
            strike_data = df[df['strike'] == strike]
            for opt_type in ['P', 'C']:
                opt_data = strike_data[strike_data['option_type'] == opt_type]
                if len(opt_data) != n_timestamps:
                    logger.warning(f"Strike {strike} {opt_type}: {len(opt_data)} records (expected {n_timestamps})")

def plot_volatility_smiles(df: pd.DataFrame, coin: str, snapshot_time: str) -> None:
    """Plot volatility smiles at start and end of the hour."""
    # Convert timestamps and sort
    df['timestamp'] = pd.to_datetime(df['snapshot_timestamp'])
    timestamps = sorted(df['timestamp'].unique())
    first_time = timestamps[0]
    last_time = timestamps[-1]
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8))
    fig.suptitle(f"{coin} Volatility Smiles\n{snapshot_time}", size=14)
    
    # Plot for both puts and calls at first and last timestamp
    for opt_type in ['C', 'P']:
        for time_point, style in [(first_time, '--'), (last_time, '-')]:
            mask = (df['timestamp'] == time_point) & (df['option_type'] == opt_type)
            data = df[mask].sort_values('strike')
            
            # Plot with different line styles for start/end
            ax.plot(
                data['strike'],
                data['mark_iv'],  # Convert to percentage
                style,
                label=f"{opt_type} ({time_point.strftime('%H:%M:%S')})",
                alpha=0.8
            )
    
    ax.set_title("Implied Volatility Smile")
    ax.set_xlabel("Strike Price")
    ax.set_ylabel("Implied Volatility (%)")
    ax.grid(True)
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    file_path = input("Enter path to hourly Parquet file: ")
    if os.path.exists(file_path):
        df = analyze_hourly_file(file_path)
        if df is not None:
            while True:
                print("\n=== Analysis Menu ===")
                print("1. Show strike distribution")
                print("2. Show full Greeks data for specific strike")
                print("3. Export to CSV")
                print("q. Quit")
                
                choice = input("\nEnter choice: ")
                if choice == "1":
                    strikes = sorted(df['strike'].unique())
                    print("\nStrike distribution:")
                    print(f"Range: {min(strikes):,.2f} - {max(strikes):,.2f}")
                    print(f"Count: {len(strikes)}")
                    print("\nStrike spacing:")
                    for i in range(min(5, len(strikes)-1)):
                        print(f"Gap {i+1}: {strikes[i]:,.2f} to {strikes[i+1]:,.2f}",
                              f"(Δ = {strikes[i+1]-strikes[i]:,.2f})")
                
                elif choice == "2":
                    strike = float(input("Enter strike price: "))
                    opt_type = input("Enter option type (P/C): ").upper()
                    mask = (df['strike'] == strike) & (df['option_type'] == opt_type)
                    if mask.any():
                        selected = df[mask].sort_values('timestamp')
                        print("\nGreeks evolution:")
                        greeks_df = pd.json_normalize(selected['greeks'])
                        print(pd.concat([selected[['timestamp']], greeks_df], axis=1))
                    else:
                        print("No data found for specified strike and option type")
                
                elif choice == "3":
                    out_path = input("Enter output CSV path: ")
                    df.to_csv(out_path, index=False)
                    print(f"Data exported to {out_path}")
                
                elif choice.lower() == 'q':
                    break
    else:
        print(f"File not found: {file_path}")
