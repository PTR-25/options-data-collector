import os
import pandas as pd
import glob
from datetime import datetime
from typing import Optional, Tuple
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
from pandas import json_normalize
import re

def parse_instrument_name(name: str) -> Optional[Tuple[str, str, float, str]]:
    """Parse a Deribit instrument name into components."""
    pattern = re.compile(r"^([A-Z]+(?:_USDC)?)-(\d{1,2}[A-Z]{3}\d{2})-(\d+(?:d\d+)?)-([CP])$")
    match = pattern.match(name)
    if not match:
        return None
    
    coin_raw, expiry_raw, strike_raw, option_type = match.groups()
    coin = coin_raw.replace('_USDC', '')
    
    try:
        expiry_dt = datetime.strptime(expiry_raw, "%d%b%y")
        expiry = expiry_dt.strftime("%Y-%m-%d")
        
        if 'd' in strike_raw:
            integer, decimal = strike_raw.split('d')
            strike = float(f"{integer}.{decimal}")
        else:
            strike = float(strike_raw)
            
        return coin, expiry, strike, option_type
    except (ValueError, TypeError):
        return None

def analyze_expiry_consistency(df: pd.DataFrame, folder_path: str) -> None:
    """Check if instruments are in the correct expiry folders."""
    print("\n=== Expiry Date Analysis ===")
    
    # Extract expiry from folder path, handling the hour part properly
    folder_expiry = next(
        (part.split('=')[1].split('\\')[0] for part in folder_path.split(os.sep) 
         if part.startswith('expiry=')), 
        None
    )
    
    if folder_expiry:
        # Clean up expiry (remove any trailing parts like \hour=17)
        folder_expiry = folder_expiry.split('\\')[0]
        mismatched = df[df['expiry'] != folder_expiry]
        if not mismatched.empty:
            print(f"\nWARNING: Found {len(mismatched)} instruments with expiry different from folder {folder_expiry}:")
            print(mismatched[['instrument_name', 'expiry']].head())
        else:
            print(f"✓ All instruments match folder expiry date: {folder_expiry}")

def analyze_strikes(df: pd.DataFrame) -> None:
    """Analyze strike price distribution."""
    print("\n=== Strike Price Analysis ===")
    
    for coin in df['coin'].unique():
        coin_data = df[df['coin'] == coin]
        print(f"\n{coin} Options:")
        print(f"Strike range: {coin_data['strike'].min():,.2f} - {coin_data['strike'].max():,.2f}")
        
        # Show strike distribution
        strike_counts = coin_data.groupby('strike').size().sort_index()
        print("\nStrike distribution (top 5 most common):")
        print(strike_counts.nlargest(5))

def read_latest_snapshot(base_path='./local_data'):
    """Read the most recent Parquet dataset."""
    try:
        all_paths = []
        for root, _, files in os.walk(base_path):
            for file in files:
                if file.endswith('.parquet'):
                    file_path = os.path.join(root, file)
                    all_paths.append((file_path, os.path.getmtime(file_path)))
        
        if not all_paths:
            print("No data found in local_data directory")
            return None
        
        latest_time = max(p[1] for p in all_paths)
        latest_files = [p[0] for p in all_paths if p[1] == latest_time]
        
        print(f"Reading data from timestamp {datetime.fromtimestamp(latest_time)}")
        
        dfs = []
        for file_path in latest_files:
            try:
                # Read the parquet file
                df = pd.read_parquet(file_path)
                
                # Extract partition values from path parts
                parts = file_path.split(os.sep)
                partition_values = {}
                
                for part in parts:
                    if '=' in part:
                        key, value = part.split('=', 1)
                        # Remove any trailing path components
                        value = value.split('\\')[0].split('/')[0]
                        partition_values[key] = value
                
                # Add partition columns if they don't exist
                for col, value in partition_values.items():
                    if col not in df.columns:
                        df[col] = value
                
                dfs.append(df)
                print(f"Loaded {len(df)} rows from {partition_values.get('coin', 'Unknown')}")
                
            except Exception as e:
                print(f"Error reading file {file_path}: {e}")
                continue
        
        if not dfs:
            raise ValueError("No valid data found in any partition")
        
        # Combine all DataFrames
        df = pd.concat(dfs, ignore_index=True)
        
        print(f"\nLoaded total of {len(df)} rows from {len(df['coin'].unique())} coins")
        
        print("\n=== Dataset Overview ===")
        print(f"Total rows: {len(df)}")
        print(f"Unique instruments: {df['instrument_name'].nunique()}")
        
        print("\n=== Coin Distribution ===")
        coin_counts = df.groupby('coin').size()
        print(coin_counts)
        
        print("\n=== Expiry Distribution ===")
        if 'expiry' in df.columns:
            expiry_counts = df.groupby(['coin', 'expiry']).size().unstack(fill_value=0)
            print(expiry_counts)
        
        # Continue with existing analysis...
        analyze_strikes(df)
        
        print("\n=== Sample Instruments by Strike Range (per coin) ===")
        for coin in sorted(df['coin'].unique()):
            coin_data = df[df['coin'] == coin]
            print(f"\n{coin}:")
            strikes = sorted(coin_data['strike'].unique())
            if len(strikes) >= 3:
                for strike in [strikes[0], strikes[len(strikes)//2], strikes[-1]]:
                    sample = coin_data[coin_data['strike'] == strike].iloc[0]
                    print(f"\nStrike {strike:,.2f}:")
                    print(f"Instrument: {sample['instrument_name']}")
                    for col in ['mark_price', 'mark_iv', 'underlying_price']:
                        if col in sample:
                            print(f"{col}: {sample[col]}")
        
        return df
        
    except Exception as e:
        print(f"Error reading parquet files: {e}")
        return None

def read_specific_file(file_path: str) -> Optional[pd.DataFrame]:
    """Read a specific Parquet file and display its contents."""
    try:
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return None
            
        print(f"Reading file: {file_path}")
        df = pd.read_parquet(file_path)
        
        print("\n=== File Contents Overview ===")
        print(f"Total rows: {len(df)}")
        print(f"Unique instruments: {df['instrument_name'].nunique()}")
        
        print("\n=== Instruments List ===")
        instruments = df[['instrument_name', 'coin', 'strike', 'expiry']].drop_duplicates()
        instruments = instruments.sort_values(['coin', 'expiry', 'strike'])
        print(instruments)
        
        return df
        
    except Exception as e:
        print(f"Error reading file: {e}")
        return None

def analyze_hourly_file(file_path: str) -> Optional[pd.DataFrame]:
    """Analyze a hourly aggregated Parquet file from S3."""
    try:
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return None
            
        print(f"\n=== Analyzing Hourly Aggregated File ===")
        print(f"File: {os.path.basename(file_path)}")
        
        # Read parquet metadata first
        parquet_file = pq.ParquetFile(file_path)
        print("\nParquet File Metadata:")
        print(f"Number of row groups: {parquet_file.num_row_groups}")
        print(f"Schema:\n{parquet_file.schema_arrow}")
        
        # Read the data
        df = pd.read_parquet(file_path)
        
        print("\n=== Data Overview ===")
        print(f"Total rows: {len(df)}")
        print(f"Unique instruments: {df['instrument_name'].nunique()}")
        
        print("\n=== Timestamp Range ===")
        if 'snapshot_timestamp' in df.columns:
            timestamps = pd.to_datetime(df['snapshot_timestamp'])
            print(f"Start: {timestamps.min()}")
            print(f"End: {timestamps.max()}")
            print(f"Total time span: {timestamps.max() - timestamps.min()}")
            print(f"Number of unique timestamps: {timestamps.nunique()}")
        
        print("\n=== Instrument Details ===")
        instruments = df.groupby(['coin', 'expiry', 'option_type'])['strike'].agg(['count', 'min', 'max'])
        print(instruments)
        
        print("\n=== Sample Price Data ===")
        if 'mark_price' in df.columns and 'mark_iv' in df.columns:
            sample = df.groupby('option_type').agg({
                'mark_price': ['mean', 'min', 'max'],
                'mark_iv': ['mean', 'min', 'max']
            })
            print(sample)
        
        # Add sanity checks and Greek plots
        sanity_check_hourly(df)
        plot_greeks_evolution(df)
        
        return df
        
    except Exception as e:
        print(f"Error analyzing hourly file: {e}")
        return None

def sanity_check_hourly(df: pd.DataFrame) -> None:
    """Perform sanity checks on hourly data."""
    print("\n=== Sanity Check Results ===")
    
    # Extract coin and expiry from instrument names
    parsed_data = [parse_instrument_name(name) for name in df['instrument_name']]
    valid_data = [d for d in parsed_data if d is not None]
    
    if not valid_data:
        print("Error: Could not parse any instrument names")
        return
        
    coins = {d[0] for d in valid_data}  # Get unique coins
    
    # Count unique timestamps
    timestamps = pd.to_datetime(df['snapshot_timestamp'])
    n_timestamps = timestamps.nunique()
    print(f"Number of unique timestamps: {n_timestamps}")
    if n_timestamps != 60:
        print(f"WARNING: Expected 60 timestamps (one per minute), found {n_timestamps}")
    
    # Count unique strikes per coin
    for coin in coins:
        coin_instruments = [d for d in valid_data if d[0] == coin]
        unique_strikes = {d[2] for d in coin_instruments}
        n_strikes = len(unique_strikes)
        
        print(f"\nCoin: {coin}")
        print(f"Number of unique strikes: {n_strikes}")
        
        # Count options (puts and calls)
        n_instruments = len([d for d in coin_instruments])
        expected = n_timestamps * n_strikes * 2  # times 2 for puts and calls
        print(f"Total records: {n_instruments}")
        print(f"Expected records (timestamps × strikes × 2): {expected}")
        
        if n_instruments != expected:
            print("WARNING: Number of records doesn't match expected count!")
            # Detailed analysis
            for strike in sorted(unique_strikes):
                n_puts = len([d for d in coin_instruments if d[2] == strike and d[3] == 'P'])
                n_calls = len([d for d in coin_instruments if d[2] == strike and d[3] == 'C'])
                if n_puts != n_timestamps or n_calls != n_timestamps:
                    print(f"Strike {strike}: {n_puts} puts, {n_calls} calls (expected {n_timestamps} each)")

def plot_greeks_evolution(df: pd.DataFrame) -> None:
    """Plot the evolution of Greeks over time."""
    print("\n=== Plotting Greeks Evolution ===")
    
    # Parse instrument names to get coin and strike information
    parsed_data = [(name, *parse_instrument_name(name)) for name in df['instrument_name']]
    valid_data = [(name, *data) for name, *data in parsed_data if data]
    
    if not valid_data:
        print("Error: Could not parse instrument names for plotting")
        return
    
    # Convert timestamps
    df['timestamp'] = pd.to_datetime(df['snapshot_timestamp'])
    
    # Normalize the greeks data
    df_greeks = pd.json_normalize(df['greeks'])
    
    # Plot for each coin
    for coin in {data[1] for _, *data in valid_data}:
        coin_instruments = [inst for inst, *data in valid_data if data[0] == coin]
        
        greek_cols = ['delta', 'gamma', 'vega', 'theta', 'rho']
        fig, axes = plt.subplots(len(greek_cols), 1, figsize=(15, 4*len(greek_cols)))
        fig.suptitle(f"{coin} Greeks Evolution", size=16)
        
        for i, greek in enumerate(greek_cols):
            ax = axes[i]
            
            # Get min, max, and middle strikes for this coin
            strikes = sorted({data[3] for _, *data in valid_data if data[0] == coin})
            sample_strikes = [strikes[0], strikes[len(strikes)//2], strikes[-1]]
            
            for strike in sample_strikes:
                for option_type in ['C', 'P']:
                    mask = (df['instrument_name'].isin(coin_instruments)) & \
                           (df['option_type'] == option_type) & \
                           df['instrument_name'].str.contains(f"-{int(strike)}-")
                    
                    if mask.any():
                        ax.plot(df[mask]['timestamp'], 
                               df_greeks[greek][mask],
                               label=f"K={strike:,.0f} {option_type}",
                               alpha=0.7)
            
            ax.set_title(f"{greek.capitalize()}")
            ax.set_xlabel("Time")
            ax.set_ylabel(greek.capitalize())
            ax.grid(True)
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        
        plt.tight_layout()
        plt.show()

def visualize_parquet_data(file_path: str) -> Optional[pd.DataFrame]:
    """Detailed visualization of Parquet file contents."""
    try:
        df = pd.read_parquet(file_path)
        
        print("\n=== Basic Information ===")
        print(df.info())
        
        print("\n=== All Column Names ===")
        for i, col in enumerate(df.columns, 1):
            print(f"{i}. {col}")
        
        print("\n=== First 5 Minutes ===")
        df['timestamp'] = pd.to_datetime(df['snapshot_timestamp'])
        df = df.sort_values('timestamp')
        print(df.head().to_string())
        
        print("\n=== Last 5 Minutes ===")
        print(df.tail().to_string())
        
        print("\n=== Sample Statistics ===")
        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
        print(df[numeric_cols].describe())
        
        # Extract nested data if present
        if 'stats' in df.columns:
            print("\n=== Sample Stats Structure ===")
            print(df['stats'].iloc[0])
            
        if 'greeks' in df.columns:
            print("\n=== Sample Greeks Structure ===")
            print(df['greeks'].iloc[0])
        
        return df
        
    except Exception as e:
        print(f"Error analyzing Parquet file: {e}")
        return None

def export_to_csv(df: pd.DataFrame, base_path: str) -> None:
    """Export DataFrame to CSV with proper path handling."""
    try:
        # If path is a directory, create a timestamped filename
        if os.path.isdir(base_path):
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"export_{timestamp}.csv"
            full_path = os.path.join(base_path, filename)
        else:
            # Ensure the directory exists
            os.makedirs(os.path.dirname(base_path), exist_ok=True)
            # Add .csv extension if not present
            full_path = base_path if base_path.endswith('.csv') else f"{base_path}.csv"
        
        # Export with progress indication for large files
        print(f"Exporting {len(df)} rows to {full_path}...")
        df.to_csv(full_path, index=False)
        print(f"Export completed successfully!")
        
    except PermissionError:
        print("Error: Permission denied. Make sure you have write access and the file isn't open.")
    except Exception as e:
        print(f"Error exporting to CSV: {e}")

if __name__ == "__main__":
    print("\nChoose analysis mode:")
    print("1. Analyze live data directory")
    print("2. Analyze specific hourly file")
    print("3. Analyze specific snapshot file")
    print("4. Detailed Parquet visualization")  # New option
    
    choice = input("\nEnter choice (1-4): ")
    
    if choice == "2":
        file_path = input("Enter path to hourly Parquet file: ")
        df = analyze_hourly_file(file_path)
        if df is not None:
            while True:
                print("\n=== Hourly File Analysis Menu ===")
                print("1. Show detailed statistics")
                print("2. Filter by strike range")
                print("3. Show specific instrument history")
                print("4. Export to CSV")
                print("q. Quit")
                
                sub_choice = input("\nEnter choice: ")
                if sub_choice == "1":
                    print("\nDetailed Statistics:")
                    print(df.describe())
                elif sub_choice == "2":
                    min_strike = float(input("Enter minimum strike: "))
                    max_strike = float(input("Enter maximum strike: "))
                    filtered = df[(df['strike'] >= min_strike) & (df['strike'] <= max_strike)]
                    print(f"\nFound {len(filtered)} records in strike range")
                    print(filtered)
                elif sub_choice == "3":
                    instrument = input("Enter instrument name (or part of it): ")
                    matches = df[df['instrument_name'].str.contains(instrument, case=False)]
                    if not matches.empty:
                        print(f"\nFound {len(matches)} records:")
                        print(matches.sort_values('snapshot_timestamp'))
                    else:
                        print("No matching instruments found")
                elif sub_choice == "4":
                    out_path = input("Enter output path (directory or .csv file): ")
                    export_to_csv(df, out_path)
                elif sub_choice.lower() == 'q':
                    break
    elif choice == "3":
        file_path = input("Enter path to snapshot Parquet file: ")
        df = read_specific_file(file_path)
        if df is not None:
            while True:
                print("\n=== Snapshot File Analysis Menu ===")
                print("1. Show raw data")
                print("2. Show statistics")
                print("3. Filter by instrument")
                print("b. Back to main menu")
                
                sub_choice = input("\nEnter your choice: ")
                if sub_choice == '1':
                    n = input("How many rows to show? ")
                    try:
                        print(df.head(int(n)))
                    except ValueError:
                        print("Please enter a valid number")
                elif sub_choice == '2':
                    print("\nBasic statistics for numeric columns:")
                    print(df.describe())
                elif sub_choice == '3':
                    instrument = input("Enter instrument name (or part of it): ")
                    matches = df[df['instrument_name'].str.contains(instrument, case=False)]
                    if not matches.empty:
                        print(f"\nFound {len(matches)} matching rows:")
                        print(matches)
                    else:
                        print("No matching instruments found")
                elif sub_choice.lower() == 'b':
                    break
                else:
                    print("Invalid choice")
    elif choice == "4":
        file_path = input("Enter path to Parquet file: ")
        df = visualize_parquet_data(file_path)
        if df is not None:
            while True:
                print("\n=== Detailed Analysis Menu ===")
                print("1. Show specific columns")
                print("2. Filter by timestamp range")
                print("3. Show unique values in column")
                print("4. Analyze nested structures")
                print("5. Export to CSV")
                print("q. Quit")
                
                sub_choice = input("\nEnter choice: ")
                if sub_choice == "1":
                    print("\nAvailable columns:")
                    for i, col in enumerate(df.columns):
                        print(f"{i}. {col}")
                    cols = input("Enter column numbers (comma-separated): ")
                    try:
                        col_indices = [int(x.strip()) for x in cols.split(",")]
                        selected_cols = df.columns[col_indices]
                        print(df[selected_cols].to_string())
                    except Exception as e:
                        print(f"Error: {e}")
                
                elif sub_choice == "2":
                    start = input("Enter start time (YYYY-MM-DD HH:MM:SS): ")
                    end = input("Enter end time (YYYY-MM-DD HH:MM:SS): ")
                    try:
                        mask = (df['timestamp'] >= start) & (df['timestamp'] <= end)
                        print(df[mask].to_string())
                    except Exception as e:
                        print(f"Error: {e}")
                
                elif sub_choice == "3":
                    col = input("Enter column name: ")
                    if col in df.columns:
                        print(df[col].value_counts())
                    else:
                        print("Column not found")
                
                elif sub_choice == "4":
                    if 'stats' in df.columns:
                        print("\nStats structure for first row:")
                        print(pd.json_normalize(df['stats'].iloc[0]))
                    if 'greeks' in df.columns:
                        print("\nGreeks structure for first row:")
                        print(pd.json_normalize(df['greeks'].iloc[0]))
                
                elif sub_choice == "5":
                    out_path = input("Enter output path (directory or .csv file): ")
                    export_to_csv(df, out_path)
                
                elif sub_choice.lower() == 'q':
                    break
    else:
        df = read_latest_snapshot()
        if df is not None:
            while True:
                print("\n=== Interactive Menu ===")
                print("1. Show all columns")
                print("2. Show basic statistics")
                print("3. Show sample rows")
                print("4. Filter by coin")
                print("5. Show unique values in a column")
                print("6. Analyze specific expiry date")
                print("7. Show strike distribution")
                print("q. Quit")
                
                choice = input("\nEnter your choice: ")
                
                if choice == '1':
                    print("\nAvailable columns:")
                    for col in df.columns:
                        print(f"- {col}")
                
                elif choice == '2':
                    print("\nBasic statistics for numeric columns:")
                    print(df.describe())
                
                elif choice == '3':
                    n = input("How many rows to show? ")
                    try:
                        print(df.sample(int(n)))
                    except ValueError:
                        print("Please enter a valid number")
                
                elif choice == '4':
                    coin = input("Enter coin (e.g., BTC, ETH): ").upper()
                    if coin in df['coin'].unique():
                        coin_df = df[df['coin'] == coin]
                        print(f"\nShowing {len(coin_df)} {coin} options:")
                        print(coin_df.head())
                    else:
                        print(f"No data found for {coin}")
                
                elif choice == '5':
                    print("\nAvailable columns:")
                    for i, col in enumerate(df.columns):
                        print(f"{i}. {col}")
                    col_idx = input("Enter column number: ")
                    try:
                        col_name = df.columns[int(col_idx)]
                        print(f"\nUnique values in {col_name}:")
                        print(df[col_name].value_counts().head(10))
                    except (ValueError, IndexError):
                        print("Invalid column number")
                
                elif choice == '6':
                    expiry = input("Enter expiry date (YYYY-MM-DD): ")
                    expiry_df = df[df['expiry'] == expiry]
                    if not expiry_df.empty:
                        print(f"\nInstruments expiring on {expiry}:")
                        for coin in expiry_df['coin'].unique():
                            coin_expiry = expiry_df[expiry_df['coin'] == coin]
                            print(f"\n{coin}: {len(coin_expiry)} instruments")
                            print("Strike range:", f"{coin_expiry['strike'].min():,.2f} -",
                                  f"{coin_expiry['strike'].max():,.2f}")
                            print("\nSample instruments:")
                            print(coin_expiry[['instrument_name', 'strike', 'mark_price', 'mark_iv']].head())
                    else:
                        print(f"No instruments found for expiry {expiry}")
                
                elif choice == '7':
                    coin = input("Enter coin (e.g., BTC, ETH): ").upper()
                    if coin in df['coin'].unique():
                        coin_df = df[df['coin'] == coin]
                        strikes = sorted(coin_df['strike'].unique())
                        print(f"\n{coin} strike distribution:")
                        print(f"Number of strikes: {len(strikes)}")
                        print(f"Strike range: {min(strikes):,.2f} - {max(strikes):,.2f}")
                        print("\nStrike spacing examples:")
                        for i in range(min(5, len(strikes)-1)):
                            print(f"Gap {i+1}: {strikes[i]:,.2f} to {strikes[i+1]:,.2f}",
                                  f"(Δ = {strikes[i+1]-strikes[i]:,.2f})")
                    else:
                        print(f"No data found for {coin}")
                
                elif choice.lower() == 'q':
                    break
                
                else:
                    print("Invalid choice")
