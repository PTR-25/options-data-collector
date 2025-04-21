import os
import pandas as pd
import glob
from datetime import datetime

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
    # Find all parquet files
    all_paths = []
    for root, _, files in os.walk(base_path):
        for file in files:
            if file.endswith('.parquet'):
                file_path = os.path.join(root, file)
                all_paths.append((file_path, os.path.getmtime(file_path)))
    
    if not all_paths:
        print("No data found in local_data directory")
        return None
    
    # Get the most recent timestamp
    latest_time = max(p[1] for p in all_paths)
    latest_files = [p[0] for p in all_paths if p[1] == latest_time]
    
    print(f"Reading data from timestamp {datetime.fromtimestamp(latest_time)}")
    
    try:
        # Read all files from the same timestamp
        dfs = []
        for file_path in latest_files:
            try:
                # Extract partition values from path
                path_parts = file_path.split(os.sep)
                coin = next((p.split('=')[1] for p in path_parts if p.startswith('coin=')), None)
                date = next((p.split('=')[1] for p in path_parts if p.startswith('date=')), None)
                expiry = next((p.split('=')[1] for p in path_parts if p.startswith('expiry=')), None)
                hour = next((p.split('=')[1] for p in path_parts if p.startswith('hour=')), None)
                
                # Read the parquet file
                df = pd.read_parquet(file_path)
                
                # Add partition columns if not present
                if coin and 'coin' not in df.columns:
                    df['coin'] = coin
                if date and 'date' not in df.columns:
                    df['date'] = date
                if expiry and 'expiry' not in df.columns:
                    df['expiry'] = expiry.split('\\')[0]  # Remove any trailing parts
                if hour and 'hour' not in df.columns:
                    df['hour'] = hour
                
                dfs.append(df)
                print(f"Loaded {len(df)} rows from {coin}")
                
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

if __name__ == "__main__":
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
