import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np
from pathlib import Path

# Set seaborn style after importing it
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = [12, 6]

def analyze_parquet_data(base_path='./local_data', date='2025/04/21', hour='15'):
    """Analyze Parquet data for a specific date and hour."""
    # Construct the base path for the given timestamp
    data_path = os.path.join(base_path, date, hour)
    
    if not os.path.exists(data_path):
        raise ValueError(f"No data found at {data_path}")
    
    # Find all coin partitions
    coin_dirs = [d for d in Path(data_path).iterdir() if d.is_dir() and d.name.startswith('coin=')]
    
    if not coin_dirs:
        raise ValueError(f"No coin partitions found in {data_path}")
    
    for coin_dir in coin_dirs:
        coin = coin_dir.name.replace('coin=', '')
        print(f"\nAnalyzing {coin} options data...")
        
        # Read parquet files for this coin
        df = pd.read_parquet(str(coin_dir))
        
        # Convert timestamp to datetime if it's not already
        df['snapshot_timestamp'] = pd.to_datetime(df['snapshot_timestamp'])
        
        # Create output directory for plots
        plot_dir = os.path.join(base_path, 'analysis_plots', coin)
        os.makedirs(plot_dir, exist_ok=True)
        
        # Print data types for each column
        print(f"\n{coin} Column Data Types:")
        print("-" * 50)
        for col in df.columns:
            print(f"{col:<30} {df[col].dtype}")
        
        # Plot each numeric column over time
        for column in df.columns:
            try:
                if df[column].dtype in ['float64', 'int64']:
                    # Create a new figure for each column
                    plt.figure()
                    
                    # Calculate basic statistics
                    mean_val = df[column].mean()
                    std_val = df[column].std()
                    
                    # Create scatter plot
                    plt.scatter(df['snapshot_timestamp'], df[column], alpha=0.5, s=1)
                    plt.axhline(y=mean_val, color='r', linestyle='--', label=f'Mean: {mean_val:.2f}')
                    plt.axhline(y=mean_val + std_val, color='g', linestyle=':', label=f'±1 STD: {std_val:.2f}')
                    plt.axhline(y=mean_val - std_val, color='g', linestyle=':')
                    
                    plt.title(f'{column} Over Time')
                    plt.xlabel('Time')
                    plt.ylabel(column)
                    plt.xticks(rotation=45)
                    plt.legend()
                    plt.tight_layout()
                    
                    # Save the plot
                    plt.savefig(os.path.join(plot_dir, f'{column}_analysis.png'))
                    plt.close()
                    
                elif df[column].dtype == 'object' and column not in ['date', 'hour', 'type', 'instrument_name']:
                    # For object columns that might be dictionaries or complex structures
                    print(f"\nSample values for {column}:")
                    print(df[column].head())
                    
            except Exception as e:
                print(f"Error plotting {column}: {e}")
        
        # Create summary statistics
        print("\nNumeric Columns Summary Statistics:")
        print("-" * 50)
        print(df.describe().to_string())
        
        # Print unique values for categorical columns
        categorical_cols = ['coin', 'type', 'state']
        print("\nCategorical Columns Summary:")
        print("-" * 50)
        for col in categorical_cols:
            if col in df.columns:
                value_counts = df[col].value_counts()
                print(f"\n{col} distribution:")
                print(value_counts)

if __name__ == "__main__":
    try:
        analyze_parquet_data()
        print("\nAnalysis completed! Check the 'analysis_plots' directory for visualizations.")
    except Exception as e:
        print(f"Error during analysis: {e}")
