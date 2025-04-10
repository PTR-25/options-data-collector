# Deribit Options Data Collector

## Overview

This Python script is designed to collect real-time options order book data from the Deribit cryptocurrency exchange. It authenticates with the Deribit API, retrieves order book information for various options instruments, classifies these instruments based on their expiry, and stores the data in organized CSV files. The script handles API rate limiting, retries failed requests, and validates the incoming data to ensure its integrity. It is intended for users who need historical or near real-time options market data for analysis, research, or trading strategies.

## Features

* **Real-time Data Collection:** Fetches up-to-date order book information for a wide range of Deribit options.
* **API Authentication:** Securely authenticates with the Deribit API using client credentials stored in environment variables.
* **Rate Limit Handling:** Implements a rate limiter with credit management to avoid exceeding Deribit API limits.
* **Retry Mechanism:** Automatically retries failed API requests with exponential backoff for robustness.
* **Data Validation:** Validates the structure and content of the fetched order book data to ensure accuracy.
* **Options Classification:** Classifies options based on their time to expiry (e.g., 0DTE, Weekly, Monthly, Quarterly, LEAPS) following Deribit's conventions.
* **Organized Data Storage:** Saves data into CSV files, structured by underlying asset, expiry type, and specific expiry date, making it easy to locate and analyze specific datasets.
* **Efficient Data Handling:** Uses a data buffer to collect data in chunks before writing to disk, improving efficiency.
* **Comprehensive Logging:** Provides detailed logging of the script's operations, including API calls, errors, and metrics.
* **Environment Variable Configuration:** Utilizes `.env` files for easy configuration of API keys and other settings.
* **Metrics Tracking:** Collects and logs key performance indicators such as API call counts, success rates, and processing times.

## Requirements

* **Python Version:** 3.7 or higher (due to the use of `dataclasses` and `asyncio`).
* **Core Dependencies:**
    ```
    aiohttp>=3.8.0
    pandas>=2.0.0
    python-dateutil>=2.8.2
    python-dotenv>=1.0.0
    pyarrow>=14.0.1
    numpy>=1.24.0
    boto3>=1.28.0
    botocore>=1.31.0
    ```
* **Performance Optimizations:**
    ```
    aiodns>=3.0.0      # For better DNS resolution performance
    cchardet>=2.1.7    # For better character encoding detection
    ujson>=5.7.0       # For faster JSON processing
    ```
* **Additional Features:**
    ```
    aiofiles>=23.1.0   # For async file operations
    cryptography>=41.0.0  # For secure connections
    tenacity>=8.2.0    # For enhanced retry mechanisms
    ```

## Installation Instructions

1.  **Clone the repository**:
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  **Create a virtual environment** (recommended):
    ```bash
    python -m venv venv
    source venv/bin/activate  # On macOS/Linux
    # venv\Scripts\activate  # On Windows
    ```

3.  **Install the required dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Create a `.env` file** in the same directory as the script and configure the following environment variables:
    ```
    CLIENT_ID_1=<your_deribit_client_id>
    CLIENT_SECRET_1=<your_deribit_client_secret>
    BASE_URL=[https://www.deribit.com/api/v2](https://www.deribit.com/api/v2)
    BASE_SAVE_PATH=data  # Optional: Define a custom path to save data
    ```
    Replace `<your_deribit_client_id>` and `<your_deribit_client_secret>` with your actual Deribit API credentials. You can obtain these from your Deribit account settings. The `BASE_URL` is set to the standard Deribit API v2 endpoint. `BASE_SAVE_PATH` specifies the directory where the collected data will be stored (default is `data`).

## Usage

The script can be run either locally or on an AWS EC2 instance.

### Local Usage

For local data collection without S3 synchronization:

1. **Configure environment:**
   ```bash
   # In .env file
   BASE_SAVE_PATH="./data"  # Local directory for data storage
   BASE_URL="https://www.deribit.com/api/v2"
   CLIENT_ID_1="your_deribit_client_id"
   CLIENT_SECRET_1="your_deribit_client_secret"
   ```

2. **Run the local version:**
   ```bash
   python local_runner.py
   ```

### EC2 Deployment

For production deployment with S3 synchronization:

1. **Set up AWS credentials:**
   - Configure AWS credentials for S3 access
   - Create an S3 bucket for data storage

2. **Configure environment:**
   ```bash
   # In .env file
   BASE_SAVE_PATH="/home/ec2-user/data"
   BASE_URL="https://www.deribit.com/api/v2"
   CLIENT_ID_1="your_deribit_client_id"
   CLIENT_SECRET_1="your_deribit_client_secret"
   S3_BUCKET="your-s3-bucket-name"
   ```

3. **Run the EC2 version:**
   ```bash
   python ec2_runner.py
   ```

   The EC2 version includes:
   - Automatic data rotation every day
   - S3 synchronization of collected data
   - Cleanup of old local data folders
   - Connection management with automatic recovery

## Code Structure

The script is organized into several key components:

* **`Constants`:** Defines constants like `BATCH_SIZE`, `MAX_CONCURRENT_REQUESTS`, and `CHUNK_SIZE` for optimization.
* **`is_last_friday(expiry_date)`:** A function to determine if a given date is the last Friday of its month.
* **`RateLimitError`, `AuthError`, `ValidationError`:** Custom exception classes for handling specific error conditions.
* **`Metrics` (dataclass):** A dataclass to store and track various metrics related to API calls and processing times.
* **`retry_with_backoff(max_retries=3, initial_delay=1.0)`:** A decorator that implements an exponential backoff retry mechanism for functions that might fail due to rate limits or network issues.
* **`RateLimiter` (class):** Manages the rate of API requests by tracking credits and waiting when necessary.
* **`DataBuffer` (class):** Buffers incoming data in memory and flushes it to disk in chunks to improve writing efficiency.
* **`DataValidator` (class):** Contains static methods for validating the structure and content of the fetched data, such as the order book.
* **`DeribitAuth` (class):** Handles the authentication process with the Deribit API, obtaining and refreshing access tokens.
* **`DeribitAPI` (class):** Provides methods for interacting with the Deribit API, including fetching instruments and order book data. It uses the `DeribitAuth` class for authentication and the `RateLimiter` for rate control.
* **`OptionsDataStore` (class):** The main class responsible for orchestrating the data collection process. It initializes the API client, loads instrument lists, fetches data in batches, classifies options, and stores the data in CSV files.
    * `load_instruments()`: Fetches the list of active options instruments from Deribit for configured currencies.
    * `classify_option(expiry_date, current_datetime=None)`: Determines the expiry type of an option based on its expiry date.
    * `get_storage_path(underlying, expiry_type, expiry_date)`: Generates the file path where the data for a specific instrument should be stored.
    * `Workspace_instrument_data(instrument)`: Fetches the order book data for a given instrument from the Deribit API.
    * `process_instrument(instrument)`: Fetches and buffers data for a single instrument.
    * `process_instrument_batch(instruments)`: Processes a list of instruments concurrently.
    * `process_instruments_in_chunks()`: Iterates through all instruments in chunks and processes them.
    * `write_buffered_data()`: Writes the data currently in the buffer to the respective CSV files.
    * `log_metrics()`: Logs the collected API and processing metrics.
    * `run()`: The main loop that continuously fetches and stores data at 5-minute intervals.
* **`main()` (async function):** Creates an instance of `OptionsDataStore` and runs the data collection process.
