# Start the project here.

import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
import logging
from typing import Dict, List, Optional, Tuple, Any, Callable
import json
from calendar import monthrange
import os
from dotenv import load_dotenv
import time
from dataclasses import dataclass
from enum import Enum
import functools
from collections import defaultdict
import statistics
import numpy as np
import shutil
import boto3
from pathlib import Path

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants for optimization
BATCH_SIZE = 100  # Number of instruments to process in parallel
MAX_CONCURRENT_REQUESTS = 20  # Maximum concurrent API requests
CHUNK_SIZE = 1000  # Number of records to write in one batch

def is_last_friday(expiry_date: datetime.date) -> bool:
    """
    Return True if expiry_date is the last Friday of its month.
    """
    if expiry_date.weekday() != 4:  # Friday is 4
        return False
    next_friday = expiry_date + timedelta(days=7)
    return next_friday.month != expiry_date.month

class RateLimitError(Exception):
    pass

class AuthError(Exception):
    pass

class ValidationError(Exception):
    pass

@dataclass
class Metrics:
    """Metrics collection for monitoring"""
    api_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    retry_attempts: int = 0
    rate_limit_hits: int = 0
    processing_times: List[float] = None
    last_update: float = 0
    
    def __post_init__(self):
        self.processing_times = []
     
    def reset(self):
        self.api_calls = 0
        self.successful_calls = 0
        self.failed_calls = 0
        self.retry_attempts = 0
        self.rate_limit_hits = 0
        self.processing_times.clear()   
         
    def record_api_call(self, success: bool, retry: bool = False, rate_limit: bool = False):
        """Record an API call"""
        self.api_calls += 1
        if success:
            self.successful_calls += 1
        else:
            self.failed_calls += 1
        if retry:
            self.retry_attempts += 1
        if rate_limit:
            self.rate_limit_hits += 1
            
    def record_processing_time(self, duration: float):
        """Record processing time"""
        self.processing_times.append(duration)
        if len(self.processing_times) > 1000:  # Keep last 1000 measurements
            self.processing_times.pop(0)
            
    def get_stats(self) -> dict:
        """Get current statistics"""
        return {
            "api_calls": self.api_calls,
            "success_rate": self.successful_calls / self.api_calls if self.api_calls > 0 else 0,
            "failure_rate": self.failed_calls / self.api_calls if self.api_calls > 0 else 0,
            "retry_rate": self.retry_attempts / self.api_calls if self.api_calls > 0 else 0,
            "rate_limit_rate": self.rate_limit_hits / self.api_calls if self.api_calls > 0 else 0,
            "avg_processing_time": statistics.mean(self.processing_times) if self.processing_times else 0,
            "p95_processing_time": statistics.quantiles(self.processing_times, n=20)[-1] if self.processing_times else 0
        }

def retry_with_backoff(max_retries: int = 3, initial_delay: float = 1.0):
    """Decorator for retrying functions with exponential backoff"""
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except (RateLimitError, aiohttp.ClientError) as e:
                    last_exception = e
                    if isinstance(e, RateLimitError):
                        if args and hasattr(args[0], 'metrics'):
                            args[0].metrics.record_api_call(False, retry=True, rate_limit=isinstance(e, RateLimitError))
                        # For rate limits, wait longer
                        await asyncio.sleep(delay * 2)
                    else:
                        await asyncio.sleep(delay)
                    delay *= 2  # Exponential backoff
                    
            raise last_exception
        return wrapper
    return decorator

class RateLimiter:
    def __init__(self):
        self.credits = 50000.0
        self.refill_rate = 10000.0
        self.last_refill = time.time()
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        
    async def acquire(self, amount: float = 500.0) -> bool:
        current_time = time.time()
        time_passed = current_time - self.last_refill
        self.credits = min(50000.0, self.credits + time_passed * self.refill_rate)
        self.last_refill = current_time
        
        if self.credits < amount:
            return False
            
        self.credits -= amount
        return True
        
    async def wait_for_credits(self, amount: float = 500.0):
        while not await self.acquire(amount):
            await asyncio.sleep(0.1)

class DataBuffer:
    def __init__(self, chunk_size: int = CHUNK_SIZE):
        self.chunk_size = chunk_size
        self.buffer: Dict[str, List[dict]] = defaultdict(list)
        self.lock = asyncio.Lock()
        
    async def add(self, instrument: str, data: dict):
        async with self.lock:
            self.buffer[instrument].append(data)
            if len(self.buffer[instrument]) >= self.chunk_size:
                return await self.flush(instrument)
        return None
        
    async def flush(self, instrument: str) -> Optional[pd.DataFrame]:
        async with self.lock:
            if instrument in self.buffer and self.buffer[instrument]:
                df = pd.DataFrame(self.buffer[instrument])
                self.buffer[instrument] = []
                return df
        return None

class DataValidator:
    """Data validation for order book data"""
    @staticmethod
    def validate_order_book(data: dict) -> bool:
        """Validate order book data"""
        required_fields = [
            "timestamp", "best_bid_price", "best_ask_price",
            "best_bid_amount", "best_ask_amount", "greeks",
            "mark_iv", "mark_price", "underlying_price"
        ]
        
        # Check required fields
        for field in required_fields:
            if field not in data:
                raise ValidationError(f"Missing required field: {field}")
                
        # Validate price relationships
        #if data["best_bid_price"] > data["best_ask_price"]:
        #    raise ValidationError("Bid price higher than ask price")
            
        # Validate Greeks
        greeks = data["greeks"]
        #if not all(-1 <= greeks[greek] <= 1 for greek in ["delta", "gamma", "theta", "vega", "rho"]):
        #    raise ValidationError("Invalid Greeks values")
            
        # Validate IV
        if not 0 <= data["mark_iv"] <= 1000:  # Assuming max IV of 500%
            raise ValidationError("Invalid implied volatility")
            
        return True

class DeribitAuth:
    def __init__(self):
        self.client_id = os.getenv('CLIENT_ID_1')
        self.client_secret = os.getenv('CLIENT_SECRET_1')
        self.access_token = None
        self.refresh_token = None
        self.token_expiry = None
        self._lock = asyncio.Lock()
        # Validate environment variables
        if not self.client_id or not self.client_secret:
            raise AuthError("Missing CLIENT_ID_1 or CLIENT_SECRET_1 in environment variables")
        
    @retry_with_backoff(max_retries=5)
    async def get_token(self) -> str:
        """Get a valid access token"""
        # First check: if token exists and is valid, return it.
        if self.access_token and self.token_expiry and time.time() < self.token_expiry:
            return self.access_token

        # Acquire lock so that only one coroutine refreshes the token.
        async with self._lock:
            # Re-check the token in case it was refreshed while waiting for the lock.
            if self.access_token and self.token_expiry and time.time() < self.token_expiry:
                return self.access_token

            async with aiohttp.ClientSession() as session:
                url = f"{os.getenv('BASE_URL')}/public/auth"
                # Format request according to JSON-RPC 2.0
                request_data = {
                    "jsonrpc": "2.0",
                    "method": "public/auth",
                    "params": {
                        "grant_type": "client_credentials",
                        "client_id": self.client_id,
                        "client_secret": self.client_secret
                    }
                }

                try:
                    async with session.post(url, json=request_data) as response:
                        response_text = await response.text()
                        logger.debug(f"Auth response: {response_text}")

                        if response.status != 200:
                            raise AuthError(f"Authentication failed: {response_text}")

                        result = json.loads(response_text)

                        # Check for JSON-RPC error
                        if "error" in result:
                            error = result["error"]
                            raise AuthError(f"API error: {error.get('message', 'Unknown error')} (code: {error.get('code', 'unknown')})")

                        # Validate response structure
                        if "result" not in result:
                            raise AuthError("Invalid response format: missing 'result' field")

                        auth_result = result["result"]
                        required_fields = ["access_token", "refresh_token", "expires_in"]
                        for field in required_fields:
                            if field not in auth_result:
                                raise AuthError(f"Invalid response format: missing '{field}' field")

                        self.access_token = auth_result["access_token"]
                        self.refresh_token = auth_result["refresh_token"]
                        self.token_expiry = time.time() + auth_result["expires_in"]

                        logger.info("Successfully obtained new access token")
                        return self.access_token

                except json.JSONDecodeError as e:
                    raise AuthError(f"Failed to parse response: {e}")
                except Exception as e:
                    raise AuthError(f"Authentication failed: {str(e)}")

class DeribitAPI:
    def __init__(self):
        self.base_url = os.getenv('BASE_URL')
        self.auth = DeribitAuth()
        self.rate_limiter = RateLimiter()
        self.session = None
        self.metrics = Metrics()
        self.validator = DataValidator()
        
    async def initialize(self):
        """Initialize the API client"""
        self.session = aiohttp.ClientSession()
        
    async def close(self):
        """Close the API client"""
        if self.session:
            await self.session.close()
            
    @retry_with_backoff(max_retries=5)
    async def _make_request(self, method: str, params: dict) -> dict:
        """Make an authenticated API request with rate limiting"""
        start_time = time.time()
        try:
            await self.rate_limiter.wait_for_credits()
            
            headers = {
                "Authorization": f"Bearer {await self.auth.get_token()}"
            }
            
            async with self.session.post(
                f"{self.base_url}/{method}",
                json={"jsonrpc": "2.0", "method": method, "params": params},
                headers=headers
            ) as response:
                if response.status == 429 or response.status == 10028:  # Too many requests
                    self.metrics.record_api_call(False, rate_limit=True)
                    raise RateLimitError("Rate limit exceeded")
                if response.status != 200:
                    self.metrics.record_api_call(False)
                    raise Exception(f"API request failed: {await response.text()}")
                    
                result = await response.json()
                if "error" in result:
                    self.metrics.record_api_call(False)
                    raise Exception(f"API error: {result['error']}")
                    
                self.metrics.record_api_call(True)
                return result["result"]
        finally:
            duration = time.time() - start_time
            self.metrics.record_processing_time(duration)
            
    async def get_instruments(self, currency: str = "BTC", kind: str = "option") -> List[dict]:
        """Get all available instruments"""
        params = {
            "currency": currency,
            "kind": kind,
            "expired": False
        }
        return await self._make_request("public/get_instruments", params)
        
    async def get_order_book(self, instrument_name: str, depth: int = 3) -> dict:
        """Get order book for an instrument"""
        params = {
            "instrument_name": instrument_name,
            "depth": depth
        }
        data = await self._make_request("public/get_order_book", params)
        self.validator.validate_order_book(data)
        return data

class OptionsDataStore:
    def __init__(self, base_path: str = None):
        self.base_path = Path(base_path or os.getenv('BASE_SAVE_PATH', "data"))
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.data_buffer = DataBuffer()
        self.api = DeribitAPI()
        self.instruments: Dict[str, List[str]] = {}  # Changed to dict to store instruments by currency
        self.metrics = Metrics()
        
        # Define currency groups
        self.native_currencies = ["BTC", "ETH"]
        self.usdc_currencies = ["SOL", "BNB", "XRP"]
        
        # Define expiration times (in UTC)
        self.expiration_times = {
            'daily': '08:00',  # Daily options expire at 08:00 UTC
            'weekly': '08:00',  # Weekly options expire at 08:00 UTC on Fridays
            'monthly': '08:00',  # Monthly options expire at 08:00 UTC on last Friday
            'quarterly': '08:00'  # Quarterly options expire at 08:00 UTC on last Friday
        }
        # Track when instruments were last reloaded
        self.last_instrument_reload_date: Optional[date] = None
        
    async def initialize(self):
        """Initialize the data store and create necessary directories"""
        await self.api.initialize()
        self.base_path.mkdir(parents=True, exist_ok=True)
        await self.load_instruments()
        
    async def initialize(self):
        """Initialize the data store and create necessary directories."""
        await self.api.initialize()
        self.base_path.mkdir(parents=True, exist_ok=True)
        await self.load_instruments()
        # Set initial reload date based on current UTC date if after 08:00 UTC
        current_utc = datetime.now(timezone.utc)
        if current_utc.hour >= 8:
            self.last_instrument_reload_date = current_utc.date()
            
    async def load_instruments(self):
        """Load all available instruments from Deribit"""
        try:
            # Get instruments for native currencies
            for currency in self.native_currencies:
                instruments = await self.api.get_instruments(currency=currency)
                self.instruments[currency] = [
                    instrument["instrument_name"]
                    for instrument in instruments
                    if instrument["is_active"]
                ]
                logger.info(f"Loaded {len(self.instruments[currency])} {currency} instruments")
            
            # Get USDC instruments and filter for specific assets
            usdc_instruments = await self.api.get_instruments(currency="USDC")
            for asset in self.usdc_currencies:
                self.instruments[asset] = [
                    instrument["instrument_name"]
                    for instrument in usdc_instruments
                    if instrument["is_active"] and asset in instrument["instrument_name"]
                ]
                logger.info(f"Loaded {len(self.instruments[asset])} {asset} instruments from USDC")
            
        except Exception as e:
            logger.error(f"Error loading instruments: {e}")
            raise
            
    def classify_option(self, expiry_date, current_datetime: Optional[datetime] = None) -> str:
        """
        Classify an option based on its time to expiry using Deribit's conventions.

        Daily Options (for options expiring in less than 72 hours):
        - 0DTE: Expires in less than 24 hours.
        - 1DTE: Expires in less than 48 hours.
        - 2DTE: Expires in less than 72 hours.
        
        For expiries of 72 hours or more:
        - If the expiry falls on a Friday:
            - If it is the last Friday of the month:
                - Return 'Quarterly' if in March, June, September, or December.
                - Otherwise, return 'Monthly'.
            - Otherwise, return 'Weekly'.
        - Otherwise, fall back to days-to-expiry ranges:
            - 2 to 7 days: 'Weekly'
            - 8 to 30 days: 'Monthly'
            - 31 to 92 days: 'Quarterly'
            - Over 92 days: 'LEAPS'
        
        Raises:
            ValueError: If the expiry_datetime is in the past.
        """
        # Handle case where expiry_date is a date object
        if isinstance(expiry_date, date) and not isinstance(expiry_date, datetime):
            # Convert to datetime at 08:00 UTC
            expiry_datetime = datetime.combine(
                expiry_date, 
                datetime.strptime(self.expiration_times['daily'], "%H:%M").time(),
                tzinfo=timezone.utc
            )
        else:
            expiry_datetime = expiry_date  # It's already a datetime
            
        # Ensure current_datetime is a datetime with timezone
        current_datetime = current_datetime or datetime.now(timezone.utc)
        if isinstance(current_datetime, date) and not isinstance(current_datetime, datetime):
            current_datetime = datetime.combine(current_datetime, datetime.min.time(), tzinfo=timezone.utc)
            
        time_diff = expiry_datetime - current_datetime
        if time_diff.total_seconds() < 0:
            raise ValueError("Expiry datetime is in the past")
        
        # Calculate total hours to expiry
        hours = time_diff.total_seconds() / 3600.0

        # Daily classification based on exact hours
        if hours < 24:
            return '0DTE'
        elif hours < 48:
            return '1DTE'
        elif hours < 72:
            return '2DTE'
        
        # For options expiring in 72 hours or more, use Friday-based or fallback rules.
        dte = time_diff.days  # floor of days remaining
        expiry_date = expiry_datetime.date() if isinstance(expiry_datetime, datetime) else expiry_datetime
        
        if expiry_date.weekday() == 4:  # Check if expiry is on a Friday
            if is_last_friday(expiry_date):
                if expiry_date.month in {3, 6, 9, 12}:
                    return 'Quarterly'
                else:
                    return 'Monthly'
            else:
                return 'Weekly'
        
        # Fallback classification using days-to-expiry
        if 2 <= dte <= 7:
            return 'Weekly'
        elif 8 <= dte <= 30:
            return 'Monthly'
        elif 31 <= dte <= 92:
            return 'Quarterly'
        else:
            return 'LEAPS'
        
    def get_storage_path(self, underlying: str, expiry_type: str, expiry_date: date) -> Path:
        """
        Generate the storage path for an instrument.
        The folder structure is:
        base_path / underlying / expiry_type / formatted_expiry_folder
        where formatted_expiry_folder is e.g. 'XRP_USDC-25APR25'.
        The CSV file (named with the instrument) will be stored in that folder.
        """
        base_path = self.base_path / underlying
        expiry_path = base_path / expiry_type
        formatted_expiry_folder = f"{underlying}-{expiry_date.strftime('%d%b%y').upper()}"
        return expiry_path / formatted_expiry_folder
   
    async def fetch_instrument_data(self, instrument: str) -> dict:
        """Fetch data for a specific instrument from Deribit"""
        try:
            start_time = time.time()
            order_book = await self.api.get_order_book(instrument)
            
            data = {
                # Timestamps
                "timestamp": datetime.fromtimestamp(order_book["timestamp"] / 1000).isoformat(),
                
                # Instrument Info
                "instrument_name": order_book["instrument_name"],
                "state": order_book["state"],  # open/closed
                
                # Bid / Ask Information
                "best_bid_price": order_book["best_bid_price"],
                "best_bid_amount": order_book["best_bid_amount"],
                "bid_iv": order_book["bid_iv"],
                
                "best_ask_price": order_book["best_ask_price"],
                "best_ask_amount": order_book["best_ask_amount"],
                "ask_iv": order_book["ask_iv"],

                # OPTIONAL: If you really need full bids/asks depth, convert them to JSON strings
                "bids_json": json.dumps(order_book["bids"]),
                "asks_json": json.dumps(order_book["asks"]),
                
                # Greeks (flattened)
                "delta": order_book["greeks"]["delta"],
                "gamma": order_book["greeks"]["gamma"],
                "theta": order_book["greeks"]["theta"],
                "vega": order_book["greeks"]["vega"],
                "rho": order_book["greeks"]["rho"],
                
                # Volatility and Pricing (Options only)
                "mark_iv": order_book["mark_iv"],
                "mark_price": order_book["mark_price"],
                "underlying_price": order_book["underlying_price"],
                "interest_rate": order_book["interest_rate"],
                
                # Other Prices
                "last_price": order_book["last_price"],
                "index_price": order_book["index_price"],
                "delivery_price": order_book.get("delivery_price"),
                "settlement_price": order_book.get("settlement_price"),
                "max_price": order_book["max_price"],
                "min_price": order_book["min_price"],
                
                # Open Interest
                "open_interest": order_book["open_interest"],
                
                # Stats (flattened)
                "high_24h": order_book["stats"]["high"],
                "low_24h": order_book["stats"]["low"],
                "price_change_pct_24h": order_book["stats"]["price_change"],
                "volume_24h": order_book["stats"]["volume"],
                "volume_usd_24h": order_book["stats"]["volume_usd"],
                
                # Underlying Info
                "underlying_index": order_book["underlying_index"]
            }

            duration = time.time() - start_time
            self.metrics.record_processing_time(duration)
            return data
            
        except Exception as e:
            logger.error(f"Error fetching data for {instrument}: {e}")
            raise
        
    async def process_instrument(self, instrument: str) -> bool:
        """Process a single instrument and buffer its data. Returns True if successful, else False."""
        try:
            data = await self.fetch_instrument_data(instrument)
            await self.data_buffer.add(instrument, data)
            return True
        except Exception as e:
            logger.error(f"Error processing instrument {instrument}: {e}")
            return False
            
    async def process_instrument_batch(self, instruments: List[str]) -> Tuple[List[str], int]:
        """Process a batch of instruments concurrently.
        Returns a tuple: (list of failed instrument names, count of successful instruments).
        """
        tasks = [asyncio.create_task(self.process_instrument(inst)) for inst in instruments]
        results = await asyncio.gather(*tasks)
        failed = []
        success_count = 0
        for inst, result in zip(instruments, results):
            if result is True:
                success_count += 1
            else:
                failed.append(inst)
        return failed, success_count
        
    async def process_instruments_in_chunks(self):
        """Process instruments in chunks, aggregate successes, and log per-currency results."""
        for currency, instruments in self.instruments.items():
            total_instruments = len(instruments)
            remaining_instruments = instruments.copy()
            total_success = 0
            max_attempts = 3  # Maximum attempts per instrument per cycle
            attempt = 1
            while remaining_instruments and attempt <= max_attempts:
                logger.info(f"Processing {len(remaining_instruments)} {currency} instruments, attempt {attempt}")
                failed_instruments = []
                batch_success = 0
                for i in range(0, len(remaining_instruments), BATCH_SIZE):
                    chunk = remaining_instruments[i:i + BATCH_SIZE]
                    failed, success_count = await self.process_instrument_batch(chunk)
                    failed_instruments.extend(failed)
                    batch_success += success_count
                total_success += batch_success
                if failed_instruments:
                    logger.error(f"Retrying failed instruments for {currency}: {failed_instruments}")
                remaining_instruments = failed_instruments
                attempt += 1

            if remaining_instruments:
                logger.error(f"Cycle complete for {currency}: {total_success} instruments processed successfully, "
                            f"{len(remaining_instruments)} instruments failed after {max_attempts} attempts")
            else:
                logger.info(f"Cycle complete for {currency}: All {total_instruments} instruments processed successfully")
            # Optionally, log final instrument-level success rate
            success_rate = total_success / total_instruments if total_instruments > 0 else 0
            logger.info(f"Final success rate for {currency}: {success_rate:.2%}")
            
    async def write_buffered_data(self):
        """Write buffered data to CSV files (one CSV per instrument, appending rows)"""
        for instrument in list(self.data_buffer.buffer.keys()):
            df = await self.data_buffer.flush(instrument)
            if df is not None:
                try:
                    # Parse instrument details from instrument name (e.g., "XRP_USDC-25APR25-2d4-P")
                    parts = instrument.split("-")
                    underlying = parts[0]
                    expiry_str = parts[1]
                    expiry_date = datetime.strptime(expiry_str, "%d%b%y").date()
                    
                    current_datetime = datetime.now(timezone.utc)
                    expiry_type = self.classify_option(expiry_date, current_datetime)
                    
                    # Get the storage folder and ensure it exists
                    storage_path = self.get_storage_path(underlying, expiry_type, expiry_date)
                    storage_path.mkdir(parents=True, exist_ok=True)
                    
                    # Set the file path to one CSV file per instrument (e.g., "XRP_USDC-25APR25-2d4-P.csv")
                    file_path = storage_path / f"{instrument}.csv"
                    
                    # Append data to the CSV, writing header only if the file doesn't exist
                    df.to_csv(file_path, mode='a', header=not file_path.exists(), index=False)
                    #logger.info(f"Wrote {len(df)} records to {file_path}")
                    
                except Exception as e:
                    logger.error(f"Error writing data for {instrument}: {e}")
           
    async def log_metrics(self):
        """Log current metrics"""
        api_stats = self.api.metrics.get_stats()
        store_stats = self.metrics.get_stats()
        
        logger.info("API Metrics:")
        logger.info(f"  Total API calls: {api_stats['api_calls']}")
        logger.info(f"  Success rate: {api_stats['success_rate']:.2%}")
        logger.info(f"  Failure rate: {api_stats['failure_rate']:.2%}")
        logger.info(f"  Retry rate: {api_stats['retry_rate']:.2%}")
        logger.info(f"  Rate limit hits: {api_stats['rate_limit_rate']:.2%}")
        logger.info(f"  Average processing time: {api_stats['avg_processing_time']:.3f}s")
        logger.info(f"  P95 processing time: {api_stats['p95_processing_time']:.3f}s")
        
        logger.info("Store Metrics:")
        logger.info(f"  Average processing time: {store_stats['avg_processing_time']:.3f}s")
        logger.info(f"  P95 processing time: {store_stats['p95_processing_time']:.3f}s")
        
        # Reset stats for next cycle
        self.api.metrics.reset()
        self.metrics.reset()    
    
    def s3_sync_and_cleanup(self, folder: Path = None):
        """
        Upload CSV files from the specified folder (or self.base_path by default)
        to the S3 bucket and then delete the local folder.
        """
        s3_bucket = os.getenv("S3_BUCKET")
        if not s3_bucket:
            logger.error("S3_BUCKET is not defined in the environment variables.")
            return

        s3 = boto3.client("s3")
        target_path = folder or self.base_path

        # Iterate through each currency folder
        for currency_folder in target_path.iterdir():
            if not currency_folder.is_dir():
                continue

            for expiry_type_folder in currency_folder.iterdir():
                if not expiry_type_folder.is_dir():
                    continue

                for expiry_folder in expiry_type_folder.iterdir():
                    if not expiry_folder.is_dir():
                        continue

                    for csv_file in expiry_folder.glob("*.csv"):
                        s3_key = f"{currency_folder.name}/{expiry_type_folder.name}/{expiry_folder.name}/{csv_file.name}"
                        try:
                            s3.upload_file(str(csv_file), s3_bucket, s3_key)
                            logger.info(f"Uploaded {csv_file} to s3://{s3_bucket}/{s3_key}")
                        except Exception as e:
                            logger.error(f"Failed to upload {csv_file}: {e}")
                            continue

                    try:
                        shutil.rmtree(expiry_folder)
                        logger.info(f"Deleted local folder: {expiry_folder}")
                    except Exception as e:
                        logger.error(f"Failed to delete folder {expiry_folder}: {e}")
        
    def rotate_data_directory(self):
        """
        Renames the current BASE_SAVE_PATH directory to a timestamped folder,
        then creates a new empty directory at the original location.
        Returns the path of the rotated folder.
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        rotated_path = self.base_path.parent / f"data_{timestamp}"
        try:
            # Rename the current folder atomically
            self.base_path.rename(rotated_path)
            logger.info(f"Rotated data folder: {self.base_path} -> {rotated_path}")
            # Recreate the original folder for new data
            self.base_path.mkdir(parents=True, exist_ok=True)
            return rotated_path
        except Exception as e:
            logger.error(f"Failed to rotate data directory: {e}")
            return None    
    
    async def run_cycle(self):
        """Perform a single 5-minute data collection cycle."""
        # Calculate the next 5-minute boundary
        now = datetime.now(timezone.utc)
        minutes_remainder = now.minute % 5
        minutes_to_next = (5 - minutes_remainder) if minutes_remainder != 0 else 5
        next_run = (now + timedelta(minutes=minutes_to_next)).replace(second=0, microsecond=0)
        sleep_seconds = (next_run - now).total_seconds()
        logger.info(f"Sleeping for {sleep_seconds:.2f} seconds until {next_run}")
        await asyncio.sleep(sleep_seconds)

        # After sleep, check current time
        now = datetime.now(timezone.utc)
        # Special handling if it's exactly 08:00 UTC
        if now.hour == 8 and now.minute == 0:
            logger.info("08:00 UTC cycle detected. Reloading instruments for the new day.")
            # Optional: slight delay (a few seconds) to ensure new instruments are in place
            await asyncio.sleep(5)
            await self.load_instruments()
            
        # Continue with the normal fetch process
        # Normal 5-minute cycle
        cycle_start = time.time()
        await self.process_instruments_in_chunks()
        await self.write_buffered_data()
        await self.log_metrics()
        cycle_duration = time.time() - cycle_start
        logger.info(f"Cycle completed in {cycle_duration:.2f} seconds")

    async def run(self):
        """Main loop for local data collection (without S3 sync)"""
        await self.initialize()
        try:
            while True:
                await self.run_cycle()
        finally:
            await self.close()  

    async def close(self):
        """Cleanup resources when closing the store"""
        try:
            # Close API connections
            if hasattr(self, 'api') and self.api:
                await self.api.close()
            
            # Flush any remaining data
            for instrument in list(self.data_buffer.buffer.keys()):
                df = await self.data_buffer.flush(instrument)
                if df is not None:
                    try:
                        parts = instrument.split("-")
                        underlying = parts[0]
                        expiry_str = parts[1]
                        expiry_date = datetime.strptime(expiry_str, "%d%b%y").date()
                        expiry_type = self.classify_option(expiry_date)
                        storage_path = self.get_storage_path(underlying, expiry_type, expiry_date)
                        storage_path.mkdir(parents=True, exist_ok=True)
                        file_path = storage_path / f"{instrument}.csv"
                        df.to_csv(file_path, mode='a', header=not file_path.exists(), index=False)
                    except Exception as e:
                        logger.error(f"Error writing final data for {instrument}: {e}")
            
            logger.info("Successfully closed OptionsDataStore")
        except Exception as e:
            logger.error(f"Error during OptionsDataStore cleanup: {e}")
            raise

async def main():
    store = OptionsDataStore()
    await store.run()

if __name__ == "__main__":
    asyncio.run(main())