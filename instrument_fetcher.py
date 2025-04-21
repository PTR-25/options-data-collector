import asyncio
import json
import logging
from typing import List

import websockets

try:
    from ws import WS_URL
except ImportError:
    logging.warning("Could not import WS_URL from ws.py, using default Deribit URL.")
    WS_URL = "wss://www.deribit.com/ws/api/v2"

logger = logging.getLogger(__name__)

# Define supported coins and their possible instrument prefixes
SUPPORTED_COINS = {
    "BTC": ["BTC"],
    "ETH": ["ETH"],
    "SOL": ["SOL_USDC", "SOL"],  # Support both formats
    "XRP": ["XRP_USDC", "XRP"],
    "BNB": ["BNB_USDC", "BNB"],
    "PAXG" : ["PAXG_USDC", "PAXG"]
}

def classify_instrument(instrument_name: str) -> str:
    """
    Determines which coin an instrument belongs to based on its prefix.
    Returns the standardized coin name (e.g., "SOL" for "SOL_USDC-...")
    """
    for coin, prefixes in SUPPORTED_COINS.items():
        if any(instrument_name.startswith(prefix) for prefix in prefixes):
            return coin
    return None

async def fetch_option_instruments() -> List[str]:
    """
    Fetches ALL active option instruments from Deribit and filters by supported coins.
    
    Returns:
        A list of ticker channel names (e.g., ["ticker.BTC-25APR25-80000-C.100ms"]).
        Returns an empty list if connection fails or no instruments are found.
    """
    uri = WS_URL
    all_channels = []
    instruments_by_coin = {coin: [] for coin in SUPPORTED_COINS.keys()}

    try:
        # Increase max_size to 10MB to handle large responses
        async with websockets.connect(uri, ping_timeout=60, max_size=10 * 1024 * 1024) as websocket:
            logger.info(f"Connected to {uri} for instrument fetching.")
            
            # Fetch ALL instruments at once using currency="any"
            request_id = hash(f"all-{asyncio.get_event_loop().time()}")
            request = {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "public/get_instruments",
                "params": {
                    "currency": "any",  # Get all currencies
                    "kind": "option",
                    "expired": False
                }
            }
            
            logger.info("Fetching all active option instruments...")
            await websocket.send(json.dumps(request))

            try:
                raw = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                data = json.loads(raw)

                if data.get("id") == request_id:
                    if 'result' in data:
                        instruments = data['result']
                        
                        # Classify each instrument
                        for inst in instruments:
                            if inst.get('kind') != 'option':
                                continue
                                
                            name = inst['instrument_name']
                            coin = classify_instrument(name)
                            
                            if coin:
                                channel = f"ticker.{name}.100ms"
                                instruments_by_coin[coin].append(channel)
                            else:
                                logger.debug(f"Ignoring unsupported instrument: {name}")

                        # Log summary for each coin
                        for coin, channels in instruments_by_coin.items():
                            logger.info(f"Found {len(channels)} {coin} option instruments")
                            all_channels.extend(channels)

                    elif 'error' in data:
                        logger.error(f"API error fetching instruments: {data.get('error')}")
                    else:
                        logger.warning(f"Received unexpected response format: {data}")

            except asyncio.TimeoutError:
                logger.error("Timeout waiting for instrument response.")
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode response: {e}")
            except Exception as e:
                logger.exception(f"Unexpected error processing response: {e}")

    except websockets.exceptions.ConnectionClosedOK:
        logger.info("WebSocket connection closed gracefully after fetching instruments.")
    except (websockets.exceptions.WebSocketException, OSError, asyncio.TimeoutError) as e:
        logger.error(f"WebSocket connection or fetch failed: {e}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred during instrument fetching: {e}")

    total = len(all_channels)
    if total > 0:
        logger.info(f"Successfully fetched {total} total option channels")
        # Log distribution
        for coin in SUPPORTED_COINS.keys():
            count = len(instruments_by_coin[coin])
            if count > 0:
                logger.info(f"  {coin}: {count} instruments ({count/total*100:.1f}%)")
    else:
        logger.warning("No option channels were found!")

    return all_channels

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("Instrument fetcher module loaded.")

