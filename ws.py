# ws.py

import os
import asyncio
import logging
import threading
import json
import time
import copy  # <-- Add import
from typing import Optional, Callable, List, Dict
from urllib.parse import urlparse

import aiohttp
from aiohttp import WSMsgType, ClientError
from dotenv import load_dotenv

load_dotenv()
REST_URL = os.getenv('BASE_URL', 'https://www.deribit.com/api/v2')
CLIENT_ID = os.getenv('CLIENT_ID_1')
CLIENT_SECRET = os.getenv('CLIENT_SECRET_1')
if not CLIENT_ID or not CLIENT_SECRET:
    raise RuntimeError("Missing CLIENT_ID_1 or CLIENT_SECRET_1")

_parsed = urlparse(REST_URL)
_scheme = 'wss' if _parsed.scheme in ('https', 'wss') else 'ws'
WS_URL = f"{_scheme}://{_parsed.hostname}/ws/api/v2"

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class DeribitWebsocketClient:
    """
    Single WS connection (≤500 channels).
    Calls on_tick(channel, data) for market updates,
    and on_hb(params) for every server heartbeat.
    """
    def __init__(
        self,
        channels: List[str],
        on_tick: Callable[[str, dict], None],
        on_hb: Optional[Callable[[dict], None]] = None,
        heartbeat_interval: int = 30,
        reconnect_interval: float = 2.5,
    ):
        self.channels = channels
        self.on_tick = on_tick
        self.on_hb = on_hb
        self.heartbeat_interval = max(10, heartbeat_interval)
        self.reconnect_interval = reconnect_interval
        self.ws_url = WS_URL
        self._id = 1
        self._pending_pings = set()
        self._stop = False
        # each client gets its own loop & thread
        self.loop = asyncio.new_event_loop()

    def _next_id(self) -> int:
        i = self._id
        self._id += 1
        return i

    def start(self):
        threading.Thread(
            target=self.loop.run_until_complete,
            args=(self._run(),),
            daemon=True
        ).start()

    def stop(self):
        self._stop = True

    async def _run(self):
        sess = aiohttp.ClientSession(loop=self.loop)
        # initial authenticate
        await self._authenticate_full(sess)

        while not self._stop:
            try:
                logger.info(f"Attempting to connect to {self.ws_url}")
                ws = await sess.ws_connect(self.ws_url)
                logger.info(f"Connection established to {self.ws_url}")
                # request server heartbeat
                hb_req_id = self._next_id()
                await ws.send_json({
                    "jsonrpc": "2.0",
                    "id": hb_req_id,  # Use the stored ID here
                    "method": "public/set_heartbeat",
                    "params": {"interval": self.heartbeat_interval}
                })
                logger.info(f"Sent public/set_heartbeat request (id: {hb_req_id}) with interval {self.heartbeat_interval}")

                # subscribe
                sub_id = self._next_id()
                await ws.send_json({
                    "jsonrpc": "2.0", "id": sub_id,
                    "method": "public/subscribe",
                    "params": {"channels": self.channels}
                })

                logger.info(f"Subscribed to {len(self.channels)} channels")

                # ping loop
                ping_task = self.loop.create_task(self._ping(ws))
                logger.info("Started client-side ping task.")

                # receive loop
                logger.info("Starting main message receive loop...")
                async for msg in ws:
                    if msg.type == WSMsgType.TEXT:
                        try:
                            d = msg.json()
                            # Log *every* text message received in the main loop
                            # logger.debug(f"Received message: {d}")

                            # server heartbeat
                            if d.get("method") == "heartbeat":
                                params = d.get("params", {})
                                logger.info(f"Server heartbeat received: {params}")  # Log heartbeat detection
                                if self.on_hb:
                                    logger.debug("Calling self.on_hb callback...")
                                    self.on_hb(params)  # Call the callback passed during init (WsManager._on_hb)
                                else:
                                    logger.warning("Received heartbeat but no on_hb callback registered.")
                                # respond
                                test_id = self._next_id()
                                await ws.send_json({
                                    "jsonrpc": "2.0", "id": test_id,
                                    "method": "public/test", "params": {}
                                })
                                logger.info(f"Sent public/test response (id: {test_id})")
                                continue  # Skip other checks for this message

                            # subscription data
                            if d.get("method") == "subscription":
                                ch = d["params"].get("channel")
                                data = d["params"]["data"]
                                # logger.debug(f"Subscription data for {ch}") # Optional: very verbose
                                if ch:
                                    start_tick = time.monotonic()
                                    self.on_tick(ch, data)
                                    end_tick = time.monotonic()
                                    if end_tick - start_tick > 0.1:
                                        logger.warning(f"on_tick took {end_tick - start_tick:.2f}s for {ch}")

                            # pong reply
                            elif "id" in d and d.get("result") == "pong":
                                logger.debug(f"Pong received for id {d['id']}")
                                self._pending_pings.discard(d["id"])

                            # Handle responses to subscribe, set_heartbeat etc. if needed
                            elif "id" in d and "result" in d:
                                if d.get("id") == sub_id:
                                    logger.info(f"Received subscription confirmation (id: {sub_id}): {d['result']}")
                                # Add checks for other request IDs if necessary

                        except json.JSONDecodeError:
                            logger.warning(f"Received non-JSON message: {msg.data}")
                        except Exception as e:
                            logger.exception(f"Error processing message: {e}")
                            # Decide if error is fatal for this connection attempt
                            break  # Break inner loop on processing error to trigger reconnect

                    elif msg.type == WSMsgType.PONG:
                        logger.debug(f"Received WebSocket PONG: {msg.data}")  # aiohttp handles this mostly
                    elif msg.type == WSMsgType.PING:
                        logger.debug(f"Received WebSocket PING: {msg.data}, responding with PONG")
                        await ws.pong(msg.data)
                    elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSED):
                        logger.warning(f"WS connection closed or error state: {msg.type}, data: {msg.data}")
                        break  # Break inner loop to reconnect

                logger.info("Exited main message receive loop.")
                if ping_task and not ping_task.done():
                    ping_task.cancel()
                await ws.close()
                logger.info("WebSocket connection closed.")

            except ClientError as e:
                logger.error(f"Connection error: {e}. Reconnecting in {self.reconnect_interval}s...")
                await asyncio.sleep(self.reconnect_interval)
            except asyncio.CancelledError:
                logger.info("Client run task cancelled.")
                self._stop = True  # Ensure loop terminates
            except Exception:
                # Catch-all for unexpected errors during setup or loop
                logger.exception(f"Unhandled WS client error; reconnecting in {self.reconnect_interval}s...")
                await asyncio.sleep(self.reconnect_interval)

        logger.info("Client stopping.")
        if sess and not sess.closed:
            await sess.close()
        logger.info("Client session closed.")

    async def _ping(self, ws):
        try:
            while not self._stop:
                await asyncio.sleep(self.heartbeat_interval)
                pid = self._next_id()
                self._pending_pings.add(pid)
                await ws.send_json({
                    "jsonrpc": "2.0", "id": pid,
                    "method": "public/ping", "params": {}
                })
        except asyncio.CancelledError:
            pass

    async def _authenticate_full(self, sess: aiohttp.ClientSession):
        # initial auth with client_credentials
        await self._auth_request(sess, grant_type='client_credentials')
        # schedule first refresh
        self._schedule_refresh()

    async def _auth_request(self, sess: aiohttp.ClientSession, *, grant_type: str):
        params = {"grant_type": grant_type}
        if grant_type == 'client_credentials':
            params.update({"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET})
        else:  # refresh_token
            params['refresh_token'] = self.refresh_token
        req_id = self._next_id()
        fut = self.loop.create_future()
        async with sess.ws_connect(self.ws_url) as ws:
            await ws.send_json({"jsonrpc": "2.0", "id": req_id,
                                "method": "public/auth", "params": params})
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    d = msg.json()
                    if d.get('id') == req_id:
                        res = d.get('result')
                        self.access_token = res['access_token']
                        self.refresh_token = res['refresh_token']
                        self.token_expiry_ts = time.time() + res['expires_in']
                        return

    def _schedule_refresh(self):
        # sleep until 1 minute before expiry, then refresh
        if self.token_expiry_ts:
            delay = max(0, self.token_expiry_ts - time.time() - 60)
            print(f"Scheduling token refresh in {delay:.2f}s")
            self.loop.call_later(delay, lambda: asyncio.run_coroutine_threadsafe(
                self._refresh_task(), self.loop))

    async def _refresh_task(self):
        logger.info("Refreshing auth token using refresh_token grant")
        sess = aiohttp.ClientSession(loop=self.loop)
        try:
            await self._auth_request(sess, grant_type='refresh_token')
            self._schedule_refresh()
        except Exception:
            logger.exception("Token refresh failed; will retry on next reconnect")
        finally:
            await sess.close()

    async def _refresh_loop(self, sess: aiohttp.ClientSession):
        # fallback loop in case schedule fails
        while not self._stop:
            # sleep until a bit before expiry
            if not self.token_expiry_ts:
                await asyncio.sleep(60)
                continue
            wait = self.token_expiry_ts - time.time() - 60
            if wait > 0:
                await asyncio.sleep(wait)
            await self._auth_request(sess, grant_type='refresh_token')
            logger.info("Auth token refreshed in loop")
            self._schedule_refresh()


class WsManager:
    """
    Orchestrates N DeribitWebsocketClients in threads,
    maintains a global cache, and dispatches heartbeats.
    """
    def __init__(
        self,
        all_channels: List[str],
        max_channels_per_conn: int = 500,
        heartbeat_interval: int = 30,
        reconnect_interval: float = 2.5,
    ):
        self.all_channels = all_channels
        self.batch_size = max_channels_per_conn
        self.heartbeat_interval = heartbeat_interval
        self.reconnect_interval = reconnect_interval

        self._cache: Dict[str, dict] = {}
        self._lock = threading.Lock()
        self.clients: List[DeribitWebsocketClient] = []
        self._hb_callbacks: List[Callable[[dict], None]] = []

    def register_heartbeat(self, callback: Callable[[dict], None]) -> None:
        """Register to receive server heartbeat params."""
        self._hb_callbacks.append(callback)

    def _on_hb(self, params: dict) -> None:
        """Internal: fan‑out heartbeats to all listeners."""
        # Add logging here to confirm this intermediate step is reached
        logger.info(f"WsManager._on_hb called with params: {params}")
        for cb in self._hb_callbacks:
            try:
                logger.debug(f"Dispatching heartbeat to callback: {cb.__name__}")
                cb(params)
            except Exception:  # Catch errors in the *user's* callback
                logger.exception(f"Error in registered heartbeat listener ({cb.__name__})")

    def _on_tick(self, channel: str, data: dict) -> None:
        with self._lock:
            self._cache[channel] = data
        pass

    def start(self) -> None:
        """Split channels into batches & spin up client threads."""
        for i in range(0, len(self.all_channels), self.batch_size):
            batch = self.all_channels[i:i+self.batch_size]
            client = DeribitWebsocketClient(
                channels=batch,
                on_tick=self._on_tick,
                on_hb=self._on_hb,
                heartbeat_interval=self.heartbeat_interval,
                reconnect_interval=self.reconnect_interval,
            )
            self.clients.append(client)
            client.start()
        logger.info(f"Started {len(self.clients)} WebSocket threads")

    def get_latest(self) -> Dict[str, dict]:
        """Snapshot of the latest ticks across all channels."""
        with self._lock:
            # Return a shallow copy for read-only access
            return dict(self._cache)

    def pop_snapshot(self) -> Dict[str, dict]:
        """
        Atomically retrieves a deep copy of the current cache and clears it.

        Returns:
            A deep copy of the cache content before clearing.
        """
        with self._lock:
            snapshot = copy.deepcopy(self._cache)
            self._cache.clear()
            logger.info(f"Popped snapshot with {len(snapshot)} entries, cache cleared.")
        return snapshot

    def stop(self) -> None:
        """Signal all clients to shut down."""
        for c in self.clients:
            c.stop()
