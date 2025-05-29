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
        self._thread = threading.Thread(
            target=self.loop.run_until_complete,
            args=(self._run(),),
            daemon=True,
        )
        logger.info(f"[WSClient] starting thread {self._thread.name}")
        self._thread.start()

    def stop(self):
        self._stop = True
        # Tell the event loop to quit immediately
        self.loop.call_soon_threadsafe(self.loop.stop)
        # And wait for the thread to exit
        if getattr(self, "_thread", None):
            logger.info(f"[WSClient] waiting for thread {self._thread.name} to finish")
            self._thread.join(timeout=300)
            logger.info(f"[WSClient] thread {self._thread.name} has exited")
            
    # helper for WsManager to send subscribe/unsubscribe
    def send_json_threadsafe(self, payload: dict):
        """Schedule a ws.send_json() on this client’s loop."""
        # stash the ws on self in _run() so we can refer to it here
        asyncio.run_coroutine_threadsafe(self._ws.send_json(payload), self.loop)

    async def _run(self):
        """
        Main execution coroutine for this WebSocket client, running in its dedicated thread.
        Handles authentication, connection establishment, message processing, heartbeats,
        and graceful shutdown. Designed to run indefinitely until self._stop is True.
        """
        # Thread-local logger prefix for easier identification in logs
        log_prefix = f"[ClientThread-{self._thread.name if hasattr(self, '_thread') else 'Unknown'}]"
        logger.info(f"{log_prefix} Starting _run method.")

        # --- Configuration for cleanup timeouts ---
        # These timeouts are used within the finally blocks during shutdown or error handling.
        # They prevent cleanup operations from blocking indefinitely.
        # If these are too long relative to the self._thread.join(timeout=X) in stop(),
        # the thread might not terminate within the join timeout, leading to a zombie.
        # If these are too short, cleanup might be abrupt.
        WS_CLOSE_TIMEOUT_SECONDS = 2.0  # Time to wait for WebSocket close handshake
        SESSION_CLOSE_TIMEOUT_SECONDS = 2.0 # Time to wait for aiohttp.ClientSession close
        PING_TASK_AWAIT_TIMEOUT_SECONDS = 2.0 # Time to wait for a cancelled ping_task to finish
        CONNECT_TIMEOUT_SECONDS = 120.0 # Time to wait for sess.ws_connect to succeed

        sess: Optional[aiohttp.ClientSession] = None
        try:
            logger.info(f"{log_prefix} Creating aiohttp.ClientSession.")
            # Create a session that will be used for the lifetime of this client thread.
            # Timeouts here are general; specific operations can override them.
            sess = aiohttp.ClientSession(
                loop=self.loop,
                timeout=aiohttp.ClientTimeout(total=120) # General timeout for session operations
            )

            logger.info(f"{log_prefix} Performing initial authentication.")
            await self._authenticate_full(sess) # This also schedules token refresh
            logger.info(f"{log_prefix} Initial authentication successful.")

            # Main loop: attempts to connect and maintain connection until self._stop is True.
            while not self._stop:
                ws: Optional[aiohttp.ClientWebSocketResponse] = None
                ping_task: Optional[asyncio.Task] = None
                connection_attempt_successful = False

                try:
                    # --- Connection Attempt Phase ---
                    logger.info(f"{log_prefix} Attempting WebSocket connection to {self.ws_url}.")
                    # Use asyncio.wait_for to apply a specific timeout to the connection attempt.
                    ws = await asyncio.wait_for(
                        sess.ws_connect(self.ws_url),
                        timeout=CONNECT_TIMEOUT_SECONDS
                    )
                    self._ws = ws # Store for send_json_threadsafe
                    connection_attempt_successful = True
                    logger.info(f"{log_prefix} WebSocket connection established successfully to {self.ws_url}.")

                    # --- Post-Connection Setup ---
                    logger.info(f"{log_prefix} Setting up server heartbeat.")
                    hb_req_id = self._next_id()
                    await ws.send_json({
                        "jsonrpc": "2.0", "id": hb_req_id,
                        "method": "public/set_heartbeat",
                        "params": {"interval": self.heartbeat_interval}
                    })
                    logger.info(f"{log_prefix} Sent public/set_heartbeat (id: {hb_req_id}, interval: {self.heartbeat_interval}s).")

                    logger.info(f"{log_prefix} Subscribing to {len(self.channels)} channels.")
                    sub_id = self._next_id()
                    await ws.send_json({
                        "jsonrpc": "2.0", "id": sub_id,
                        "method": "public/subscribe",
                        "params": {"channels": self.channels}
                    })
                    # Note: Confirmation for subscription will be handled in the message loop.

                    logger.info(f"{log_prefix} Starting client-side ping task.")
                    ping_task = self.loop.create_task(self._ping(ws)) # _ping needs the active ws

                    # --- Message Receiving Loop ---
                    logger.info(f"{log_prefix} Starting main message receive loop.")
                    async for msg in ws:
                        if self._stop: # Check for stop signal before processing each message
                            logger.info(f"{log_prefix} Stop signal detected in message loop. Breaking.")
                            break # Exit message loop to proceed to cleanup

                        # --- Message Processing ---
                        if msg.type == WSMsgType.TEXT:
                            try:
                                d = msg.json()
                                # logger.debug(f"{log_prefix} Received TEXT message: {d}") # Very verbose

                                if d.get("method") == "heartbeat":
                                    params = d.get("params", {})
                                    # logger.debug(f"{log_prefix} Server heartbeat received: {params}")
                                    if self.on_hb: self.on_hb(params)
                                    else: logger.warning(f"{log_prefix} Received heartbeat but no on_hb callback.")
                                    test_id = self._next_id()
                                    await ws.send_json({
                                        "jsonrpc": "2.0", "id": test_id,
                                        "method": "public/test", "params": {}
                                    })
                                    # logger.debug(f"{log_prefix} Responded to heartbeat with public/test (id: {test_id}).")

                                elif d.get("method") == "subscription":
                                    ch = d["params"].get("channel")
                                    data = d["params"]["data"]
                                    if ch:
                                        # logger.debug(f"{log_prefix} Subscription data for channel {ch}.")
                                        self.on_tick(ch, data) # Assuming on_tick is thread-safe or quick

                                elif "id" in d and d.get("result") == "pong":
                                    # logger.debug(f"{log_prefix} Pong received for id {d['id']}.")
                                    self._pending_pings.discard(d["id"])

                                elif "id" in d and "result" in d: # General result for a request
                                    if d.get("id") == sub_id:
                                        logger.info(f"{log_prefix} Subscription confirmation (id: {sub_id}): {d['result']}")
                                    elif d.get("id") == hb_req_id:
                                        logger.info(f"{log_prefix} Set_heartbeat confirmation (id: {hb_req_id}): {d['result']}")
                                    # else: logger.debug(f"{log_prefix} Received result for id {d['id']}: {d['result']}")

                                # else: logger.debug(f"{log_prefix} Unhandled JSON message structure: {d}")

                            except json.JSONDecodeError:
                                logger.warning(f"{log_prefix} Received non-JSON TEXT message: {msg.data}")
                            except Exception as e_msg_proc: # Catch errors during individual message processing
                                logger.exception(f"{log_prefix} Error processing message: {e_msg_proc}. Message data: {msg.data}")
                                # Depending on severity, could break, but often better to log and continue
                                # For now, let's assume most processing errors are not fatal for the connection.
                                # If a specific error IS fatal, it should re-raise or break.

                        elif msg.type == WSMsgType.PONG:
                            logger.debug(f"{log_prefix} Received WebSocket PONG frame: {msg.data}")
                        elif msg.type == WSMsgType.PING:
                            logger.debug(f"{log_prefix} Received WebSocket PING frame: {msg.data}. Responding with PONG.")
                            await ws.pong(msg.data)
                        elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSED):
                            logger.warning(f"{log_prefix} WebSocket connection closed or error state. Type: {msg.type}, Data: {msg.data}")
                            break # Exit message loop, will trigger cleanup in finally
                        # else: logger.debug(f"{log_prefix} Received unhandled WebSocket message type: {msg.type}")

                    # --- End of Message Receiving Loop ---
                    if not self._stop: # If loop exited for reasons other than self._stop (e.g. server closed connection)
                        logger.info(f"{log_prefix} Message loop exited (e.g., server closed connection).")
                    # If self._stop is True, it was logged before breaking.

                except asyncio.TimeoutError: # From wait_for(sess.ws_connect(...))
                    logger.error(f"{log_prefix} Timeout ({CONNECT_TIMEOUT_SECONDS}s) during WebSocket connection attempt.")
                    # No ws object was assigned, or it's in an unknown state. Cleanup will be handled in finally.
                except ClientError as e_client: # aiohttp specific client errors during connect/setup
                    logger.error(f"{log_prefix} aiohttp.ClientError during connection/setup: {e_client}")
                except asyncio.CancelledError: # If this specific connection attempt task is cancelled
                    logger.info(f"{log_prefix} Connection attempt task cancelled.")
                    self._stop = True # Signal the main _run loop to terminate
                    raise # Re-raise to be caught by the outer CancelledError handler
                except Exception as e_conn_attempt: # Other unexpected errors during connection attempt/setup
                    logger.exception(f"{log_prefix} Unexpected error during connection attempt/setup: {e_conn_attempt}")
                finally:
                    # --- Cleanup for a single connection attempt ---
                    # This block executes whether the connection succeeded, failed, or was cancelled.
                    logger.debug(f"{log_prefix} Entering inner finally block for connection attempt.")

                    if ping_task and not ping_task.done():
                        logger.info(f"{log_prefix} Cancelling active ping_task.")
                        ping_task.cancel()
                        try:
                            # Wait briefly for the ping_task to acknowledge cancellation and finish.
                            await asyncio.wait_for(ping_task, timeout=PING_TASK_AWAIT_TIMEOUT_SECONDS)
                            logger.debug(f"{log_prefix} Ping_task successfully awaited after cancellation.")
                        except asyncio.CancelledError:
                            logger.debug(f"{log_prefix} Ping_task was cancelled as expected.")
                        except asyncio.TimeoutError:
                            logger.warning(f"{log_prefix} Timeout ({PING_TASK_AWAIT_TIMEOUT_SECONDS}s) waiting for cancelled ping_task to finish. It might linger.")
                        except Exception as e_ping_await:
                            logger.error(f"{log_prefix} Error awaiting cancelled ping_task: {e_ping_await}")

                    if ws and not ws.closed:
                        logger.info(f"{log_prefix} Attempting to close WebSocket connection (timeout: {WS_CLOSE_TIMEOUT_SECONDS}s).")
                        try:
                            await asyncio.wait_for(ws.close(), timeout=WS_CLOSE_TIMEOUT_SECONDS)
                            logger.info(f"{log_prefix} WebSocket connection closed successfully.")
                        except asyncio.TimeoutError:
                            logger.warning(f"{log_prefix} Timeout ({WS_CLOSE_TIMEOUT_SECONDS}s) closing WebSocket. Connection may not be cleanly closed on server-side. This can contribute to zombie client if overall stop() timeout is too short.")
                        except Exception as e_ws_close:
                            logger.error(f"{log_prefix} Error closing WebSocket: {e_ws_close}")
                    elif connection_attempt_successful and not ws: # Should not happen if logic is correct
                         logger.error(f"{log_prefix} Inconsistency: connection_attempt_successful is True but ws is None or closed prematurely in finally.")


                    self._ws = None # Clear the shared reference, as this ws is no longer valid.
                    logger.debug(f"{log_prefix} Exiting inner finally block.")

                # --- After a connection attempt (successful or failed) ---
                if self._stop:
                    logger.info(f"{log_prefix} Stop signal detected after connection attempt's finally block. Exiting main connection loop.")
                    break # Exit the `while not self._stop` loop.

                # If not stopping, it means a connection attempt failed or an active connection dropped.
                # Wait before retrying.
                logger.info(f"{log_prefix} Connection attempt concluded or connection lost. Waiting {self.reconnect_interval}s before next attempt.")
                try:
                    await asyncio.sleep(self.reconnect_interval)
                except asyncio.CancelledError: # If sleep is cancelled by loop.stop()
                    logger.info(f"{log_prefix} Reconnect sleep cancelled. Setting stop flag.")
                    self._stop = True # Ensure main loop terminates

            # --- End of `while not self._stop` loop ---
            logger.info(f"{log_prefix} Exited main connection loop (`while not self._stop`).")

        except asyncio.CancelledError:
            # This catches CancelledError raised directly in _run (e.g., during initial auth, or if re-raised from inner try)
            # This is the expected path when self.loop.stop() is called.
            logger.info(f"{log_prefix} _run task was cancelled (likely by loop.stop() during operation or auth).")
            self._stop = True # Ensure the flag is set for consistent state.
        except Exception as e_outer_run:
            # Catches unexpected errors in the outermost scope of _run (e.g., sess creation, initial auth)
            logger.exception(f"{log_prefix} Unhandled critical exception in _run's outer scope: {e_outer_run}")
            self._stop = True # Critical error, signal to stop.
        finally:
            # --- Final cleanup for the entire _run method ---
            # This block executes when _run is about to return, ensuring the session is closed.
            logger.info(f"{log_prefix} Entering _run's outer finally block for session cleanup.")
            if sess and not sess.closed:
                logger.info(f"{log_prefix} Attempting to close aiohttp.ClientSession (timeout: {SESSION_CLOSE_TIMEOUT_SECONDS}s).")
                try:
                    await asyncio.wait_for(sess.close(), timeout=SESSION_CLOSE_TIMEOUT_SECONDS)
                    logger.info(f"{log_prefix} aiohttp.ClientSession closed successfully.")
                except asyncio.TimeoutError:
                    logger.warning(f"{log_prefix} Timeout ({SESSION_CLOSE_TIMEOUT_SECONDS}s) closing aiohttp.ClientSession. Resources might not be fully released. This can contribute to zombie client if overall stop() timeout is too short.")
                except Exception as e_sess_close:
                    logger.error(f"{log_prefix} Error closing aiohttp.ClientSession: {e_sess_close}")
            else:
                logger.debug(f"{log_prefix} aiohttp.ClientSession was None or already closed.")

            logger.info(f"{log_prefix} _run method is fully exiting. Thread will now terminate.")

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
        self.all_channels = list(all_channels) # Ensure it's a mutable list copy
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
        # No specific pass needed here, original was fine

    async def start(self) -> None:
        """Split channels into batches & spin up client threads."""
        if self.clients:
            logger.warning("WsManager.start() called but clients already exist. Stopping existing clients first.")
            await self.stop() # stop() will also clear self.clients

        if not self.all_channels:
            logger.info("WsManager.start() called with no channels to subscribe to.")
            return

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
            client.start() # This starts the client's thread and internal asyncio loop
        logger.info(f"Started {len(self.clients)} WebSocket client(s) for {len(self.all_channels)} channels.")
        # Give time for clients to establish connections - this is a best-effort delay
        if self.clients:
            # Adjust sleep to be slightly more dynamic, e.g., 0.5s per client, min 1s, max 5s
            sleep_duration = min(max(1.0, len(self.clients) * 0.5), 5.0)
            logger.info(f"Waiting {sleep_duration:.1f}s for client connections to establish...")
            await asyncio.sleep(sleep_duration)
            logger.info("Initial wait for client connections complete.")

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

    async def resubscribe(self, new_channels: List[str]):
        """
        Updates the channel list and restarts clients with the new channel configuration.
        The start() method (called internally) will handle stopping existing clients if necessary.
        """
        logger.info(f"Resubscribing. Old channel count: {len(self.all_channels)}, New channel count: {len(new_channels)}")

        # 1. Update the master list of channels for the manager
        self.all_channels = list(new_channels) # Ensure it's a mutable list copy

        # 2. Clear the cache as subscriptions are changing fundamentally
        with self._lock:
            self._cache.clear()
        logger.info("Cache cleared for resubscription.")

        # 3. Re-initialize and start clients with the new channel list.
        # The start() method will first call stop() if clients are running,
        # then re-batch self.all_channels and create/start new clients.
        if self.all_channels:
            await self.start() 
            logger.info(f"Resubscribe complete. Clients (re)started with {len(self.all_channels)} channels.")
        else:
            # If there are no new channels, ensure any existing clients are stopped.
            # self.start() will call self.stop() if self.clients is not empty, 
            # and then won't start new ones if self.all_channels is empty.
            # If self.clients was already empty, this effectively does nothing more than logging.
            await self.start() # This will ensure a stop if clients were running and new_channels is empty.
            logger.info("Resubscribe complete. No channels to subscribe to, all clients (were/are) stopped.")

    async def stop(self) -> None:
        """Signal all clients to shut down, wait for them to stop, and clear the client list."""
        if not self.clients:
            logger.info("WsManager.stop() called but no clients to stop.")
            return

        logger.info(f"Stopping {len(self.clients)} WebSocket client(s)...")
        # Client.stop() is synchronous and joins the thread.
        # We call them sequentially.
        for client_instance in self.clients:
            try:
                client_instance.stop()
            except Exception as e:
                logger.error(f"Error stopping client {getattr(client_instance, '_thread', {}).get('name', 'UnknownClient')}: {e}")


        # All client threads should have been joined by their respective stop() calls.
        logger.info(f"All {len(self.clients)} WebSocket client(s) signaled to stop and threads joined/handled.")
        
        # Clear the list of clients
        self.clients.clear()
        logger.info("Client list cleared.")
        # A very brief sleep can be added if there's any concern about lingering effects
        # from the threads before the manager itself might proceed with other async tasks.
        # However, client.stop() is designed to be blocking.
        await asyncio.sleep(0.1) # Optional: very short delay for any final async context switches
