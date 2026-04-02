from datetime import datetime
from threading import Event, Lock
from typing import Any, Dict, List, Optional, Tuple, Type, Union
import pythoncom
import time

from comtypes import COMObject, GUID
from comtypes.automation import VARIANT, VARIANT_BOOL
from comtypes.client import CreateObject

from config.quote_types import QuoteType
from src.core.error_handler import (
    RTDClientError,
    RTDConnectionError,
    RTDConnectionState,
    RTDHeartbeatError,
    RTDServerError,
    RTDUpdateError,
    handle_com_error,
    log_method_call,
    validate_connection_state
)
from src.core.logger import get_logger
from src.core.settings import SETTINGS
from src.rtd.interfaces import IRTDUpdateEvent, IRtdServer
from src.utils import cleanup, state, topic
from src.utils.quote import Quote

class RTDClient(COMObject):
    """
    Real-Time Data Client for ThinkorSwim RTD Server.
    
    This class provides a synchronous interface to the ThinkorSwim RTD Server,
    handling real-time market data subscriptions and updates.
    
    Attributes:
        _state (RTDConnectionState): Current connection state
        server (IRtdServer): COM server instance
        topics (Dict[int, Tuple[str, str]]): Active topic subscriptions
        heartbeat_interval (int): Server heartbeat interval in milliseconds
    """
    _com_interfaces_ = [IRTDUpdateEvent]

    def __init__(
        self, 
        heartbeat_ms: Optional[int] = None,
        logger: Optional[Any] = None
    ) -> None:
        """
        Initialize the RTD Client.

        Args:
            heartbeat_ms: Optional heartbeat interval in milliseconds.
                         Defaults to value from config.
            logger: Optional logger instance. If None, creates a new logger.

        Raises:
            RTDClientError: If initialization fails
        """
        super().__init__()
        
        # Initialize logger
        self.logger = logger or get_logger("RTDClient")
        
        # COM server and state
        self.server: Optional[IRtdServer] = None
        self._state = RTDConnectionState.DISCONNECTED
        self._lock = Lock()
        
        # Topic management
        self.topics: Dict[int, Tuple[str, str]] = {}
        self._topic_lock = Lock()
        self._latest_values: Dict[Tuple[str, str], Quote] = {} 
        self._value_lock = Lock() 
        
        # Heartbeat configuration
        self._heartbeat_interval = (
            heartbeat_ms or 
            SETTINGS['timing']['initial_heartbeat']
        )
        
        # Update tracking
        self._update_notify_count = 0
        self._last_refresh_time = None

        # Event signaled by native UpdateNotify callback.
        # External code (main loop) can wait on this to know when data arrived,
        # or use MsgWaitForMultipleObjects to wake on COM messages directly.
        self.data_ready = Event()

        # Event signaled when the RTD server calls Disconnect on us
        # (server-initiated disconnect, e.g. TOS shutdown).
        # Main loop monitors this to trigger reconnect.
        self.disconnected = Event()

        # Staleness detection — zombie COM connection guard.
        # If heartbeat returns healthy but no actual data arrives for
        # _data_stale_sec seconds, the connection is considered zombie
        # and reconnect is triggered.
        self._last_data_time: float = time.time()
        self._data_stale_sec: float = SETTINGS['timing'].get('data_stale_sec', 60.0)

        # Status logging
        self._startup_logged = False
        self._status_log_interval = 300.0  # 5-minute status pulse
        self._last_status_log_time: float = time.time()

        self.logger.info("RTD Client instance created")

    def __enter__(self) -> 'RTDClient':
        """
        Enter the runtime context for using the RTD client.
        
        Initializes the COM server and establishes the connection.
        
        Returns:
            RTDClient: Self reference for context manager use
            
        Raises:
            RTDServerError: If server initialization fails
        """
        self.initialize()
        return self

    @handle_com_error(RTDServerError)
    @log_method_call()
    def initialize(self) -> None:
        """
        Initialize the RTD server connection.
        
        Performs COM initialization and server startup sequence.
        Should be called before any other operations.
        
        Raises:
            RTDServerError: If server initialization fails
            RTDConnectionError: If called in invalid state
        """
        if self._state != RTDConnectionState.DISCONNECTED:
            raise RTDConnectionError(
                f"Initialization attempted in invalid state: {self._state}"
            )
            
        self._state = RTDConnectionState.CONNECTING
        self.logger.info("Starting RTD server initialization")
        
        try:
            # Initialize COM for the current thread
            pythoncom.CoInitialize()
            
            # Create COM server instance
            self.server = CreateObject(
                GUID(SETTINGS['rtd']['progid']), 
                interface=IRtdServer
            )
            self.logger.debug("COM server instance created")
            
            # Start the server
            result = self.server.ServerStart(self)
            
            if result == 1:
                self._state = RTDConnectionState.CONNECTED
                self.logger.info("Server started successfully")
                
                # Configure heartbeat
                current_interval = self.heartbeat_interval
                self.heartbeat_interval = SETTINGS['timing']['default_heartbeat']
                self.logger.info(
                    f"Heartbeat interval updated: {current_interval}ms -> "
                    f"{self.heartbeat_interval}ms"
                )
            else:
                raise RTDServerError("ServerStart failed with result: {result}")
                
        except Exception as e:
            self._state = RTDConnectionState.DISCONNECTED
            self.logger.error(f"Server initialization failed: {str(e)}")
            cleanup.cleanup_com() 
            raise

    @handle_com_error(RTDClientError)
    @log_method_call()
    @validate_connection_state([RTDConnectionState.CONNECTED])
    def subscribe(self, quote_type: Union[str, QuoteType], symbol: str) -> Optional[int]:
        """
        Subscribe to a specific quote type for a symbol.
        
        Args:
            quote_type: Type of quote to subscribe to
            symbol: Trading symbol
            
        Returns:
            int: Topic ID if subscription successful, None otherwise
            
        Raises:
            RTDClientError: If subscription fails
            RTDConnectionError: If called in invalid state
        """
        with self._topic_lock:

            quote_type_str = topic.validate_quote_type(quote_type)
            topic_id = topic.generate_topic_id(quote_type_str, symbol)
            
            if topic_id in self.topics:
                self.logger.info(
                    f"Already subscribed to {symbol} {quote_type_str}"
                )
                return topic_id
                
            # subscription params per current specs
            strings = (VARIANT * 2)()
            strings[0].value = quote_type_str
            strings[1].value = symbol
            get_new_values = VARIANT_BOOL(True)
            
            try:
                result = self.server.ConnectData(
                    topic_id, strings, get_new_values
                )
                self.logger.debug(f"Subscription raw result {result}")
                
                if isinstance(result, list) and len(result) >= 1 and result[0]:
                    self.topics[topic_id] = (symbol, quote_type_str)
                    self.logger.debug(
                        f"Subscribed to {symbol} {quote_type_str} "
                        f"with ID {topic_id}"
                    )
                    return topic_id
                else:
                    self.logger.warning(
                        f"Subscription failed for {symbol} {quote_type_str}"
                    )
                    return None
                    
            except Exception as e:
                self.logger.error(
                    f"Error subscribing to {symbol} {quote_type_str}: {e}"
                )
                raise RTDClientError(
                    f"Subscription failed for {symbol}"
                ) from e

    @handle_com_error(RTDClientError)
    @log_method_call()
    @validate_connection_state([RTDConnectionState.CONNECTED, RTDConnectionState.DISCONNECTING])
    def unsubscribe(self, quote_type: Union[str, QuoteType], symbol: str) -> bool:
        """
        Unsubscribe from a specific quote type for a symbol.
        
        Args:
            quote_type: Type of quote to unsubscribe from
            symbol: Trading symbol
            
        Returns:
            bool: True if unsubscription successful, False otherwise
            
        Raises:
            RTDClientError: If unsubscription fails
            RTDConnectionError: If called in invalid state
        """
        with self._topic_lock:
            quote_type_str = topic.validate_quote_type(quote_type)
            
            topic_id = topic.find_topic_id(self.topics, symbol, quote_type_str)
            if topic_id is None:
                self.logger.warning(
                    f"Not subscribed to {symbol} {quote_type_str}"
                )
                return False
                
            try:
                result = self.server.DisconnectData(topic_id)
                self.logger.debug(f"Unsub raw result {result}")
                
                if result == 0:  # Success
                    del self.topics[topic_id]
                    self.logger.debug(
                        f"Unsubscribed from {symbol} {quote_type_str}"
                    )
                    return True
                else:
                    self.logger.warning(
                        f"Unsubscription failed for {symbol} {quote_type_str}"
                    )
                    return False
                    
            except Exception as e:
                self.logger.error(
                    f"Error unsubscribing from {symbol} {quote_type_str}: {e}"
                )
                return False


    @handle_com_error(RTDUpdateError)
    @log_method_call()
    @validate_connection_state([RTDConnectionState.CONNECTED])
    def UpdateNotify(self) -> bool:
        """
        Native COM callback — called by the RTD server through the
        IRTDUpdateEvent vtable when new data is available.

        comtypes.COMObject builds a real C-level vtable, so the server
        calls this through the native interface, NOT through IDispatch.

        Increments notify counter, signals data_ready event, and
        calls refresh_topics() inline for immediate data processing.

        Returns:
            bool: True if refresh was successful
        """
        self._update_notify_count += 1
        self.data_ready.set()
        self.logger.debug(f"UpdateNotify fired (count: {self._update_notify_count})")
        return self.refresh_topics()

    @handle_com_error(RTDClientError)
    @log_method_call()
    @validate_connection_state([RTDConnectionState.CONNECTED])
    def refresh_topics(self) -> bool:
        """
        Refresh all subscribed topics and process updates.
        Called by UpdateNotify when server has new data.
        
        Returns:
            bool: True if refresh was successful
            
        Raises:
            RTDClientError: If refresh operation fails
        """
        try:
            result = self.server.RefreshData()
            self.logger.debug(f"RefreshData raw result {result}")
            self._last_refresh_time = time.time()
            
            if not result or not isinstance(result, list) or len(result) != 2:
                self.logger.warning(f"Unexpected result format from RefreshData: {result}")
                return False

            topic_count, data = result
            if topic_count == 0 or not data:
                self.logger.debug("No new data in this update")
                return True

            # Track last time we got actual data (for staleness detection)
            self._last_data_time = time.time()

            if not self._startup_logged:
                self._startup_logged = True
                self.logger.info("Data flowing — first live update received")

            self.logger.debug(f"Received refresh data for {topic_count} topics")
            
            if isinstance(data, tuple) and len(data) == 2:
                topic_ids, raw_values = data
                for id, raw_value in zip(topic_ids, raw_values):
                    if id in self.topics:
                        symbol, quote_type = self.topics[id]
                        quote_obj = Quote(quote_type, symbol, raw_value)
                        self._handle_quote_update(id, symbol, quote_type, quote_obj)
                return True
            else:
                self.logger.warning(f"Unexpected data format in RefreshData result: {data}")
                return False

        except Exception as e:
            self.logger.error(f"Error fetching or processing refresh data: {e}", exc_info=True)
            return False


    def _handle_quote_update(self, id: int, symbol: str, quote_type: str, quote: Quote) -> None:
        """
        Process a single quote update.
        
        Args:
            id: Topic ID
            symbol: Trading symbol
            quote_type: Type of quote
            quote: Quote object
        """
        try:
            if quote.value is None:
                self.logger.debug(f"Null value received for {symbol} {quote_type}")
                return

            # Update latest value
            with self._value_lock:
                key = (symbol, quote_type)
                old_value = None
                if key in self._latest_values:
                    old_value = self._latest_values[key].value
                self._latest_values[key] = quote
                value_changed = old_value != quote.value

            if value_changed:
                timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                self.logger.quote(f"[{timestamp}] LIVE {symbol} {quote_type}: {str(quote)}")
            
        except Exception as e:
            self.logger.error(f"Error handling quote update: {e}")

    @handle_com_error(RTDHeartbeatError)
    @log_method_call()
    @validate_connection_state([RTDConnectionState.CONNECTED, RTDConnectionState.DISCONNECTED])
    def check_heartbeat(self) -> bool:
        """
        Check server heartbeat status and data staleness.

        Returns True only if heartbeat is healthy AND data is not stale.
        A healthy heartbeat with stale data indicates a zombie COM connection
        (TOS restarted but the old COM link is dead).

        Returns:
            bool: True if heartbeat healthy and data flowing, False otherwise
        """
        if self._state == RTDConnectionState.DISCONNECTED:
            self.logger.debug("Heartbeat check skipped - disconnected state")
            return False

        try:
            result = self.server.Heartbeat()
            is_healthy = result == 1

            if not is_healthy:
                self.logger.warning(
                    f"Unhealthy heartbeat response: {result}"
                )
                return False

            # Heartbeat passed — now check for zombie (healthy heartbeat, no data)
            stale_sec = time.time() - self._last_data_time
            if stale_sec > self._data_stale_sec:
                self.logger.warning(
                    f"Zombie detected — heartbeat healthy but no data for "
                    f"{stale_sec:.0f}s (threshold {self._data_stale_sec:.0f}s)"
                )
                return False

            return True

        except Exception as e:
            self.logger.error(f"Heartbeat check failed: {e}")
            raise RTDHeartbeatError("Heartbeat operation failed") from e

    def reconnect(self) -> bool:
        """
        Reconnect to the RTD server, restoring all previous subscriptions.

        Performs:
        1. Snapshot current topic subscriptions
        2. Tear down existing COM connection
        3. Re-initialize COM and server
        4. Re-subscribe to all previously active topics

        Returns:
            bool: True if reconnect succeeded and topics restored
        """
        self.logger.info("Reconnect initiated — snapshotting topics")

        # 1. Snapshot
        with self._topic_lock:
            snapshot = list(self.topics.values())  # [(symbol, quote_type), ...]
        self.logger.info(f"Snapshot: {len(snapshot)} topics to restore")

        # 2. Tear down
        try:
            if self.server is not None:
                try:
                    self.server.ServerTerminate()
                except Exception:
                    pass
                self.server = None
            cleanup.cleanup_com()
        except Exception as e:
            self.logger.warning(f"Cleanup during reconnect: {e}")

        self._state = RTDConnectionState.DISCONNECTED
        with self._topic_lock:
            self.topics.clear()

        # Pause before reconnect
        delay = SETTINGS['timing'].get('reconnect_delay', 5.0)
        self.logger.info(f"Waiting {delay}s before reconnect attempt")
        time.sleep(delay)

        # 3. Re-initialize
        try:
            self._state = RTDConnectionState.CONNECTING
            pythoncom.CoInitialize()
            self.server = CreateObject(
                GUID(SETTINGS['rtd']['progid']),
                interface=IRtdServer
            )
            result = self.server.ServerStart(self)
            if result != 1:
                self.logger.error(f"ServerStart failed during reconnect: {result}")
                self._state = RTDConnectionState.DISCONNECTED
                return False

            self._state = RTDConnectionState.CONNECTED
            self.heartbeat_interval = SETTINGS['timing']['default_heartbeat']
            self.logger.info("Server re-started successfully")
        except Exception as e:
            self.logger.error(f"Reconnect initialization failed: {e}")
            self._state = RTDConnectionState.DISCONNECTED
            return False

        # 4. Restore subscriptions
        restored = 0
        for symbol, quote_type in snapshot:
            try:
                tid = self.subscribe(quote_type, symbol)
                if tid is not None:
                    restored += 1
            except Exception as e:
                self.logger.error(f"Failed to restore {symbol} {quote_type}: {e}")

        # Reset tracking state
        self._last_data_time = time.time()
        self._startup_logged = False
        self._update_notify_count = 0
        self.disconnected.clear()

        self.logger.info(f"Reconnect complete — restored {restored}/{len(snapshot)} topics")
        return restored > 0

    @property
    def heartbeat_interval(self) -> int:
        """
        Get current heartbeat interval in milliseconds.

        Returns:
            int: Current heartbeat interval
        """
        return self._heartbeat_interval

    @heartbeat_interval.setter
    def heartbeat_interval(self, interval: int) -> None:
        """
        Set heartbeat interval.
        
        Args:
            interval: New interval in milliseconds
            
        Raises:
            ValueError: If interval is not positive
        """
        if interval <= 0:
            raise ValueError("Heartbeat interval must be positive")
            
        self._heartbeat_interval = interval
        self.logger.info(f"Heartbeat interval set to {interval}ms")

    @handle_com_error(RTDServerError)
    @log_method_call()
    @validate_connection_state([RTDConnectionState.CONNECTED, RTDConnectionState.CONNECTING])
    def Disconnect(self, _client_initiated: bool = False) -> None:
        """
        Disconnect from the RTD server and cleanup resources.

        This method serves dual purpose:
        - **Server-initiated** (dispid 12 callback): TOS calls this when it exits.
          Sets the `disconnected` event so the main loop can trigger reconnect.
        - **Client-initiated**: Called by __exit__ or user code for orderly shutdown.
          Pass _client_initiated=True to skip signaling disconnected event.

        Performs orderly shutdown:
        1. Unsubscribes from all topics
        2. Terminates server connection
        3. Releases COM resources
        """
        with self._lock:
            if self._state == RTDConnectionState.DISCONNECTED:
                self.logger.info("Already disconnected")
                return

            if self._state == RTDConnectionState.DISCONNECTING:
                self.logger.info("Disconnect already in progress")
                return

            # Detect server-initiated disconnect (COM callback)
            if not _client_initiated:
                self.logger.warning(
                    "Disconnect invoked (server-initiated or COM callback) — "
                    "signaling disconnected event for reconnect"
                )
                self.disconnected.set()

            self._state = RTDConnectionState.DISCONNECTING
            self.logger.info("Starting disconnect sequence")

            try:
                # Unsubscribe but can be optional as Excel doesn't seem to do it or not
                # very effectively for large number of topics
                subscriptions = [(qt, sym) for sym, qt in self.topics.values()]
                if subscriptions:
                    unsubscribe_results = self.batch_unsubscribe(subscriptions)

                # Clear any remaining topics from memory
                cleanup.cleanup_topics(self.topics)

                if self.server is not None:
                    try:
                        self.server.ServerTerminate()
                        self.logger.info("Server terminated")
                    except Exception as e:
                        self.logger.error(f"Error terminating server: {e}")
                    finally:
                        self.server = None

                cleanup.cleanup_com()
                self._state = RTDConnectionState.DISCONNECTED
                self.logger.info("Disconnect completed")

            except Exception as e:
                self.logger.error(f"Error during disconnect: {e}")
                raise

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any]
    ) -> None:
        """
        Exit the runtime context and cleanup resources.
        
        Performs proper cleanup:
        - Unsubscribes from all topics
        - Terminates server connection
        - Releases COM resources
        
        Args:
            exc_type: Exception type if an error occurred
            exc_val: Exception value if an error occurred
            exc_tb: Exception traceback if an error occurred
        """
        try:
            if exc_type is not None:
                self.logger.error(f"Context exit due to error: {exc_val}")
            self.Disconnect(_client_initiated=True)
        except Exception as e:
            self.logger.error(f"Error during context exit: {e}")
            if exc_type is None:
                raise

################################################
################ Helpers #######################
################################################

    def batch_subscribe(
        self,
        subscriptions: List[Tuple[Union[str, QuoteType], str]]
    ) -> Dict[Tuple[str, str], bool]:
        """
        Subscribe to multiple quote types and symbols at once.
        
        Args:
            subscriptions: List of (quote_type, symbol) tuples
            
        Returns:
            dict: Mapping of (quote_type, symbol) to subscription success status
        """
        results = {}
        for quote_type, symbol in subscriptions:
            try:
                topic_id = self.subscribe(quote_type, symbol)
                results[(str(quote_type), symbol)] = topic_id is not None
            except Exception as e:
                self.logger.error(
                    f"Error in batch subscribe for {symbol} {quote_type}: {e}"
                )
                results[(str(quote_type), symbol)] = False
                
        successful = sum(1 for result in results.values() if result)
        self.logger.info(
            f"Batch subscribe completed: {successful}/{len(subscriptions)} "
            "successful"
        )
        return results

    def batch_unsubscribe(
        self,
        subscriptions: List[Tuple[Union[str, QuoteType], str]]
    ) -> Dict[Tuple[str, str], bool]:
        """
        Unsubscribe from multiple quote types and symbols at once.
        
        Args:
            subscriptions: List of (quote_type, symbol) tuples
            
        Returns:
            dict: Mapping of (quote_type, symbol) to unsubscription success status
        """
        results = {}
        for quote_type, symbol in subscriptions:
            try:
                success = self.unsubscribe(quote_type, symbol)
                results[(str(quote_type), symbol)] = success
            except Exception as e:
                self.logger.error(
                    f"Error in batch unsubscribe for {symbol} {quote_type}: {e}"
                )
                results[(str(quote_type), symbol)] = False
                
        successful = sum(1 for result in results.values() if result)
        self.logger.info(
            f"Batch unsubscribe completed: {successful}/{len(subscriptions)} "
            "successful"
        )
        return results


    @property
    def is_connected(self) -> bool:
        """Check if client is currently connected."""
        return self._state == RTDConnectionState.CONNECTED and self.server is not None

    def __str__(self) -> str:
        """
        Get string representation of client state.
        
        Returns:
            str: Client state information
        """
        status = "Connected" if self.is_connected else "Disconnected"
        topic_count = len(self.topics)
        return (
            f"RTDClient: {status}, "
            f"Topics: {topic_count}, "
            f"Updates: {self._update_notify_count}"
        )

    def __repr__(self) -> str:
        """
        Get detailed string representation of client.
        
        Returns:
            str: Detailed client information
        """
        return (
            f"RTDClient(state={self._state.name}, "
            f"topics={len(self.topics)}, "
            f"heartbeat={self._heartbeat_interval}ms, "
            f"updates={self._update_notify_count})"
        )
