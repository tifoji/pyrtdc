import time
from typing import Any, Dict, Optional

from src.core.error_handler import RTDConnectionState
from src.core.logger import get_logger

logger = get_logger(__name__)

def verify_server_state(server, state: RTDConnectionState) -> bool:
    """
    Verify server is in a valid state.
    
    Args:
        server: RTD server instance
        state: Current connection state
        
    Returns:
        bool: True if server state is valid
    """
    if not server:
        logger.error("Server instance not initialized")
        return False
        
    if state != RTDConnectionState.CONNECTED:
        logger.error(f"Invalid server state: {state}")
        return False
        
    return True

def get_server_health(
    state: RTDConnectionState,
    heartbeat_interval: int,
    last_refresh_time: Optional[float],
    topics_count: int,
    update_count: int
) -> Dict[str, Any]:
    """
    Get current server health status.
    
    Args:
        state: Current connection state
        heartbeat_interval: Current heartbeat interval in milliseconds
        last_refresh_time: Timestamp of last refresh
        topics_count: Number of active topics
        update_count: Number of updates received
        
    Returns:
        dict: Health status information
    """
    return {
        'connection_state': state.name,
        'heartbeat_interval': heartbeat_interval,
        'last_refresh_time': last_refresh_time,
        'topic_count': topics_count,
        'update_count': update_count
    }

def get_time_since_refresh(last_refresh_time: Optional[float]) -> float:
    """
    Get time elapsed since last refresh.
    
    Args:
        last_refresh_time: Timestamp of last refresh
        
    Returns:
        float: Seconds since last refresh, or -1 if never refreshed
    """
    if last_refresh_time is None:
        return -1
    return time.time() - last_refresh_time

def check_connection_status(state: RTDConnectionState, server) -> bool:
    """
    Check if client is currently connected and operational.
    
    Args:
        state: Current connection state
        server: RTD server instance
        
    Returns:
        bool: True if connected and operational
    """
    return state == RTDConnectionState.CONNECTED and server is not None

@property
def state(self) -> RTDConnectionState:
    """
    Get current connection state.
    
    Returns:
        RTDConnectionState: Current state
    """
    return self._state