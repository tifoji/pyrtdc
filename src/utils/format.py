from datetime import datetime
from typing import Any, Dict, Tuple

from src.core.error_handler import RTDConnectionState
from src.core.logger import get_logger


logger = get_logger(__name__)

def format_time_delta(seconds: float) -> str:
    """
    Format a time delta into a human-readable string.
    
    Args:
        seconds: Number of seconds
        
    Returns:
        str: Formatted string like 'DD:HH:MM:SS.mmm'
    """
    days = int(seconds // 86400)
    hours = int((seconds % 86400) // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    msecs = int((seconds * 1000) % 1000)
    
    return f"{days:02d}:{hours:02d}:{minutes:02d}:{secs:02d}.{msecs:03d}"

def format_client_info(state: RTDConnectionState, 
                      topic_count: int, 
                      update_count: int) -> str:
    """
    Format RTD client information for display.
    
    Args:
        state: Current client state
        topic_count: Number of active topics
        update_count: Number of updates received
        
    Returns:
        str: Formatted client information string
    """
    status = "Connected" if state == RTDConnectionState.CONNECTED else state.name
    return f"RTDClient: {status}, Topics: {topic_count}, Updates: {update_count}"

def format_client_details(state: RTDConnectionState, 
                         topic_count: int, 
                         heartbeat_ms: int,
                         update_count: int) -> str:
    """
    Format detailed RTD client information.
    
    Args:
        state: Current client state
        topic_count: Number of active topics
        heartbeat_ms: Heartbeat interval in milliseconds
        update_count: Number of updates received
        
    Returns:
        str: Formatted detailed information string
    """
    return (
        f"RTDClient(state={state.name}, "
        f"topics={topic_count}, "
        f"heartbeat={heartbeat_ms}ms, "
        f"updates={update_count})"
    )

def format_update_timestamp() -> str:
    """
    Format current timestamp for updates.
    
    Returns:
        str: Formatted timestamp string
    """
    return datetime.now().strftime("%H:%M:%S")

def format_topic_table_header(width: int = 80) -> str:
    """
    Format table header for topic display.
    
    Args:
        width: Total width of the table
        
    Returns:
        str: Formatted table header
    """
    headers = ["Symbol", "Type", "Value", "Last Update"]
    col_widths = [12, 15, 20, 20]
    
    header = "| " + " | ".join(h.center(w) for h, w in zip(headers, col_widths)) + " |"
    border = "+" + "-" * (len(header) - 2) + "+"
    
    return f"{border}\n{header}\n{border}"

