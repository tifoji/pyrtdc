from typing import Dict, Tuple

import pythoncom

from src.core.logger import get_logger


logger = get_logger(__name__)

def cleanup_com() -> None:
    """
    Clean up COM resources.
    
    Ensures proper COM unintialization.
    """
    try:
        pythoncom.CoUninitialize()
        logger.debug("COM uninitialized")
    except Exception as e:
        logger.error(f"Error uninitializing COM: {e}")

def cleanup_topics(topics: Dict[int, Tuple[str, str]]) -> None:
    """
    Clean up topic tracking dictionary.
    
    Args:
        topics: Dictionary of topic IDs to (symbol, quote_type) pairs
    """
    try:
        count = len(topics)
        topics.clear()
        logger.info(f"Cleared {count} topics from tracking")
    except Exception as e:
        logger.error(f"Error clearing topics: {e}")