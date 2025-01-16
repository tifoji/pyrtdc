import hashlib
from threading import Lock
from typing import Dict, List, Optional, Set, Tuple, Union

from src.core.logger import get_logger
from src.utils.quote import Quote, QuoteType


logger = get_logger(__name__)

def generate_topic_id(quote_type: str, symbol: str) -> int:
    """
    Generate a unique topic ID for a quote type and symbol combination.
    
    Args:
        quote_type: Type of quote
        symbol: Trading symbol
        
    Returns:
        int: Unique topic ID
    """
    value = f"{quote_type}:{symbol}"
    return int(hashlib.md5(value.encode()).hexdigest(), 16) % (2**16)

def find_topic_id(topics: Dict[int, Tuple[str, str]], 
                 symbol: str, quote_type: str) -> Optional[int]:
    """
    Find topic ID for a given symbol and quote type combination.
    
    Args:
        topics: Dictionary of topic IDs to (symbol, quote_type) pairs
        symbol: Trading symbol
        quote_type: Type of quote
        
    Returns:
        int: Topic ID if found, None otherwise
    """
    for id, (sym, qt) in topics.items():
        if sym == symbol and qt == quote_type:
            return id
    return None

def get_topic_stats(topics: Dict[int, Tuple[str, str]]) -> Dict[str, int]:
    """
    Get statistics about topic subscriptions.
    
    Args:
        topics: Dictionary of topic IDs to (symbol, quote_type) pairs
        
    Returns:
        dict: Statistics including total topics, unique symbols, and quote types
    """
    symbols: Set[str] = set()
    quote_types: Set[str] = set()
    
    for symbol, quote_type in topics.values():
        symbols.add(symbol)
        quote_types.add(quote_type)
        
    return {
        'total_topics': len(topics),
        'unique_symbols': len(symbols),
        'quote_types_count': len(quote_types)
    }

def get_subscriptions(topics: Dict[int, Tuple[str, str]]) -> List[Tuple[str, str]]:
    """
    Get list of all active subscriptions.
    
    Args:
        topics: Dictionary of topic IDs to (symbol, quote_type) pairs
        
    Returns:
        list: List of (symbol, quote_type) tuples
    """
    return [(symbol, quote_type) for symbol, quote_type in topics.values()]

def is_subscribed(topics: Dict[int, Tuple[str, str]], 
                 quote_type: Union[str, QuoteType], 
                 symbol: str) -> bool:
    """
    Check if a specific quote type and symbol is subscribed.
    
    Args:
        topics: Dictionary of topic IDs to (symbol, quote_type) pairs
        quote_type: Type of quote
        symbol: Trading symbol
        
    Returns:
        bool: True if subscribed, False otherwise
    """
    quote_type_str = validate_quote_type(quote_type)
    return find_topic_id(topics, symbol, quote_type_str) is not None

def validate_quote_type(quote_type: Union[str, QuoteType]) -> str:
    """
    Validate and normalize quote type.
    
    Args:
        quote_type: Quote type to validate
        
    Returns:
        str: Normalized quote type string
        
    Raises:
        ValueError: If quote type is invalid
    """
    if isinstance(quote_type, QuoteType):
        return quote_type.value
        
    try:
        return QuoteType[str(quote_type).upper()].value
    except KeyError:
        raise ValueError(f"Invalid quote type: {quote_type}")

def format_topic_info(topics: Dict[int, Tuple[str, str]], topic_id: int) -> str:
    """
    Format topic information for logging.
    
    Args:
        topics: Dictionary of topic IDs to (symbol, quote_type) pairs
        topic_id: Topic ID to format
        
    Returns:
        str: Formatted topic information
    """
    if topic_id in topics:
        symbol, quote_type = topics[topic_id]
        return f"Topic {topic_id}: {symbol} {quote_type}"
    return f"Topic {topic_id}: Unknown"

def get_all_latest(latest_values: Dict[Tuple[str, str], Quote], value_lock: Lock) -> List[Quote]:
    """
    Get all latest quote values.
    
    Args:
        latest_values: Dictionary of (symbol, quote_type) -> Quote
        value_lock: Lock for thread-safe access
        
    Returns:
        list: List of latest Quote objects
    """
    with value_lock:
        return list(latest_values.values())