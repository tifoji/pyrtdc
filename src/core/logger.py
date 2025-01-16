import logging
import os
from pathlib import Path
from threading import Lock
from typing import Optional

from colorama import Fore, Style, init
from concurrent_log_handler import ConcurrentRotatingFileHandler

from src.core.settings import SETTINGS


# colorama
init(autoreset=True)

# RTD_ROOT 
if not os.getenv('RTD_ROOT'):
    os.environ['RTD_ROOT'] = os.getcwd()

try:
    RTD_ROOT = os.getenv('RTD_ROOT')
    if not RTD_ROOT:
        RTD_ROOT = os.getcwd()
        
    BASE_DIR = Path(RTD_ROOT)
    LOGS_DIR = BASE_DIR / 'logs'
    
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    
except Exception as e:
    print(f"Error setting up log directory: {e}")
    LOGS_DIR = Path('logs')
    LOGS_DIR.mkdir(exist_ok=True)

#  From config
CONSOLE_LOG_LEVEL = SETTINGS['logging']['console_level']
FILE_LOG_LEVEL = SETTINGS['logging']['file_level']
MAX_BYTES = SETTINGS['logging']['max_bytes']
BACKUP_COUNT = SETTINGS['logging']['backup_count']

# Custom log level for quotes in console display
QUOTE = 15  #  DEBUG (10) < QUOTE (15) < INFO (20) < WARNING (30)
logging.addLevelName(QUOTE, "QUOTE")

def quote(self, message, *args, **kwargs):
    """Custom quote logging method."""
    if self.isEnabledFor(QUOTE):
        self._log(QUOTE, message, args, **kwargs)

logging.Logger.quote = quote

class ColoredQuoteFormatter(logging.Formatter):
    """Custom formatter for colorized quote messages in console output."""
    def format(self, record):
        if record.levelno == QUOTE:
            if record.msg.startswith('['):
                timestamp, rest = record.msg.split(']', 1)
                timestamp = timestamp + ']'
                parts = rest.strip().split(' ', 3)
                if len(parts) == 4:
                    status, symbol, quote_type, value = parts
                    formatted_msg = (
                        f"{Fore.CYAN}{timestamp} "
                        f"{Fore.WHITE}{status} "
                        f"{Fore.GREEN}{symbol} "
                        f"{Fore.RED}{quote_type} "
                        f"{Fore.GREEN}{value}"
                        f"{Style.RESET_ALL}"
                    )
                    record.msg = formatted_msg
                    return formatted_msg
        return super().format(record)

class PyRTDLogger:
    """Main logger class for pyrtdc."""
    def __init__(self):
        self.loggers = {}
        self.setup_logging()

    def get_log_level(self, level_name: str) -> int:
        """Convert string log level to logging constant."""
        return getattr(logging, level_name.upper(), logging.INFO)

    def setup_logging(self):
        """Initialize logging configuration."""
        # root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)  # Allow all levels
        
        root_logger.handlers.clear()
        
        # Console handler for QUOTE level only
        console_handler = logging.StreamHandler()
        console_handler.setLevel(QUOTE)  # Show QUOTE and above
        console_handler.addFilter(lambda record: record.levelno == QUOTE)  # But only QUOTE in console
        
        # Simple formatter for console
        console_formatter = ColoredQuoteFormatter('%(message)s')
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)

    def get_logger(self, name: Optional[str] = None) -> logging.Logger:
        """Get or create a logger instance."""
        if name in self.loggers:
            return self.loggers[name]
            
        logger = logging.getLogger(name)
        
        # Only add file handler if not already present
        if not any(isinstance(h, ConcurrentRotatingFileHandler) for h in logger.handlers):
            log_file = LOGS_DIR / f"{name or 'pyrtdc'}.log"
            
            try:
                file_handler = ConcurrentRotatingFileHandler(
                    filename=str(log_file),
                    maxBytes=MAX_BYTES,
                    backupCount=BACKUP_COUNT
                )
                
                file_formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s'
                )
                file_handler.setFormatter(file_formatter)
                file_handler.setLevel(self.get_log_level(FILE_LOG_LEVEL))
                logger.addHandler(file_handler)
                
            except Exception as e:
                print(f"Error setting up log file handler: {e}")

        self.loggers[name] = logger
        return logger

# Global logger instance
_logger_instance = PyRTDLogger()

def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Get a logger instance."""
    return _logger_instance.get_logger(name)