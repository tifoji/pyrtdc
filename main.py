# main.py

import pythoncom
import time

from colorama import init, Fore, Style
from tabulate import tabulate

from config.quote_types import QuoteType
from src.core.error_handler import RTDError
from src.core.logger import get_logger
from src.core.settings import SETTINGS
from src.rtd.client import RTDClient
from src.utils.state import check_connection_status
from src.utils import topic


logger = get_logger(__name__)

#  colorama
init(autoreset=True)

def display_summary(client: RTDClient) -> None:
    """Display formatted summary of all active topics."""
    print(f"\n{Fore.CYAN}{Style.BRIGHT}Real Time Data Summary{Style.RESET_ALL}")
    
    headers = ["Symbol", "Type", "Value"]
    table_data = [
        [
            f"{Fore.GREEN}{quote.symbol}{Style.RESET_ALL}",
            f"{Fore.YELLOW}{quote.quote_type.name}{Style.RESET_ALL}",
            f"{Fore.WHITE}{str(quote)}{Style.RESET_ALL}"
        ]
        for quote in topic.get_all_latest(client._latest_values, client._value_lock)
    ]
    
    print(tabulate(table_data, headers=headers, tablefmt="fancy_grid", stralign="center"))
    print()

def main():
    """
    Main entry point for the RTD client application.
    """
    try:
        with RTDClient(heartbeat_ms=SETTINGS['timing']['initial_heartbeat']) as client:
            logger.info(f"RTD Client initialized with heartbeat: {client.heartbeat_interval}ms")

            # Initial subscriptions
            subscriptions = [
                ("SPY", [QuoteType.LAST, QuoteType.BID, QuoteType.ASK, QuoteType.VOLUME]),
                ("/ES:XCME", [QuoteType.LAST, QuoteType.BID, QuoteType.ASK])
            ]
            
            # Set up subscriptions
            for symbol, quote_types in subscriptions:
                for quote_type in quote_types:
                    try:
                        if client.subscribe(quote_type, symbol):
                            logger.info(f"Subscribed to {symbol} {quote_type.name}")
                        else:
                            logger.warning(f"Failed to subscribe to {symbol} {quote_type.name}")
                    except Exception as e:
                        logger.error(f"Error subscribing to {symbol} {quote_type.name}: {e}")

            logger.info(f"Initialized with {len(client.topics)} active subscriptions")

            # Main loop
            now = time.time()
            last_summary_time = now
            start_time = now
            last_heartbeat_time = start_time

            while True:
                try:
                    current_time = time.time()
                    
                    # This will return earlier if COM UpdateNotify messages are available
                    # But we still protect later with a loop sleep = Excel default refresh interval
                    pythoncom.PumpWaitingMessages(200)

                    # Check heartbeat periodically (every 30 seconds)
                    if current_time - last_heartbeat_time >= SETTINGS['timing']['heartbeat_check_interval']:
                        heartbeat_result = client.check_heartbeat()
                        logger.info(f"Heartbeat check result: {heartbeat_result}")
                        last_heartbeat_time = current_time

                    #A manual way to invoke the refreshes. Useful for some alternate implementations/async implementations 
                    #instead of pumping the COM UpdateNotify(s). UpdateNotify and Disconnect methods are synchronous RTD Server side
                    #implementations in the Single Threaded Apartment model of Thinkorswim's RTD. 

                    #if check_connection_status(client._state, client.server):
                    #    client.refresh_topics()

                    current_time = time.time()
                    if current_time - last_summary_time >= SETTINGS['timing']['summary_interval']:
                        display_summary(client)
                        last_summary_time = current_time

                    # Excel defaults to 2 seconds for RTD refreshes
                    time.sleep(SETTINGS['timing']['loop_sleep_time'])

                except KeyboardInterrupt:
                    logger.info("User interrupted execution")
                    break
                except Exception as e:
                    logger.error(f"Error in main loop: {e}")
                    time.sleep(1)  

    except KeyboardInterrupt:
        logger.info("Application terminated by user")
        return 0
    except RTDError as e:
        logger.error(f"RTD Error: {e}")
        return 1
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1
    finally:
        logger.info("Application shutdown complete")

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)