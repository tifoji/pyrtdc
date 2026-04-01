# main.py
#
# Event-driven main loop using MsgWaitForMultipleObjects.
#
# The thread blocks at the OS kernel level until a COM message arrives
# (UpdateNotify callback from the RTD server), then pumps and processes.
# Wakes instantly on data, zero CPU while idle.

import pythoncom
import time
import win32event

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

# QS_ALLINPUT: wake on any Windows message type (keyboard, mouse, COM, timer, etc.)
QS_ALLINPUT = 0x04FF


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

    Uses MsgWaitForMultipleObjects for an efficient event-driven loop:
    - Thread sleeps at OS kernel level until a COM message arrives
    - Wakes instantly when the RTD server calls UpdateNotify via native vtable
    - PumpWaitingMessages delivers the callback -> UpdateNotify -> refresh_topics
    - Periodic housekeeping (heartbeat, summary) via timeout fallback
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

            # Timing state
            now = time.time()
            last_summary_time = now
            last_heartbeat_time = now

            # Timeout for MsgWaitForMultipleObjects (milliseconds).
            # This is the maximum time we'll sleep before waking for housekeeping.
            # COM callbacks wake us instantly regardless of this value.
            WAIT_TIMEOUT_MS = int(SETTINGS['timing']['loop_sleep_time'] * 1000)

            logger.info(
                f"Entering event loop (MsgWait timeout={WAIT_TIMEOUT_MS}ms, "
                f"heartbeat check={SETTINGS['timing']['heartbeat_check_interval']}s, "
                f"summary={SETTINGS['timing']['summary_interval']}s)"
            )

            # --- Main event loop ---
            while True:
                try:
                    # Block at OS kernel level until:
                    #   (a) A COM message arrives (UpdateNotify callback), OR
                    #   (b) Timeout expires (housekeeping interval)
                    # Zero CPU while idle, instant wake on data.
                    win32event.MsgWaitForMultipleObjects(
                        [],              # no extra handles (count inferred from list)
                        False,           # wake on ANY signal
                        WAIT_TIMEOUT_MS,
                        QS_ALLINPUT,
                    )

                    # Drain pending COM messages — delivers UpdateNotify callbacks
                    # which call refresh_topics() inline
                    pythoncom.PumpWaitingMessages()

                    # Clear the data_ready event (set by UpdateNotify)
                    client.data_ready.clear()

                    # --- Periodic housekeeping ---
                    current_time = time.time()

                    # Check heartbeat periodically
                    if current_time - last_heartbeat_time >= SETTINGS['timing']['heartbeat_check_interval']:
                        heartbeat_result = client.check_heartbeat()
                        logger.info(f"Heartbeat check: {'healthy' if heartbeat_result else 'FAILED'} "
                                    f"(UpdateNotify count: {client._update_notify_count})")
                        last_heartbeat_time = current_time

                    # Display summary periodically
                    if current_time - last_summary_time >= SETTINGS['timing']['summary_interval']:
                        display_summary(client)
                        last_summary_time = current_time

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
