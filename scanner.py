import os
import logging
import sys
# Make sure to import the initialized api_client directly, not get_futures_tickers itself
from bitget_client import get_futures_tickers, api_client
from config import MIN_VOLUME_THRESHOLD # Import MIN_VOLUME_THRESHOLD from config
from logging import StreamHandler, Formatter
from dotenv import load_dotenv # Added this import for standalone execution



# Configure scanner-specific logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = StreamHandler(sys.stdout)
    formatter = Formatter('[%(levelname)s] %(name)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

logger.debug(f"[SCANNER_DEBUG] MIN_VOLUME_THRESHOLD is set to: {MIN_VOLUME_THRESHOLD}")

def find_eligible_coins_for_analysis():
    """
    Fetches futures ticker data from Bitget and filters coins based on volume.
    The primary goal is to find liquid USDT futures pairs for further detailed analysis.
    """
    logger.info("[Scanner] Fetching USDT futures tickers from Bitget for analysis...")
    # get_futures_tickers uses the global api_client initialized in bitget_client.py
    # It now correctly requests 'USDT-FUTURES' productType
    bitget_tickers = get_futures_tickers()

    if not bitget_tickers:
        logger.warning("[Scanner] No futures tickers received from Bitget API.")
        return []

    eligible_coins = []
    for ticker in bitget_tickers:
        symbol = ticker.get('symbol')
        quoteVolume = ticker.get('quoteVolume', 0)
        change24h = ticker.get('change24h', 0)
        lastPr = ticker.get('lastPr', 0)

        # Removed the '_UMCBL' / '_USDT' suffix check.
        # We trust that if productType="USDT-FUTURES" was requested, these are USDT futures.
        # Add a check for quoteVolume > 0 to filter out potentially inactive pairs.

        # In V2 API response for mix/market/tickers with productType=USDT-FUTURES,
        # the symbol itself might be like "BTCUSDT".
        # Let's assume any symbol returned when productType is USDT-FUTURES is valid.
        # The primary filter becomes minimum volume and actual USDT volume presence.

        if (symbol and
            quoteVolume > 0 and # Ensure there's actual trading volume
            quoteVolume >= MIN_VOLUME_THRESHOLD):

            eligible_coins.append({
                'symbol': symbol,
                'quoteVolume': quoteVolume,
                'change24h': change24h,
                'lastPr': lastPr
            })
            # logger.info(f"[Scanner] Found eligible coin for analysis: {symbol} (Volume: {quoteVolume:.2f})")
        # else:
            # logger.debug(f"[Scanner] Skipping {symbol} (Volume: {quoteVolume:.3f}) - Does not meet volume criteria or is inactive.")

    return eligible_coins

def scan_coins():
    """
    Main function to initiate the coin scanning process.
    This is the function main.py expects to call.
    """
    # logger.info("Scanning for liquid coins to analyze...")
    eligible_coins = find_eligible_coins_for_analysis()
    # logger.info(f"Finished scanning. Found {len(eligible_coins)} liquid coins for detailed analysis.")
    return eligible_coins

# This block runs only when scanner.py is executed directly (e.g., python scanner.py)
if __name__ == '__main__':
    print("Running scanner.py directly for testing...")
    # It's important to load_dotenv here too if you run scanner.py directly
    load_dotenv()
    # And to ensure MIN_VOLUME_THRESHOLD is loaded correctly if config.py relies on it
    # For a direct run, you might want to re-initiate the logger to see output clearly
    logger.setLevel(os.getenv("LOG_LEVEL", "DEBUG").upper()) # Set to DEBUG for testing
    print(f"MIN_VOLUME_THRESHOLD for direct test: {MIN_VOLUME_THRESHOLD}")

    found_coins = scan_coins()
    print(f"Test scan found {len(found_coins)} coins.")
    # for coin in found_coins:
        # print(f"   - {coin['symbol']}: Volume={coin['quoteVolume']:.2f}, 24h_Change={coin['change24h']:.2f}%, LastPrice={coin['lastPr']:.4f}")