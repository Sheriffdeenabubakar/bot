# bitget_client.py
import os
import hmac
import hashlib
import base64
import json
import requests
import time
import sys
from urllib.parse import urlencode
import logging
from logging import StreamHandler, Formatter

# Configure bitget_client-specific logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = StreamHandler(sys.stdout)
    formatter = Formatter('[%(levelname)s] %(name)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

_BITGET_TIME_OFFSET_MS = 0
_BITGET_TIME_SYNCED_AT = 0.0
_BITGET_TIME_SYNC_TTL_S = 300.0

# Import API keys and URL from config
try:
    from config import BITGET_API_KEY, BITGET_SECRET_KEY, BITGET_PASSPHRASE, BITGET_API_URL, SIGNAL_CONFIG
except ImportError:
    logger.error("config.py not found or missing BITGET_API_KEY, BITGET_SECRET_KEY, BITGET_PASSPHRASE, BITGET_API_URL.")
    BITGET_API_KEY = os.getenv('BITGET_API_KEY')
    BITGET_SECRET_KEY = os.getenv('BITGET_SECRET_KEY')
    BITGET_PASSPHRASE = os.getenv('BITGET_PASSPHRASE')
    BITGET_API_URL = os.getenv('BITGET_API_URL', "https://api.bitget.com")
    SIGNAL_CONFIG = {}


def _timeout_s(key, default):
    try:
        raw = (SIGNAL_CONFIG or {}).get(key, default)
        if raw is None:
            return None
        value = float(raw)
        if value <= 0:
            return None
        return value
    except Exception:
        try:
            fallback = float(default)
            return None if fallback <= 0 else fallback
        except Exception:
            return None


def _retry_attempts(default=3):
    try:
        return max(1, int((SIGNAL_CONFIG or {}).get("api_request_retry_attempts", default) or default))
    except Exception:
        return int(default)


def _retry_backoff_seconds(default=1.0):
    try:
        return max(0.0, float((SIGNAL_CONFIG or {}).get("api_request_retry_backoff_seconds", default) or default))
    except Exception:
        return float(default)


def _sleep_retry_backoff(base_backoff_s, attempt_number):
    if base_backoff_s <= 0:
        return
    time.sleep(base_backoff_s * max(1.0, (2 ** max(0, int(attempt_number) - 1))))

class BitgetAPIClient:
    def __init__(self, api_key, api_secret, api_passphrase, use_testnet=False):
        if not api_key or not api_secret or not api_passphrase:
            logger.error("API_KEY, API_SECRET, or API_PASSPHRASE not provided for BitgetAPIClient.")
            raise ValueError("API credentials must be provided.")

        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.use_testnet = use_testnet

        if self.use_testnet:
            self.base_url = "https://api.bitget.com/api/testnet"
            logger.warning("BitgetAPIClient initialized for TESTNET environment. Ensure your API keys are for testnet.")
        else:
            self.base_url = "https://api.bitget.com"
            logger.info("BitgetAPIClient initialized for MAINNET environment.")

        logger.info(f"[BitgetAPIClient] Initialized with Base URL: {self.base_url} (Testnet: {self.use_testnet})")

    def _sync_time_offset(self, force=False):
        global _BITGET_TIME_OFFSET_MS, _BITGET_TIME_SYNCED_AT
        now = time.time()
        if not force and _BITGET_TIME_SYNCED_AT and (now - _BITGET_TIME_SYNCED_AT) < _BITGET_TIME_SYNC_TTL_S:
            return _BITGET_TIME_OFFSET_MS
        server_time = None
        timeout_s = _timeout_s("public_api_timeout_seconds", 5.0)
        retry_attempts = _retry_attempts()
        backoff_s = _retry_backoff_seconds()
        for attempt_number in range(1, retry_attempts + 1):
            try:
                response = requests.get(
                    f"{self.base_url}/api/v2/public/time",
                    timeout=timeout_s,
                )
                payload = response.json()
                if response.status_code == 200 and isinstance(payload, dict) and str(payload.get("code")) == "00000":
                    server_time = int(float(((payload.get("data") or {}).get("serverTime"))))
                    break
            except Exception as exc:
                logger.warning(
                    f"[BitgetAPIClient] Time sync failed "
                    f"(attempt {attempt_number}/{retry_attempts}): {exc}"
                )
            if attempt_number < retry_attempts:
                _sleep_retry_backoff(backoff_s, attempt_number)
        if server_time is not None:
            local_ms = int(time.time() * 1000)
            _BITGET_TIME_OFFSET_MS = int(server_time - local_ms)
            _BITGET_TIME_SYNCED_AT = now
        return _BITGET_TIME_OFFSET_MS

    def _get_timestamp(self, force_sync=False):
        offset = self._sync_time_offset(force=force_sync)
        return str(int(time.time() * 1000) + int(offset or 0))

    def _sign(self, timestamp, method, request_path, body_str):
        message = timestamp + method.upper() + request_path + body_str
        hmac_key = self.api_secret.encode('utf-8')
        signature = hmac.new(hmac_key, message.encode('utf-8'), hashlib.sha256).digest()
        return base64.b64encode(signature).decode('utf-8')

    def make_request(self, method, request_path, params=None, body=None):
        timeout_s = _timeout_s("signed_api_timeout_seconds", 10.0)
        retry_attempts = _retry_attempts()
        backoff_s = _retry_backoff_seconds()
        timestamp = self._get_timestamp()
        request_url = f"{self.base_url}{request_path}"
        full_path_for_signing = request_path

        if params:
            sorted_params = sorted(params.items())
            query_string = urlencode(sorted_params)
            request_url += f"?{query_string}"
            full_path_for_signing = f"{request_path}?{query_string}"

        headers = {
            'Content-Type': 'application/json',
            'ACCESS-KEY': self.api_key,
            'ACCESS-TIMESTAMP': timestamp,
            'ACCESS-PASSPHRASE': self.api_passphrase,
            'locale': 'en-US'
        }

        body_str = json.dumps(body) if body else ""
        headers['ACCESS-SIGN'] = self._sign(timestamp, method, full_path_for_signing, body_str)

        if method.upper() == 'POST':
            logger.error(
                f"Attempted POST request to {request_url} with body {body_str}. "
                "This bot is configured for signals only; trade execution is disabled."
            )
            return None
        if method.upper() != 'GET':
            logger.error(f"Unsupported HTTP method: {method}")
            return None

        last_payload = None
        for attempt_number in range(1, retry_attempts + 1):
            response = None
            try:
                response = requests.get(request_url, headers=headers, timeout=timeout_s)
                payload = response.json()
                if payload.get("code") == "40008":
                    timestamp = self._get_timestamp(force_sync=True)
                    headers['ACCESS-TIMESTAMP'] = timestamp
                    headers['ACCESS-SIGN'] = self._sign(timestamp, method, full_path_for_signing, body_str)
                    response = requests.get(request_url, headers=headers, timeout=timeout_s)
                    payload = response.json()

                if response.status_code == 200 and isinstance(payload, dict) and payload.get("code") == "00000":
                    return payload

                last_payload = payload if isinstance(payload, dict) else None
                if response.status_code >= 500 and attempt_number < retry_attempts:
                    logger.warning(
                        f"[BitgetAPIClient] Server error on {method.upper()} {request_path} "
                        f"(attempt {attempt_number}/{retry_attempts}); retrying."
                    )
                    _sleep_retry_backoff(backoff_s, attempt_number)
                    continue

                response.raise_for_status()
                return payload

            except requests.exceptions.Timeout as timeout_err:
                logger.warning(
                    f"[BitgetAPIClient] Timeout on {method.upper()} {request_path} "
                    f"(attempt {attempt_number}/{retry_attempts}): {timeout_err}"
                )
            except requests.exceptions.ConnectionError as conn_err:
                logger.warning(
                    f"[BitgetAPIClient] Connection error on {method.upper()} {request_path} "
                    f"(attempt {attempt_number}/{retry_attempts}): {conn_err}"
                )
            except requests.exceptions.HTTPError as http_err:
                response_text = response.text if isinstance(response, requests.Response) else ""
                logger.error(f"HTTP error occurred: {http_err} - Response: {response_text}")
                if response is None or response.status_code < 500 or attempt_number >= retry_attempts:
                    break
            except requests.exceptions.RequestException as req_err:
                logger.warning(
                    f"[BitgetAPIClient] Request error on {method.upper()} {request_path} "
                    f"(attempt {attempt_number}/{retry_attempts}): {req_err}"
                )
            except json.JSONDecodeError:
                response_text = response.text if isinstance(response, requests.Response) else ""
                logger.error(f"JSON decoding error: Could not parse response: {response_text}")
                break

            if attempt_number < retry_attempts:
                _sleep_retry_backoff(backoff_s, attempt_number)

        return last_payload

    def get_candlestick_data(self, symbol, interval, limit=100, start_time=None, end_time=None):
        """
        Fetches historical candlestick data for a given symbol and interval.
        Args:
            symbol (str): Trading pair symbol (e.g., BTCUSDT_UMCBL).
            interval (str): Candlestick interval (e.g., 1m, 5m, 1H, 4H, 1D).
            limit (int): Number of candlesticks to retrieve (max 200 for Bitget V2).
            start_time (int): Start timestamp in milliseconds (optional).
            end_time (int): End timestamp in milliseconds (optional).
        Returns:
            list: A list of candlestick data, each as a dictionary
                  {'timestamp', 'open', 'high', 'low', 'close', 'volume'}.
                  Returns an empty list if data fetching fails.
        """
        request_path = "/api/v2/mix/market/history-candles"
        requested = max(1, int(limit))
        page_size = min(requested, 200)
        max_pages = max(1, (requested + page_size - 1) // page_size + 2)

        start_ms = int(start_time) if start_time is not None else None
        end_ms = int(end_time) if end_time is not None else None

        candles_by_ts = {}
        current_end = end_ms
        last_oldest = None

        for _ in range(max_pages):
            params = {
                "symbol": symbol,
                "granularity": interval,
                "limit": page_size,
                "productType": "USDT-FUTURES",
            }
            if start_ms is not None:
                params["startTime"] = str(start_ms)
            if current_end is not None:
                params["endTime"] = str(current_end)

            response_data = self.make_request("GET", request_path, params=params)
            if not (response_data and response_data.get('code') == '00000' and response_data.get('data')):
                if not candles_by_ts:
                    logger.error(f"[BitgetAPIClient] Failed to fetch candlestick data for {symbol}, {interval} (V2). Response: {response_data}")
                    return []
                break

            batch = []
            for candle in response_data['data']:
                try:
                    row = {
                        'timestamp': int(candle[0]),
                        'open': float(candle[1]),
                        'high': float(candle[2]),
                        'low': float(candle[3]),
                        'close': float(candle[4]),
                        'volume': float(candle[5]),
                    }
                    batch.append(row)
                except (ValueError, IndexError) as e:
                    logger.error(f"[BitgetAPIClient] Error parsing candlestick data for {symbol}: {candle} - {e}")

            if not batch:
                break

            for row in batch:
                ts = row['timestamp']
                if start_ms is not None and ts < start_ms:
                    continue
                if end_ms is not None and ts > end_ms:
                    continue
                candles_by_ts[ts] = row

            oldest_ts = min(x['timestamp'] for x in batch)
            if len(candles_by_ts) >= requested:
                break
            if len(batch) < page_size:
                break
            if start_ms is not None and oldest_ts <= start_ms:
                break
            if last_oldest is not None and oldest_ts >= last_oldest:
                break

            last_oldest = oldest_ts
            current_end = oldest_ts - 1

        candles = sorted(candles_by_ts.values(), key=lambda x: x['timestamp'])
        if len(candles) > requested:
            candles = candles[-requested:]
        return candles

    def get_current_price(self, symbol):
        # logger.debug(f"[BitgetAPIClient] Fetching current price for {symbol} (V2 endpoint, USDT-FUTURES productType)...")
        request_path = "/api/v2/mix/market/tickers"
        params = {"symbol": symbol, "productType": "USDT-FUTURES"}

        response_data = self.make_request("GET", request_path, params=params)

        if response_data and response_data.get('code') == '00000' and response_data.get('data'):
            found_ticker = None
            for item in response_data['data']:
                if item.get('symbol') == symbol:
                    found_ticker = item
                    break
            if found_ticker:
                price = float(found_ticker.get('lastPr', 0))
                # logger.debug(f"[BitgetAPIClient] Current price for {symbol}: {price}")
                return price
            else:
                logger.warning(f"[BitgetAPIClient] Ticker for {symbol} not found in Bitget response data for USDT-FUTURES.")
                return None
        else:
            logger.error(f"[BitgetAPIClient] Failed to get current price for {symbol} (V2). Response: {response_data}")
            return None

    def get_futures_tickers(self):
        # 24h futures ticker data from Bitget (V2 endpoint, USDT-FUTURES productType)...")
        request_path = "/api/v2/mix/market/tickers"
        params = {"productType": "USDT-FUTURES"}

        response_data = self.make_request("GET", request_path, params=params)

        tickers = []
        if response_data and response_data.get('code') == '00000' and response_data.get('data'):
            for ticker in response_data['data']:
                symbol = ticker.get('symbol')
                quoteVolume = ticker.get('quoteVolume', 0) if ticker.get('quoteVolume') else ticker.get('usdtVolume', 0)
                change24h = float(ticker.get('priceChangePercent', 0)) * 100
                lastPr = float(ticker.get('lastPr', 0))
                if symbol:
                    tickers.append({
                        'symbol': symbol,
                        'quoteVolume': float(quoteVolume),
                        'change24h': change24h,
                        'lastPr': lastPr
                    })
            # logger.info(f"[BitgetAPIClient] Successfully fetched {len(tickers)} futures tickers (V2).")
        else:
            logger.error(f"[BitgetAPIClient] Failed to fetch futures tickers (V2). Response: {response_data}")
        return tickers

    def place_order(self, symbol: str, trade_type: str, price: float, quantity: float):
        # logger.info(f"Mock: Placing {trade_type} order for {quantity} of {symbol} at {price}")
        return {"orderId": f"mock_{int(time.time()*1000)}", "status": "success", "clientOid": f"my_bot_order_{int(time.time()*1000)}"}

# Global Instances for backward compatibility
# Prefer the repo's canonical BITGET_* variables so telemetry and signal execution
# share the same credentials. Legacy API_* names are kept only as a fallback.
_api_key = (
    BITGET_API_KEY
    or os.getenv('BITGET_API_KEY')
    or os.getenv('API_KEY')
)
_api_secret = (
    BITGET_SECRET_KEY
    or os.getenv('BITGET_SECRET_KEY')
    or os.getenv('API_SECRET')
)
_api_passphrase = (
    BITGET_PASSPHRASE
    or os.getenv('BITGET_PASSPHRASE')
    or os.getenv('API_PASSPHRASE')
)
_use_testnet = os.getenv('USE_TESTNET', 'False').strip().lower() == 'true'
api_client = BitgetAPIClient(_api_key, _api_secret, _api_passphrase, _use_testnet)
get_futures_tickers = api_client.get_futures_tickers
get_current_price = api_client.get_current_price
get_candlestick_data = api_client.get_candlestick_data
