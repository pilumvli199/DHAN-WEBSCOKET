import asyncio
import os
import sys
import requests
import logging
import csv
import io
from datetime import datetime, timedelta
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import mplfinance as mpf
import google.generativeai as genai
from telegram import Bot

# Force stdout flush for deployment logs
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# ========================
# LOGGING SETUP
# ========================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ========================
# CONFIGURATION
# ========================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

try:
    genai.configure(api_key=GEMINI_API_KEY)
except Exception as e:
    logger.critical(f"❌ Failed to configure AI clients: {e}")
    sys.exit(1)

DHAN_API_BASE = "https://api.dhan.co"
DHAN_INTRADAY_URL = f"{DHAN_API_BASE}/v2/charts/intraday"
DHAN_OPTION_CHAIN_URL = f"{DHAN_API_BASE}/v2/optionchain"
DHAN_INSTRUMENTS_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

STOCKS_WATCHLIST = [
    "RELIANCE", "HDFCBANK", "ICICIBANK", "BAJFINANCE", 
    "INFY", "TATAMOTORS", "AXISBANK", "SBIN"
]

# ========================
# F&O TRADING BOT v2.0
# ========================

class FnOTradingBot:
    def __init__(self):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.running = True
        self.headers = {
            'access-token': DHAN_ACCESS_TOKEN,
            'client-id': DHAN_CLIENT_ID,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.security_id_map = {}
        
        self.gemini_flash = genai.GenerativeModel('gemini-1.5-flash')
        self.gemini_pro_analyzer = genai.GenerativeModel('gemini-pro')
        
        logger.info("🚀 F&O Trading Bot v2.0 initialized (Final Fix)")

    # =================== THIS IS THE COMPLETELY REWRITTEN FUNCTION ===================
    async def load_security_ids(self):
        """
        Loads the correct F&O security IDs by finding the nearest expiry future contract for each stock.
        This is the definitive fix for the '400 Bad Request' error.
        """
        try:
            logger.info("Downloading Dhan instruments master file...")
            response = requests.get(DHAN_INSTRUMENTS_URL, timeout=30)
            response.raise_for_status()
            
            all_rows = list(csv.DictReader(io.StringIO(response.text)))
            today = datetime.now()

            for symbol in STOCKS_WATCHLIST:
                futures_contracts = []
                for row in all_rows:
                    try:
                        # Find all future contracts for the given stock symbol
                        if (row.get('SEM_TRADING_SYMBOL') == symbol and 
                            row.get('SEM_INSTRUMENT_TYPE') == 'FUTSTK' and
                            row.get('SEM_EXM_EXCH_ID') == 'NSE'):
                            
                            expiry_date_str = row.get('SEM_EXPIRY_DATE')
                            if expiry_date_str:
                                expiry_date = datetime.strptime(expiry_date_str, '%Y-%m-%d %H:%M:%S')
                                if expiry_date > today:
                                    futures_contracts.append({
                                        'expiry': expiry_date,
                                        'security_id': int(row.get('SEM_SMST_SECURITY_ID')),
                                        'equity_id': int(row.get('SEM_UNDERLYING_SECURITY_ID'))
                                    })
                    except (ValueError, TypeError):
                        continue
                
                if futures_contracts:
                    # Sort by expiry date to find the nearest one
                    nearest_future = min(futures_contracts, key=lambda x: x['expiry'])
                    self.security_id_map[symbol] = {
                        'fno_security_id': nearest_future['security_id'],
                        'equity_security_id': nearest_future['equity_id']
                    }
                    logger.info(f"✅ {symbol}: Loaded F&O Security ID = {nearest_future['security_id']}")
                else:
                    logger.warning(f"⚠️ {symbol}: No active future contracts found.")

            logger.info(f"Total {len(self.security_id_map)} F&O securities loaded.")
            return True

        except Exception as e:
            logger.error(f"Error loading security IDs: {e}", exc_info=True)
            return False

    def get_historical_data(self, equity_security_id, symbol):
        try:
            to_date, from_date = datetime.now(), datetime.now() - timedelta(days=7)
            payload = {
                "securityId": str(equity_security_id), "exchangeSegment": "NSE_EQ",
                "instrument": "EQUITY", "fromDate": from_date.strftime("%Y-%m-%d"),
                "toDate": to_date.strftime("%Y-%m-%d"), "interval": "FIVE_MINUTE"
            }
            logger.info(f"📊 Fetching chart data for {symbol}...")
            response = requests.post(DHAN_INTRADAY_URL, json=payload, headers=self.headers, timeout=15)
            response.raise_for_status()
            data = response.json()

            if 'data' in data and data.get('status') == 'success':
                df = pd.DataFrame({
                    'Date': pd.to_datetime(data['data']['start_Time'], unit='s'),
                    'Open': data['data']['open'], 'High': data['data']['high'],
                    'Low': data['data']['low'], 'Close': data['data']['close'],
                    'Volume': data['data']['volume']
                })
                logger.info(f"✅ {symbol}: Got {len(df)} candles")
                return df
            return None
        except Exception as e:
            logger.error(f"❌ Error getting historical data for {symbol}: {e}")
            return None

    def get_option_chain(self, fno_security_id, symbol):
        try:
            payload = {"securityId": str(fno_security_id), "exchangeSegment": "NSE_FNO"}
            logger.info(f"⛓️ Fetching option chain for {symbol}...")
            response = requests.post(DHAN_OPTION_CHAIN_URL, json=payload, headers=self.headers, timeout=15)
            response.raise_for_status()
            data = response.json()

            if data.get('status') == 'success' and 'data' in data:
                logger.info(f"✅ Option chain loaded for {symbol}")
                return data['data']
            return None
        except Exception as e:
            logger.error(f"❌ Error getting option chain for {symbol}: {e}")
            return None

    async def process_stock(self, symbol):
        try:
            if symbol not in self.security_id_map: return

            info = self.security_id_map[symbol]
            fno_security_id = info['fno_security_id']
            equity_security_id = info['equity_security_id']
            
            # --- Main Pipeline ---
            oc_data = self.get_option_chain(fno_security_id, symbol)
            if not oc_data: return

            df = self.get_historical_data(equity_security_id, symbol)
            if df is None or len(df) < 50:
                logger.warning(f"⚠️ {symbol}: Insufficient candle data.")
                return

            # (The rest of the pipeline for chart creation and AI analysis would go here)
            # For now, let's confirm the data fetching works
            spot_price = oc_data.get('spotPrice', 0)
            logger.info(f"✅✅✅ Successfully fetched all data for {symbol}! Spot Price: {spot_price}")

        except Exception as e:
            logger.error(f"❌ FATAL error processing {symbol}: {e}", exc_info=True)

# ========================
# HTTP SERVER & MAIN EXECUTION
# ========================
class KeepAliveHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.send_header('Content-type', 'text/plain'); self.end_headers()
        self.wfile.write(b"Bot is alive!")

def run_server():
    port = int(os.environ.get("PORT", 8080))
    server_address = ('', port)
    try:
        httpd = HTTPServer(server_address, KeepAliveHandler)
        logger.info(f"Starting keep-alive server on port {port}...")
        httpd.serve_forever()
    except Exception as e:
        logger.critical(f"Could not start HTTP server: {e}")

async def main():
    if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, GEMINI_API_KEY]):
        logger.critical("❌ Missing critical environment variables. Exiting.")
        return

    server_thread = Thread(target=run_server, daemon=True)
    server_thread.start()
    bot_instance = FnOTradingBot()
    
    if await bot_instance.load_security_ids():
        await bot_instance.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="✅ **Bot is ONLINE (Final Fix)**\nStarting scan cycle...", parse_mode='Markdown')

        while bot_instance.running:
            logger.info("============== NEW SCAN CYCLE ==============")
            for stock_symbol in STOCKS_WATCHLIST:
                await bot_instance.process_stock(stock_symbol)
                logger.info(f"--- Waiting 1.5 seconds before next stock ---")
                await asyncio.sleep(1.5) # Increased delay to be safer
            
            logger.info(f"Scan cycle complete. Waiting for 10 minutes...")
            await asyncio.sleep(600)
    else:
        await bot_instance.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="❌ **Bot failed to start.** Check logs.", parse_mode='Markdown')

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped manually.")
    except Exception as e:
        logger.critical(f"A critical error occurred in main: {e}", exc_info=True)
