import asyncio
import os
import sys
import requests
import logging
import csv
import io
import json
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
        # Only initialize the bot if the token is present
        if TELEGRAM_BOT_TOKEN:
            self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        else:
            self.bot = None
        self.headers = {
            'access-token': DHAN_ACCESS_TOKEN,
            'client-id': DHAN_CLIENT_ID,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.security_id_map = {}
        logger.info("🚀 F&O Trading Bot v2.0 initialized (DEBUG MODE)")

    async def load_security_ids(self):
        try:
            logger.info("Downloading Dhan instruments master file...")
            response = requests.get(DHAN_INSTRUMENTS_URL, timeout=30)
            response.raise_for_status()
            
            all_rows = list(csv.DictReader(io.StringIO(response.text)))
            today = datetime.now()

            # ===================== DEBUG CODE ADDED HERE =====================
            debug_printed = False
            for row in all_rows:
                if row.get('SEM_INSTRUMENT_TYPE') == 'FUTSTK' and not debug_printed:
                    logger.info("====================== DEBUG INFO: FIRST FUTSTK ROW FOUND ======================")
                    logger.info(row)
                    logger.info("==============================================================================")
                    debug_printed = True
                    break
            # ===================== END OF DEBUG CODE =====================

            for symbol in STOCKS_WATCHLIST:
                futures_contracts = []
                for row in all_rows:
                    try:
                        if (row.get('SEM_TRADING_SYMBOL') == symbol and 
                            row.get('SEM_INSTRUMENT_TYPE') == 'FUTSTK' and
                            row.get('SEM_EXM_EXCH_ID') == 'NFO'):
                            
                            expiry_date_str = row.get('SEM_EXPIRY_DATE')
                            if expiry_date_str:
                                expiry_date = datetime.strptime(expiry_date_str.split(' ')[0], '%Y-%m-%d')
                                if expiry_date > today:
                                    futures_contracts.append({
                                        'expiry': expiry_date,
                                        'fno_security_id': int(row.get('SEM_SMST_SECURITY_ID')),
                                        'equity_id': int(row.get('SEM_UNDERLYING_SECURITY_ID'))
                                    })
                    except (ValueError, TypeError):
                        continue
                
                if futures_contracts:
                    nearest_future = min(futures_contracts, key=lambda x: x['expiry'])
                    self.security_id_map[symbol] = nearest_future
                    logger.info(f"✅ {symbol}: Loaded F&O Security ID = {nearest_future['fno_security_id']}")
                else:
                    logger.warning(f"⚠️ {symbol}: No active future contracts found with current logic.")

            logger.info(f"Total {len(self.security_id_map)} F&O securities loaded.")
            return True

        except Exception as e:
            logger.error(f"Error loading security IDs: {e}", exc_info=True)
            return False

    async def send_telegram_message(self, text):
        if self.bot and TELEGRAM_CHAT_ID:
            try:
                await self.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode='Markdown')
            except Exception as e:
                logger.error(f"Failed to send Telegram message: {e}")

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
    if not all([DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, GEMINI_API_KEY]):
        logger.critical("❌ Missing critical DHAN or GEMINI environment variables. Exiting.")
        return

    server_thread = Thread(target=run_server, daemon=True)
    server_thread.start()
    bot_instance = FnOTradingBot()
    
    if await bot_instance.load_security_ids():
        await bot_instance.send_telegram_message("✅ **Bot is ONLINE (Debug Mode)**\nScan cycle will run based on loaded securities.")
        # The bot will now idle, as no securities are loaded. The key is the log output.
    else:
        await bot_instance.send_telegram_message("❌ **Bot failed to start.** Could not load F&O security IDs.")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        logger.critical(f"A critical error occurred in main: {e}", exc_info=True)
