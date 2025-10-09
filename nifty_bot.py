import asyncio
import os
import sys
import requests
import logging
import csv
import io
from datetime import datetime
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler
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

DHAN_INSTRUMENTS_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# ========================
# F&O TRADING BOT v2.0
# ========================

class FnOTradingBot:
    def __init__(self):
        if TELEGRAM_BOT_TOKEN:
            self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        else:
            self.bot = None
        logger.info("🚀 F&O Trading Bot v2.0 initialized (SUPER DEBUG MODE)")

    async def load_security_ids_debug(self):
        try:
            logger.info("Downloading Dhan instruments master file for debugging...")
            response = requests.get(DHAN_INSTRUMENTS_URL, timeout=30)
            response.raise_for_status()
            
            all_rows = list(csv.DictReader(io.StringIO(response.text)))

            # ===================== SUPER DEBUG CODE =====================
            # This code will find the first instrument in the NFO segment and print its details.
            # This is GUARANTEED to give us the information we need.
            debug_printed_futstk = False
            debug_printed_optstk = False

            for row in all_rows:
                # Find the first Stock Future
                if row.get('SEM_INSTRUMENT_TYPE') == 'FUTSTK' and not debug_printed_futstk:
                    logger.info("="*20 + " DEBUG: FIRST FUTSTK ROW " + "="*20)
                    logger.info(row)
                    logger.info("="*60)
                    debug_printed_futstk = True

                # Find the first Stock Option
                if row.get('SEM_INSTRUMENT_TYPE') == 'OPTSTK' and not debug_printed_optstk:
                    logger.info("="*20 + " DEBUG: FIRST OPTSTK ROW " + "="*20)
                    logger.info(row)
                    logger.info("="*60)
                    debug_printed_optstk = True

                if debug_printed_futstk and debug_printed_optstk:
                    break
            # ===================== END OF SUPER DEBUG CODE =====================

            logger.info("Debug information has been printed to the log. Please check the output.")
            return True

        except Exception as e:
            logger.error(f"Error during debug loading: {e}", exc_info=True)
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
    if not all([DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN]):
        logger.critical("❌ Missing critical DHAN environment variables. Exiting.")
        return

    server_thread = Thread(target=run_server, daemon=True)
    server_thread.start()
    bot_instance = FnOTradingBot()
    
    if await bot_instance.load_security_ids_debug():
        await bot_instance.send_telegram_message("✅ **Debug run complete.** Please check your application logs for the required information.")
    else:
        await bot_instance.send_telegram_message("❌ **Debug run failed.** Could not download or process the instruments file.")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        logger.critical(f"A critical error occurred in main: {e}", exc_info=True)
