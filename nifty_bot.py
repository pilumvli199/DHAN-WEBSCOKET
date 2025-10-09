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

# Third-party libraries
from telegram import Bot
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import mplfinance as mpf
import google.generativeai as genai

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
        self.headers = {
            'access-token': DHAN_ACCESS_TOKEN,
            'client-id': DHAN_CLIENT_ID,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.security_id_map = {}
        self.gemini_flash = genai.GenerativeModel('gemini-1.5-flash')
        self.gemini_pro_analyzer = genai.GenerativeModel('gemini-pro')
        logger.info("🚀 F&O Trading Bot v2.0 initialized (Final Version)")

    async def load_security_ids(self):
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
                        # ===================== THE CRITICAL FIX IS HERE =====================
                        # Futures contracts are in the 'NFO' exchange, not 'NSE'
                        if (row.get('SEM_TRADING_SYMBOL') == symbol and 
                            row.get('SEM_INSTRUMENT_TYPE') == 'FUTSTK' and
                            row.get('SEM_EXM_EXCH_ID') == 'NFO'): # Changed 'NSE' to 'NFO'
                            
                            expiry_date_str = row.get('SEM_EXPIRY_DATE')
                            if expiry_date_str:
                                # Safely parse date, ignoring time part if present
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
                    logger.warning(f"⚠️ {symbol}: No active future contracts found in NFO segment.")

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
                df = pd.DataFrame({'Date': pd.to_datetime(data['data']['start_Time'], unit='s'), 'Open': data['data']['open'], 'High': data['data']['high'], 'Low': data['data']['low'], 'Close': data['data']['close'], 'Volume': data['data']['volume']})
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

    def create_candlestick_chart(self, df, symbol, spot_price):
        try:
            if df is None or len(df) < 2: return None
            df_chart = df.copy(); df_chart.set_index('Date', inplace=True)
            mc = mpf.make_marketcolors(up='#26a69a', down='#ef5350', inherit=True)
            s = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='nightclouds')
            fig, _ = mpf.plot(df_chart.tail(150), type='candle', style=s, volume=True, title=f'\n{symbol} | Spot: ₹{spot_price:,.2f}', ylabel='Price (₹)', figsize=(15, 8), returnfig=True, tight_layout=True)
            buf = io.BytesIO(); fig.savefig(buf, format='png', dpi=100); buf.seek(0)
            logger.info(f"✅ Chart created for {symbol}")
            return buf
        except Exception as e:
            logger.error(f"❌ Error creating chart for {symbol}: {e}")
            return None

    def format_data_for_ai(self, symbol, oc_data, df):
        try:
            spot_price = oc_data.get('spotPrice', 0)
            atm_strike = min([d['strikePrice'] for d in oc_data['optionChainDetails']], key=lambda x: abs(x - spot_price))
            text = f"ANALYSIS FOR: {symbol.upper()}\n"
            text += f"CURRENT SPOT PRICE: {spot_price:,.2f}\n"
            text += "--- RECENT PRICE ACTION (Last 15 Candles) ---\n"
            text += "Time | Open | High | Low | Close | Volume\n"
            for _, row in df.tail(15).iterrows():
                text += f"{row.name.strftime('%H:%M')} | {row.Open:.2f} | {row.High:.2f} | {row.Low:.2f} | {row.Close:.2f} | {row.Volume:,}\n"
            return text
        except Exception as e:
            logger.error(f"❌ Error formatting data for AI: {e}")
            return "Error formatting data."

    async def run_ai_analysis(self, model, symbol, chart_buf, formatted_text, prompt):
        try:
            chart_buf.seek(0)
            image_bytes = chart_buf.read()
            logger.info(f"🧠 Running {model.model_name.split('/')[-1]} analysis for {symbol}...")
            response = await model.generate_content_async([prompt, formatted_text, {"mime_type": "image/png", "data": image_bytes}])
            return json.loads(response.text.strip().replace('```json', '').replace('```', ''))
        except Exception as e:
            logger.error(f"❌ AI analysis error ({model.model_name}) for {symbol}: {e}")
            return None

    async def send_trade_alert(self, trade_signal):
        try:
            symbol = trade_signal['symbol']; pro_result = trade_signal['pro']; chart_buf = trade_signal['chart']
            caption = (f"🚨 **Trade Alert: {symbol}** 🚨\n\n"
                       f"**Signal:** {pro_result.get('signal', 'N/A').upper()}\n"
                       f"**Entry Option:** {pro_result.get('entry_option', 'N/A')}\n"
                       f"**Entry Price:** ₹{pro_result.get('entry_price', 0):.2f}\n"
                       f"**Target:** ₹{pro_result.get('target_price', 0):.2f}\n"
                       f"**Stop Loss:** ₹{pro_result.get('stop_loss', 0):.2f}\n\n"
                       f"**Confidence:** {pro_result.get('confidence', 0)}%\n\n"
                       f"**Strategy:**\n_{pro_result.get('strategy', 'Not available.')}_")
            chart_buf.seek(0)
            await self.bot.send_photo(chat_id=TELEGRAM_CHAT_ID, photo=chart_buf, caption=caption, parse_mode='Markdown')
            logger.info(f"✅ Trade alert sent to Telegram for {symbol}")
        except Exception as e:
            logger.error(f"❌ Failed to send Telegram alert for {symbol}: {e}")

    async def process_stock(self, symbol):
        try:
            if symbol not in self.security_id_map: return
            info = self.security_id_map[symbol]
            oc_data = self.get_option_chain(info['fno_security_id'], symbol)
            if not oc_data: return
            df = self.get_historical_data(info['equity_id'], symbol)
            if df is None or len(df) < 50: return
            chart_buf = self.create_candlestick_chart(df, symbol, oc_data.get('spotPrice', 0))
            if not chart_buf: return
            formatted_text = self.format_data_for_ai(symbol, oc_data, df)
            flash_prompt = 'Analyze for a trade. Respond in JSON: {"tradeable": boolean, "signal": "bullish/bearish/neutral", "reason": "brief reason"}'
            flash_result = await self.run_ai_analysis(self.gemini_flash, symbol, chart_buf, formatted_text, flash_prompt)
            if not flash_result or not flash_result.get('tradeable'):
                logger.info(f"➡️ {symbol} not tradeable per Gemini Flash.")
                return
            pro_prompt = f'Flash found a "{flash_result.get("signal")}" signal. Create a precise F&O trade plan. Respond in JSON with: {{"signal": "...", "entry_option": "...", "entry_price": float, "target_price": float, "stop_loss": float, "confidence": integer, "strategy": "..."}}'
            pro_result = await self.run_ai_analysis(self.gemini_pro_analyzer, symbol, chart_buf, formatted_text, pro_prompt)
            if not pro_result or pro_result.get('confidence', 0) < 65:
                logger.info(f"➡️ {symbol} rejected by Gemini Pro (Confidence: {pro_result.get('confidence', 0)}%).")
                return
            trade_signal = {'symbol': symbol, 'pro': pro_result, 'chart': chart_buf}
            logger.info(f"🎯🎯🎯 {symbol} TRADE SIGNAL GENERATED! 🎯🎯🎯")
            await self.send_trade_alert(trade_signal)
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
        await bot_instance.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="✅ **Bot is ONLINE (Final Version)**\nStarting scan cycle...", parse_mode='Markdown')
        while True:
            logger.info("============== NEW SCAN CYCLE ==============")
            for stock_symbol in STOCKS_WATCHLIST:
                if stock_symbol in bot_instance.security_id_map:
                    await bot_instance.process_stock(stock_symbol)
                logger.info(f"--- Waiting 1.5s before next stock ---")
                await asyncio.sleep(1.5)
            logger.info(f"Scan cycle complete. Waiting for 10 minutes...")
            await asyncio.sleep(600)
    else:
        await bot_instance.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="❌ **Bot failed to start.** Could not load F&O security IDs.", parse_mode='Markdown')

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped manually.")
    except Exception as e:
        logger.critical(f"A critical error occurred in main: {e}", exc_info=True)
