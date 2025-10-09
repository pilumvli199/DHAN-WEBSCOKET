import asyncio
import os
import sys
import requests
import logging
import csv
import io
import base64
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

# Force stdout flush for deployment logs (like Railway)
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
# Load environment variables
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# AI Setup
try:
    genai.configure(api_key=GEMINI_API_KEY)
except Exception as e:
    logger.critical(f"❌ Failed to configure AI clients: {e}")
    sys.exit(1)

# Dhan API URLs
DHAN_API_BASE = "https://api.dhan.co"
DHAN_INTRADAY_URL = f"{DHAN_API_BASE}/v2/charts/intraday"
DHAN_OPTION_CHAIN_URL = f"{DHAN_API_BASE}/v2/optionchain"
DHAN_INSTRUMENTS_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# =================== FIX 1: REMOVED INDICES, FOCUS ON STOCKS ===================
STOCKS_INDICES = {
    "RELIANCE": {"symbol": "RELIANCE", "segment": "NSE_EQ", "search_variants": ["RELIANCE"]},
    "HDFCBANK": {"symbol": "HDFCBANK", "segment": "NSE_EQ", "search_variants": ["HDFCBANK"]},
    "ICICIBANK": {"symbol": "ICICIBANK", "segment": "NSE_EQ", "search_variants": ["ICICIBANK"]},
    "BAJFINANCE": {"symbol": "BAJFINANCE", "segment": "NSE_EQ", "search_variants": ["BAJFINANCE"]},
    "INFY": {"symbol": "INFY", "segment": "NSE_EQ", "search_variants": ["INFY"]},
    "TATAMOTORS": {"symbol": "TATAMOTORS", "segment": "NSE_EQ", "search_variants": ["TATAMOTORS"]},
    "AXISBANK": {"symbol": "AXISBANK", "segment": "NSE_EQ", "search_variants": ["AXISBANK"]},
    "SBIN": {"symbol": "SBIN", "segment": "NSE_EQ", "search_variants": ["SBIN"]},
}

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
        self.gemini_pro = genai.GenerativeModel('gemini-1.5-pro')
        
        logger.info("🚀 F&O Trading Bot v2.0 initialized")
    
    async def load_security_ids(self):
        try:
            logger.info("Downloading Dhan instruments master file...")
            response = requests.get(DHAN_INSTRUMENTS_URL, timeout=30)
            response.raise_for_status()
            
            csv_text = response.text
            reader = csv.DictReader(io.StringIO(csv_text))
            
            all_rows = list(reader)
            
            for symbol, info in STOCKS_INDICES.items():
                found = False
                for row in all_rows:
                    try:
                        if (row.get('SEM_SEGMENT') == 'E' and
                            row.get('SEM_TRADING_SYMBOL') in info['search_variants'] and
                            row.get('SEM_EXM_EXCH_ID') == 'NSE'):
                            
                            sec_id = row.get('SEM_SMST_SECURITY_ID')
                            if sec_id:
                                self.security_id_map[symbol] = {
                                    'security_id': int(sec_id),
                                    'segment': info['segment']
                                }
                                logger.info(f"✅ {symbol}: Security ID = {sec_id}")
                                found = True
                                break
                    except (ValueError, TypeError):
                        continue
                
                if not found:
                    logger.warning(f"⚠️ {symbol}: Not found in CSV")

            logger.info(f"Total {len(self.security_id_map)} securities loaded.")
            return True

        except Exception as e:
            logger.error(f"Error loading security IDs: {e}", exc_info=True)
            return False

    def get_historical_data(self, security_id, symbol):
        try:
            to_date = datetime.now()
            from_date = to_date - timedelta(days=7)
            
            payload = {
                "securityId": str(security_id),
                "exchangeSegment": "NSE_EQ",
                "instrument": "EQUITY",
                "fromDate": from_date.strftime("%Y-%m-%d"),
                "toDate": to_date.strftime("%Y-%m-%d"),
                "interval": "FIVE_MINUTE"
            }
            
            logger.info(f"📊 Fetching chart data for {symbol}...")
            response = requests.post(DHAN_INTRADAY_URL, json=payload, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            if 'data' in data and data.get('status') == 'success':
                candle_data = data['data']
                df = pd.DataFrame({
                    'Date': pd.to_datetime(candle_data['start_Time'], unit='s'),
                    'Open': candle_data['open'], 'High': candle_data['high'],
                    'Low': candle_data['low'], 'Close': candle_data['close'],
                    'Volume': candle_data['volume']
                })
                logger.info(f"✅ {symbol}: Got {len(df)} candles")
                return df
            else:
                logger.warning(f"⚠️ {symbol}: No candle data in response. Response: {data}")
                return None
        except Exception as e:
            logger.error(f"❌ Error getting historical data for {symbol}: {e}")
            return None

    def create_candlestick_chart(self, df, symbol, spot_price):
        try:
            if df is None or len(df) < 2: return None
            df_chart = df.copy()
            df_chart.set_index('Date', inplace=True)
            
            mc = mpf.make_marketcolors(up='#26a69a', down='#ef5350', inherit=True)
            s = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='nightclouds')

            fig, _ = mpf.plot(
                df_chart.tail(150), type='candle', style=s, volume=True,
                title=f'\n{symbol} | Spot: ₹{spot_price:,.2f}', ylabel='Price (₹)',
                figsize=(15, 8), returnfig=True, tight_layout=True
            )
            
            buf = io.BytesIO()
            fig.savefig(buf, format='png', dpi=100)
            buf.seek(0)
            logger.info(f"✅ Chart created for {symbol}")
            return buf
        except Exception as e:
            logger.error(f"❌ Error creating chart for {symbol}: {e}")
            return None

    def get_option_chain(self, security_id, symbol):
        try:
            payload = {"securityId": str(security_id), "exchangeSegment": "NSE_FNO"}
            logger.info(f"⛓️ Fetching option chain for {symbol}...")
            response = requests.post(DHAN_OPTION_CHAIN_URL, json=payload, headers=self.headers, timeout=15)
            response.raise_for_status()
            data = response.json()

            if data.get('status') == 'success' and 'data' in data:
                logger.info(f"✅ Option chain loaded for {symbol}")
                return data['data']
            else:
                logger.error(f"❌ Option chain API error for {symbol}: {data.get('remarks')}")
                return None
        except Exception as e:
            logger.error(f"❌ Error getting option chain for {symbol}: {e}")
            return None

    def pre_filter_stock(self, symbol, oc_data):
        try:
            if not oc_data: return False, "No option chain data"
            
            spot_price = oc_data.get('spotPrice', 0)
            total_ce_oi = oc_data.get('totalCE_OI', 0)
            total_pe_oi = oc_data.get('totalPE_OI', 0)

            if total_ce_oi == 0 or total_pe_oi == 0: return False, "OI data is zero"

            pcr = total_pe_oi / total_ce_oi
            if not (0.7 <= pcr <= 1.5): return False, f"PCR out of range: {pcr:.2f}"
            
            logger.info(f"✅ {symbol} PASSED pre-filter: PCR={pcr:.2f}")
            return True, {'pcr': pcr}
        except Exception as e:
            logger.error(f"❌ Pre-filter error for {symbol}: {e}")
            return False, str(e)

    def format_data_for_ai(self, symbol, oc_data, df):
        try:
            spot_price = oc_data.get('spotPrice', 0)
            atm_strike = min([d['strikePrice'] for d in oc_data['optionChainDetails']], key=lambda x: abs(x - spot_price))

            text = f"ANALYSIS FOR: {symbol.upper()}\n"
            text += f"CURRENT SPOT PRICE: {spot_price:,.2f}\n"
            text += f"ATM STRIKE: {atm_strike:,.0f}\n\n"
            text += "--- OPTION CHAIN (Near ATM) ---\n"
            text += "STRIKE | CE LTP | CE OI (Lakhs) | PE LTP | PE OI (Lakhs)\n"
            text += "-" * 55 + "\n"

            for detail in sorted(oc_data['optionChainDetails'], key=lambda x: x['strikePrice']):
                if abs(detail['strikePrice'] - atm_strike) <= (atm_strike * 0.05): # 5% range around ATM
                    strike = detail['strikePrice']
                    text += (f"{strike:<6.0f} | "
                             f"{detail.get('ce_lastPrice', 0):<6.2f} | "
                             f"{detail.get('ce_openInterest', 0) / 100000:<15.2f} | "
                             f"{detail.get('pe_lastPrice', 0):<6.2f} | "
                             f"{detail.get('pe_openInterest', 0) / 100000:<15.2f}\n")

            text += "\n--- RECENT PRICE ACTION (Last 15 Candles) ---\n"
            text += "Time (IST)        | Open   | High   | Low    | Close  | Volume\n"
            text += "-" * 70 + "\n"
            for _, row in df.tail(15).iterrows():
                time_str = row.name.strftime('%H:%M')
                text += f"{time_str:<19} | {row.Open:<6.2f} | {row.High:<6.2f} | {row.Low:<6.2f} | {row.Close:<6.2f} | {row.Volume:,}\n"

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
            result_text = response.text.strip().replace('```json', '').replace('```', '')
            return json.loads(result_text)
        except Exception as e:
            logger.error(f"❌ AI analysis error ({model.model_name}) for {symbol}: {e}")
            return None

    async def send_trade_alert(self, trade_signal):
        try:
            symbol = trade_signal['symbol']
            pro_result = trade_signal['pro']
            chart_buf = trade_signal['chart']
            
            caption = (f"🚨 **Trade Alert: {symbol}** 🚨\n\n"
                       f"**Signal:** {pro_result.get('signal', 'N/A').upper()}\n"
                       f"**Entry Option:** {pro_result.get('entry_option', 'N/A')}\n"
                       f"**Entry Price:** ₹{pro_result.get('entry_price', 0):.2f}\n"
                       f"**Target:** ₹{pro_result.get('target_price', 0):.2f}\n"
                       f"**Stop Loss:** ₹{pro_result.get('stop_loss', 0):.2f}\n"
                       f"**Risk:Reward:** {pro_result.get('risk_reward', 'N/A')}\n\n"
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
            security_id = info['security_id']
            
            oc_data = self.get_option_chain(security_id, symbol)
            if not oc_data: return
            
            passed, filter_reason = self.pre_filter_stock(symbol, oc_data)
            if not passed:
                logger.info(f"➡️ {symbol} filtered out: {filter_reason}")
                return
            
            df = self.get_historical_data(security_id, symbol)
            if df is None or len(df) < 50:
                logger.warning(f"⚠️ {symbol}: Insufficient candle data.")
                return

            chart_buf = self.create_candlestick_chart(df, symbol, oc_data.get('spotPrice', 0))
            if not chart_buf: return

            formatted_text = self.format_data_for_ai(symbol, oc_data, df)
            
            flash_prompt = 'Analyze for a trade. Respond in JSON: {"tradeable": boolean, "signal": "bullish/bearish/neutral", "reason": "brief reason"}'
            flash_result = await self.run_ai_analysis(self.gemini_flash, symbol, chart_buf, formatted_text, flash_prompt)
            if not flash_result or not flash_result.get('tradeable'):
                logger.info(f"➡️ {symbol} not tradeable per Gemini Flash.")
                return

            pro_prompt = f'Flash found a "{flash_result.get("signal")}" signal. Create a precise F&O trade plan. Respond in JSON with: {{"signal": "...", "entry_option": "...", "entry_price": float, "target_price": float, "stop_loss": float, "risk_reward": "...", "confidence": integer, "strategy": "..."}}'
            pro_result = await self.run_ai_analysis(self.gemini_pro, symbol, chart_buf, formatted_text, pro_prompt)
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
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
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
        await bot_instance.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="✅ **Bot is ONLINE**\nStarting scan cycle...", parse_mode='Markdown')

        # =================== FIX 2: SEQUENTIAL LOOP WITH DELAY ===================
        while bot_instance.running:
            logger.info("============== NEW SCAN CYCLE ==============")
            for stock_symbol in STOCKS_INDICES.keys():
                await bot_instance.process_stock(stock_symbol)
                logger.info(f"--- Waiting 1 second before next stock ---")
                await asyncio.sleep(1) # Delay to respect API rate limits
            
            logger.info(f"Scan cycle complete. Waiting for 5 minutes...")
            await asyncio.sleep(300)
    else:
        await bot_instance.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="❌ **Bot failed to start.** Check logs.", parse_mode='Markdown')

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped manually.")
    except Exception as e:
        logger.critical(f"A critical error occurred in main: {e}", exc_info=True)
