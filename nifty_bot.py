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
from openai import OpenAI

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
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") # This is optional as we removed GPT-4o

# AI Setup
try:
    genai.configure(api_key=GEMINI_API_KEY)
except Exception as e:
    logger.critical(f"❌ Failed to configure AI clients: {e}")
    sys.exit(1) # Exit if AI keys are not set

# Dhan API URLs
DHAN_API_BASE = "https://api.dhan.co"
DHAN_INTRADAY_URL = f"{DHAN_API_BASE}/v2/charts/intraday"
DHAN_OPTION_CHAIN_URL = f"{DHAN_API_BASE}/v2/optionchain" # Correct URL from logs
DHAN_INSTRUMENTS_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# Stock/Index Watchlist
STOCKS_INDICES = {
    "NIFTY": {"symbol": "Nifty 50", "segment": "IDX_I", "search_variants": ["NIFTY 50", "Nifty 50", "NIFTY"]},
    "BANKNIFTY": {"symbol": "Nifty Bank", "segment": "IDX_I", "search_variants": ["NIFTY BANK", "Nifty Bank", "BANKNIFTY"]},
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
        
        # Initialize AI Models
        self.gemini_flash = genai.GenerativeModel('gemini-1.5-flash')
        self.gemini_pro = genai.GenerativeModel('gemini-1.5-pro')
        
        logger.info("🚀 F&O Trading Bot v2.0 initialized")
    
    async def load_security_ids(self):
        """Loads security IDs from Dhan's master CSV file."""
        try:
            logger.info("Downloading Dhan instruments master file...")
            response = requests.get(DHAN_INSTRUMENTS_URL, timeout=30)
            response.raise_for_status()
            
            csv_text = response.text
            reader = csv.DictReader(io.StringIO(csv_text))
            
            all_rows = list(reader)
            
            for symbol, info in STOCKS_INDICES.items():
                segment_code = 'I' if info['segment'] == 'IDX_I' else 'E'
                search_variants = info['search_variants']
                
                found = False
                for row in all_rows:
                    try:
                        is_match = False
                        if segment_code == 'I' and row.get('SEM_SEGMENT') == 'I':
                            if row.get('SEM_TRADING_SYMBOL') in search_variants:
                                is_match = True
                        elif segment_code == 'E' and row.get('SEM_SEGMENT') == 'E':
                            if row.get('SEM_TRADING_SYMBOL') in search_variants and row.get('SEM_EXM_EXCH_ID') == 'NSE':
                                is_match = True
                        
                        if is_match:
                            sec_id = row.get('SEM_SMST_SECURITY_ID')
                            trading_symbol = row.get('SEM_TRADING_SYMBOL')
                            if sec_id:
                                self.security_id_map[symbol] = {
                                    'security_id': int(sec_id),
                                    'segment': info['segment'],
                                    'trading_symbol': trading_symbol
                                }
                                logger.info(f"✅ {symbol}: Security ID = {sec_id} (Found as: {trading_symbol})")
                                found = True
                                break
                    except (ValueError, TypeError):
                        continue
                
                if not found:
                    logger.warning(f"⚠️ {symbol}: Not found in CSV (tried variants: {search_variants})")

            logger.info(f"Total {len(self.security_id_map)} securities loaded.")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download instruments file: {e}")
            return False
        except Exception as e:
            logger.error(f"Error loading security IDs: {e}")
            return False

    def get_historical_data(self, security_id, segment, symbol):
        """Fetches historical intraday data for the last few days."""
        try:
            exch_seg = "IDX" if segment == "IDX_I" else "NSE_EQ"
            instrument_type = "INDEX" if segment == "IDX_I" else "EQUITY"
            
            to_date = datetime.now()
            from_date = to_date - timedelta(days=7)
            
            payload = {
                "securityId": str(security_id),
                "exchangeSegment": exch_seg,
                "instrument": instrument_type,
                "expiryDate": "0",
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
                    'Open': candle_data['open'],
                    'High': candle_data['high'],
                    'Low': candle_data['low'],
                    'Close': candle_data['close'],
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
        """Creates a candlestick chart image from a pandas DataFrame."""
        try:
            if df is None or len(df) < 2:
                logger.warning(f"{symbol}: Not enough data for chart.")
                return None

            df_chart = df.copy()
            df_chart.set_index('Date', inplace=True)
            
            mc = mpf.make_marketcolors(up='#26a69a', down='#ef5350', inherit=True)
            s = mpf.make_mpf_style(marketcolors=mc, base_mpf_style='nightclouds')

            fig, axes = mpf.plot(
                df_chart.tail(150),
                type='candle',
                style=s,
                volume=True,
                title=f'\n{symbol} | Spot: ₹{spot_price:,.2f}',
                ylabel='Price (₹)',
                figsize=(15, 8),
                returnfig=True,
                tight_layout=True
            )
            
            buf = io.BytesIO()
            fig.savefig(buf, format='png', dpi=100)
            buf.seek(0)
            
            logger.info(f"✅ Chart created for {symbol}")
            return buf
        except Exception as e:
            logger.error(f"❌ Error creating chart for {symbol}: {e}")
            return None

    # ============== THIS IS THE CORRECTED FUNCTION ==============
    def get_option_chain(self, security_id, segment, symbol):
        """Fetches the full option chain for a given security."""
        try:
            payload = {
                "securityId": str(security_id),
                "exchangeSegment": "NSE_FNO" # Option chain is in FNO segment
            }
            logger.info(f"⛓️ Fetching option chain for {symbol}...")
            # THE FIX: Changed from requests.get to requests.post and params to json
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
        """Pre-filters the stock based on OI, PCR, and IV."""
        try:
            if not oc_data:
                return False, "No option chain data"
            
            spot_price = oc_data.get('spotPrice', 0)
            total_ce_oi = oc_data.get('totalCE_OI', 0)
            total_pe_oi = oc_data.get('totalPE_OI', 0)

            if total_ce_oi == 0 or total_pe_oi == 0:
                return False, "OI data is zero"

            pcr = total_pe_oi / total_ce_oi
            if not (0.7 <= pcr <= 1.5):
                return False, f"PCR out of range: {pcr:.2f}"
            
            all_strikes = [d['strikePrice'] for d in oc_data.get('optionChainDetails', [])]
            if not all_strikes:
                return False, "No strikes found in option chain"
            
            atm_strike = min(all_strikes, key=lambda x: abs(x - spot_price))
            
            avg_iv = 0
            count = 0
            for details in oc_data.get('optionChainDetails', []):
                if abs(details['strikePrice'] - atm_strike) <= 200:
                    if details.get('ce_impledVolatility', 0) > 0:
                        avg_iv += details['ce_impledVolatility']
                        count += 1
            if count > 0:
                avg_iv /= count
            
            if avg_iv > 50:
                 return False, f"High IV: {avg_iv:.1f}%"
            
            logger.info(f"✅ {symbol} PASSED pre-filter: PCR={pcr:.2f}, IV={avg_iv:.1f}%")
            return True, {'pcr': pcr, 'iv': avg_iv, 'atm_strike': atm_strike}

        except Exception as e:
            logger.error(f"❌ Pre-filter error for {symbol}: {e}")
            return False, str(e)

    def format_data_for_ai(self, symbol, oc_data, df):
        """Formats option chain and candle data into a single text block for AI analysis."""
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
                if abs(detail['strikePrice'] - atm_strike) <= 300 * (1 if 'BANK' in symbol or 'NIFTY' in symbol else 2):
                    strike = detail['strikePrice']
                    ce_ltp = detail.get('ce_lastPrice', 0)
                    ce_oi = detail.get('ce_openInterest', 0) / 100000
                    pe_ltp = detail.get('pe_lastPrice', 0)
                    pe_oi = detail.get('pe_openInterest', 0) / 100000
                    text += f"{strike:<6.0f} | {ce_ltp:<6.2f} | {ce_oi:<15.2f} | {pe_ltp:<6.2f} | {pe_oi:<15.2f}\n"

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
        """Generic function to run analysis on a specific AI model."""
        try:
            chart_buf.seek(0)
            image_bytes = chart_buf.read()
            
            logger.info(f"🧠 Running {model.model_name.split('/')[-1]} analysis for {symbol}...")
            
            response = await model.generate_content_async([
                prompt,
                formatted_text,
                {"mime_type": "image/png", "data": image_bytes}
            ])
            
            result_text = response.text.strip().replace('```json', '').replace('```', '')
            result = json.loads(result_text)
            return result
        except Exception as e:
            logger.error(f"❌ AI analysis error ({model.model_name}) for {symbol}: {e}")
            return None

    async def send_trade_alert(self, trade_signal):
        """Formats and sends the final trade signal to Telegram."""
        try:
            symbol = trade_signal['symbol']
            pro_result = trade_signal['pro']
            chart_buf = trade_signal['chart']
            
            caption = f"🚨 **Trade Alert: {symbol}** 🚨\n\n"
            caption += f"**Signal:** {pro_result.get('signal', 'N/A').upper()}\n"
            caption += f"**Entry Option:** {pro_result.get('entry_option', 'N/A')}\n"
            caption += f"**Entry Price:** ₹{pro_result.get('entry_price', 0):.2f}\n"
            caption += f"**Target:** ₹{pro_result.get('target_price', 0):.2f}\n"
            caption += f"**Stop Loss:** ₹{pro_result.get('stop_loss', 0):.2f}\n"
            caption += f"**Risk:Reward:** {pro_result.get('risk_reward', 'N/A')}\n\n"
            caption += f"**Confidence:** {pro_result.get('confidence', 0)}%\n\n"
            caption += f"**Strategy:**\n_{pro_result.get('strategy', 'Not available.')}_"
            
            chart_buf.seek(0)
            await self.bot.send_photo(
                chat_id=TELEGRAM_CHAT_ID,
                photo=chart_buf,
                caption=caption,
                parse_mode='Markdown'
            )
            logger.info(f"✅ Trade alert sent to Telegram for {symbol}")
            
        except Exception as e:
            logger.error(f"❌ Failed to send Telegram alert for {symbol}: {e}")

    async def process_stock(self, symbol):
        """Runs the complete analysis pipeline for a single stock."""
        try:
            if symbol not in self.security_id_map:
                logger.warning(f"Skipping {symbol}, not found in security map.")
                return

            info = self.security_id_map[symbol]
            security_id = info['security_id']
            segment = info['segment']
            
            oc_data = self.get_option_chain(security_id, segment, symbol)
            if not oc_data:
                return
            
            passed, filter_reason = self.pre_filter_stock(symbol, oc_data)
            if not passed:
                logger.info(f"➡️ {symbol} filtered out: {filter_reason}")
                return
            
            spot_price = oc_data.get('spotPrice', 0)

            df = self.get_historical_data(security_id, segment, symbol)
            if df is None or len(df) < 50:
                logger.warning(f"⚠️ {symbol}: Insufficient candle data ({len(df) if df is not None else 0})")
                return

            chart_buf = self.create_candlestick_chart(df, symbol, spot_price)
            if not chart_buf: return

            formatted_text = self.format_data_for_ai(symbol, oc_data, df)
            
            flash_prompt = """
            Analyze the provided chart, option chain, and candle data for a quick trading opportunity.
            Respond in JSON with: {"tradeable": boolean, "signal": "bullish/bearish/neutral", "reason": "brief reason"}
            """
            flash_result = await self.run_ai_analysis(self.gemini_flash, symbol, chart_buf, formatted_text, flash_prompt)
            if not flash_result or not flash_result.get('tradeable'):
                logger.info(f"➡️ {symbol} not tradeable per Gemini Flash.")
                return

            pro_prompt = f"""
            Gemini Flash found a '{flash_result.get('signal')}' signal. Now, create a precise F&O trade plan.
            Analyze all data and respond in JSON with:
            {{
                "signal": "bullish/bearish",
                "entry_option": "e.g., 22500 CE or 22400 PE",
                "entry_price": float,
                "target_price": float,
                "stop_loss": float,
                "risk_reward": "e.g., 1:2.5",
                "confidence": integer (0-100),
                "strategy": "Detailed step-by-step strategy and reasoning."
            }}
            """
            pro_result = await self.run_ai_analysis(self.gemini_pro, symbol, chart_buf, formatted_text, pro_prompt)
            if not pro_result or pro_result.get('confidence', 0) < 65:
                logger.info(f"➡️ {symbol} rejected by Gemini Pro due to low confidence.")
                return
                
            trade_signal = {
                'symbol': symbol,
                'pro': pro_result,
                'chart': chart_buf
            }
            logger.info(f"🎯🎯🎯 {symbol} TRADE SIGNAL GENERATED! 🎯🎯🎯")
            await self.send_trade_alert(trade_signal)

        except Exception as e:
            logger.error(f"❌ FATAL error processing {symbol}: {e}", exc_info=True)

# ========================
# HTTP SERVER FOR DEPLOYMENT
# ========================
class KeepAliveHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"Bot is alive and running!")

def run_server():
    """Runs a simple HTTP server to keep the bot alive on deployment platforms."""
    port = int(os.environ.get("PORT", 8080))
    server_address = ('', port)
    try:
        httpd = HTTPServer(server_address, KeepAliveHandler)
        logger.info(f"Starting keep-alive server on port {port}...")
        httpd.serve_forever()
    except Exception as e:
        logger.critical(f"Could not start HTTP server: {e}")

# ========================
# MAIN EXECUTION
# ========================
async def main():
    """Main function to initialize and run the bot."""
    if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, GEMINI_API_KEY]):
        logger.critical("❌ Missing one or more critical environment variables. Exiting.")
        return

    server_thread = Thread(target=run_server, daemon=True)
    server_thread.start()

    bot_instance = FnOTradingBot()
    
    if await bot_instance.load_security_ids():
        await bot_instance.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text="✅ **F&O Trading Bot v2.0 is ONLINE**\nInitializing first scan cycle...",
            parse_mode='Markdown'
        )

        while bot_instance.running:
            logger.info("============== NEW SCAN CYCLE ==============")
            tasks = [bot_instance.process_stock(stock) for stock in STOCKS_INDICES.keys()]
            await asyncio.gather(*tasks)
            
            logger.info(f"Scan cycle complete. Waiting for 5 minutes...")
            await asyncio.sleep(300)
    else:
        await bot_instance.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text="❌ **Bot failed to start.**\nCould not load security IDs from Dhan. Check logs.",
            parse_mode='Markdown'
        )

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped manually.")
    except Exception as e:
        logger.critical(f"A critical error occurred in main: {e}", exc_info=True)
