import asyncio
import os
from telegram import Bot
import requests
from datetime import datetime
import logging
import csv
import io
import base64
import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import mplfinance as mpf
import pandas as pd
import google.generativeai as genai
from openai import OpenAI

# Logging setup
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
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# AI Setup
genai.configure(api_key=GEMINI_API_KEY)
openai_client = OpenAI(api_key=OPENAI_API_KEY)

# Dhan API URLs
DHAN_API_BASE = "https://api.dhan.co"
DHAN_OHLC_URL = f"{DHAN_API_BASE}/v2/marketfeed/ohlc"
DHAN_OPTION_CHAIN_URL = f"{DHAN_API_BASE}/v2/optionchain"
DHAN_EXPIRY_LIST_URL = f"{DHAN_API_BASE}/v2/optionchain/expirylist"
DHAN_INSTRUMENTS_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
DHAN_INTRADAY_URL = f"{DHAN_API_BASE}/v2/charts/intraday"

# Stock/Index List
STOCKS_INDICES = {
    "NIFTY 50": {"symbol": "NIFTY 50", "segment": "IDX_I"},
    "NIFTY BANK": {"symbol": "NIFTY BANK", "segment": "IDX_I"},
    "RELIANCE": {"symbol": "RELIANCE", "segment": "NSE_EQ"},
    "HDFCBANK": {"symbol": "HDFCBANK", "segment": "NSE_EQ"},
    "ICICIBANK": {"symbol": "ICICIBANK", "segment": "NSE_EQ"},
    "BAJFINANCE": {"symbol": "BAJFINANCE", "segment": "NSE_EQ"},
    "INFY": {"symbol": "INFY", "segment": "NSE_EQ"},
    "TATAMOTORS": {"symbol": "TATAMOTORS", "segment": "NSE_EQ"},
    "AXISBANK": {"symbol": "AXISBANK", "segment": "NSE_EQ"},
    "SBIN": {"symbol": "SBIN", "segment": "NSE_EQ"},
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
        
        # AI Models
        self.gemini_flash = genai.GenerativeModel('gemini-1.5-flash')
        self.gemini_pro = genai.GenerativeModel('gemini-1.5-pro')
        
        logger.info("🚀 F&O Trading Bot v2.0 initialized")
    
    async def load_security_ids(self):
        """Load security IDs from Dhan"""
        try:
            logger.info("Loading security IDs from Dhan...")
            response = requests.get(DHAN_INSTRUMENTS_URL, timeout=30)
            
            if response.status_code == 200:
                csv_text = response.text
                
                for symbol, info in STOCKS_INDICES.items():
                    segment = info['segment']
                    symbol_name = info['symbol']
                    
                    # Parse CSV for each symbol
                    csv_data = csv_text.split('\n')
                    reader = csv.DictReader(csv_data)
                    
                    for row in reader:
                        try:
                            if segment == "IDX_I":
                                if (row.get('SEM_SEGMENT') == 'I' and 
                                    row.get('SEM_TRADING_SYMBOL') == symbol_name):
                                    sec_id = row.get('SEM_SMST_SECURITY_ID')
                                    if sec_id:
                                        self.security_id_map[symbol] = {
                                            'security_id': int(sec_id),
                                            'segment': segment,
                                            'trading_symbol': symbol_name
                                        }
                                        logger.info(f"✅ {symbol}: Security ID = {sec_id}")
                                        break
                            else:
                                if (row.get('SEM_SEGMENT') == 'E' and 
                                    row.get('SEM_TRADING_SYMBOL') == symbol_name and
                                    row.get('SEM_EXM_EXCH_ID') == 'NSE'):
                                    sec_id = row.get('SEM_SMST_SECURITY_ID')
                                    if sec_id:
                                        self.security_id_map[symbol] = {
                                            'security_id': int(sec_id),
                                            'segment': segment,
                                            'trading_symbol': symbol_name
                                        }
                                        logger.info(f"✅ {symbol}: Security ID = {sec_id}")
                                        break
                        except Exception as e:
                            continue
                
                logger.info(f"Total {len(self.security_id_map)} securities loaded")
                return True
            else:
                logger.error(f"Failed to load instruments: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error loading security IDs: {e}")
            return False
    
    def get_historical_data(self, security_id, segment, symbol):
        """Get last 5 days of 5-min candles"""
        try:
            from datetime import datetime, timedelta
            
            if segment == "IDX_I":
                exch_seg = "IDX_I"
                instrument = "INDEX"
            else:
                exch_seg = "NSE_EQ"
                instrument = "EQUITY"
            
            to_date = datetime.now()
            from_date = to_date - timedelta(days=7)
            
            payload = {
                "securityId": str(security_id),
                "exchangeSegment": exch_seg,
                "instrument": instrument,
                "interval": "5",
                "fromDate": from_date.strftime("%Y-%m-%d"),
                "toDate": to_date.strftime("%Y-%m-%d")
            }
            
            logger.info(f"📊 Fetching chart data for {symbol}...")
            
            response = requests.post(
                DHAN_INTRADAY_URL,
                json=payload,
                headers=self.headers,
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if 'open' in data and 'high' in data and 'low' in data and 'close' in data:
                    opens = data.get('open', [])
                    highs = data.get('high', [])
                    lows = data.get('low', [])
                    closes = data.get('close', [])
                    volumes = data.get('volume', [])
                    timestamps = data.get('start_Time', [])
                    
                    candles = []
                    for i in range(len(opens)):
                        candles.append({
                            'timestamp': timestamps[i] if i < len(timestamps) else '',
                            'open': opens[i] if i < len(opens) else 0,
                            'high': highs[i] if i < len(highs) else 0,
                            'low': lows[i] if i < len(lows) else 0,
                            'close': closes[i] if i < len(closes) else 0,
                            'volume': volumes[i] if i < len(volumes) else 0
                        })
                    
                    logger.info(f"✅ {symbol}: Got {len(candles)} candles")
                    return candles
                else:
                    logger.warning(f"⚠️ {symbol}: Invalid data structure in response")
            else:
                logger.error(f"❌ {symbol}: API error {response.status_code}")
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Error getting historical data for {symbol}: {e}")
            return None
    
    def create_candlestick_chart(self, candles, symbol, spot_price):
        """Create candlestick chart"""
        try:
            df_data = []
            for candle in candles:
                timestamp = candle.get('timestamp', candle.get('start_Time', ''))
                df_data.append({
                    'Date': pd.to_datetime(timestamp) if timestamp else pd.Timestamp.now(),
                    'Open': float(candle.get('open', 0)),
                    'High': float(candle.get('high', 0)),
                    'Low': float(candle.get('low', 0)),
                    'Close': float(candle.get('close', 0)),
                    'Volume': int(float(candle.get('volume', 0)))
                })
            
            df = pd.DataFrame(df_data)
            df.set_index('Date', inplace=True)
            
            if len(df) < 2:
                logger.warning(f"{symbol}: Not enough candles for chart")
                return None
            
            mc = mpf.make_marketcolors(
                up='#26a69a',
                down='#ef5350',
                edge='inherit',
                wick='inherit',
                volume='in'
            )
            
            s = mpf.make_mpf_style(
                marketcolors=mc,
                gridstyle='-',
                gridcolor='#333333',
                facecolor='#1e1e1e',
                figcolor='#1e1e1e',
                gridaxis='both',
                y_on_right=False
            )
            
            fig, axes = mpf.plot(
                df,
                type='candle',
                style=s,
                volume=True,
                title=f'\n{symbol} - {len(candles)} Candles | Spot: ₹{spot_price:,.2f}',
                ylabel='Price (₹)',
                ylabel_lower='Volume',
                figsize=(12, 8),
                returnfig=True,
                tight_layout=True
            )
            
            axes[0].set_title(
                f'{symbol} - {len(candles)} Candles | Spot: ₹{spot_price:,.2f}',
                color='white',
                fontsize=14,
                fontweight='bold',
                pad=20
            )
            
            for ax in axes:
                ax.tick_params(colors='white', which='both')
                ax.spines['bottom'].set_color('white')
                ax.spines['top'].set_color('white')
                ax.spines['left'].set_color('white')
                ax.spines['right'].set_color('white')
                ax.xaxis.label.set_color('white')
                ax.yaxis.label.set_color('white')
            
            buf = io.BytesIO()
            fig.savefig(buf, format='png', dpi=100, bbox_inches='tight', facecolor='#1e1e1e')
            buf.seek(0)
            plt.close(fig)
            
            logger.info(f"✅ Chart created for {symbol}")
            return buf
            
        except Exception as e:
            logger.error(f"❌ Error creating chart for {symbol}: {e}")
            return None
    
    def get_nearest_expiry(self, security_id, segment):
        """Get nearest expiry"""
        try:
            payload = {
                "UnderlyingScrip": security_id,
                "UnderlyingSeg": segment
            }
            
            logger.info(f"📅 Fetching expiry dates...")
            
            response = requests.post(
                DHAN_EXPIRY_LIST_URL,
                json=payload,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success' and data.get('data'):
                    expiries = data['data']
                    if expiries:
                        logger.info(f"✅ Nearest expiry: {expiries[0]}")
                        return expiries[0]
            else:
                logger.error(f"❌ Expiry API error: {response.status_code}")
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Error getting expiry: {e}")
            return None
    
    def get_option_chain(self, security_id, segment, expiry):
        """Get option chain data"""
        try:
            payload = {
                "UnderlyingScrip": security_id,
                "UnderlyingSeg": segment,
                "Expiry": expiry
            }
            
            logger.info(f"⛓️ Fetching option chain...")
            
            response = requests.post(
                DHAN_OPTION_CHAIN_URL,
                json=payload,
                headers=self.headers,
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('data'):
                    logger.info(f"✅ Option chain loaded")
                    return data['data']
            else:
                logger.error(f"❌ Option chain API error: {response.status_code}")
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Error getting option chain: {e}")
            return None
    
    def pre_filter_stock(self, symbol, oc_data):
        """Pre-filter based on OI, PCR, IV"""
        try:
            spot_price = oc_data.get('last_price', 0)
            oc = oc_data.get('oc', {})
            
            if not oc:
                return False, "No option chain data"
            
            # Find ATM
            strikes = sorted([float(s) for s in oc.keys()])
            atm_strike = min(strikes, key=lambda x: abs(x - spot_price))
            
            # Get ATM data
            atm_data = oc.get(f"{atm_strike:.6f}", {})
            ce_data = atm_data.get('ce', {})
            pe_data = atm_data.get('pe', {})
            
            # OI Check (minimum 10,000)
            ce_oi = ce_data.get('oi', 0)
            pe_oi = pe_data.get('oi', 0)
            total_oi = ce_oi + pe_oi
            
            if total_oi < 10000:
                return False, f"Low OI: {total_oi}"
            
            # PCR Check (0.6 to 1.5 - tradeable zone)
            pcr = pe_oi / ce_oi if ce_oi > 0 else 0
            if not (0.6 <= pcr <= 1.5):
                return False, f"PCR out of range: {pcr:.2f}"
            
            # IV Check (< 50% - not too expensive)
            ce_iv = ce_data.get('implied_volatility', 0)
            pe_iv = pe_data.get('implied_volatility', 0)
            avg_iv = (ce_iv + pe_iv) / 2
            
            if avg_iv > 50:
                return False, f"High IV: {avg_iv:.1f}%"
            
            logger.info(f"✅ {symbol} PASSED pre-filter: OI={total_oi}, PCR={pcr:.2f}, IV={avg_iv:.1f}%")
            return True, {
                'oi': total_oi,
                'pcr': pcr,
                'iv': avg_iv,
                'atm_strike': atm_strike
            }
            
        except Exception as e:
            logger.error(f"❌ Pre-filter error for {symbol}: {e}")
            return False, str(e)
    
    def format_option_data_for_ai(self, symbol, oc_data, spot_price):
        """Format option chain data as text for AI"""
        try:
            oc = oc_data.get('oc', {})
            strikes = sorted([float(s) for s in oc.keys()])
            atm_strike = min(strikes, key=lambda x: abs(x - spot_price))
            
            atm_idx = strikes.index(atm_strike)
            start_idx = max(0, atm_idx - 5)
            end_idx = min(len(strikes), atm_idx + 6)
            selected_strikes = strikes[start_idx:end_idx]
            
            text = f"OPTION CHAIN DATA - {symbol}\n"
            text += f"Spot Price: ₹{spot_price:,.2f}\n"
            text += f"ATM Strike: ₹{atm_strike:,.0f}\n\n"
            text += "Strike | CE_LTP | CE_OI | CE_Vol | CE_IV | PE_LTP | PE_OI | PE_Vol | PE_IV\n"
            text += "-" * 80 + "\n"
            
            for strike in selected_strikes:
                strike_key = f"{strike:.6f}"
                strike_data = oc.get(strike_key, {})
                
                ce = strike_data.get('ce', {})
                pe = strike_data.get('pe', {})
                
                text += f"{strike:.0f} | "
                text += f"₹{ce.get('last_price', 0):.1f} | "
                text += f"{ce.get('oi', 0)/1000:.0f}K | "
                text += f"{ce.get('volume', 0)/1000:.0f}K | "
                text += f"{ce.get('implied_volatility', 0):.1f}% | "
                text += f"₹{pe.get('last_price', 0):.1f} | "
                text += f"{pe.get('oi', 0)/1000:.0f}K | "
                text += f"{pe.get('volume', 0)/1000:.0f}K | "
                text += f"{pe.get('implied_volatility', 0):.1f}%\n"
            
            # Add Greeks
            atm_data = oc.get(f"{atm_strike:.6f}", {})
            ce_greeks = atm_data.get('ce', {}).get('greeks', {})
            pe_greeks = atm_data.get('pe', {}).get('greeks', {})
            
            text += f"\nATM GREEKS:\n"
            text += f"CE: Delta={ce_greeks.get('delta', 0):.3f}, Theta={ce_greeks.get('theta', 0):.2f}, Gamma={ce_greeks.get('gamma', 0):.4f}\n"
            text += f"PE: Delta={pe_greeks.get('delta', 0):.3f}, Theta={pe_greeks.get('theta', 0):.2f}, Gamma={pe_greeks.get('gamma', 0):.4f}\n"
            
            return text
            
        except Exception as e:
            logger.error(f"❌ Error formatting option data: {e}")
            return ""
    
    def format_candle_data_for_ai(self, candles):
        """Format candle data as text"""
        try:
            text = "\nRAW CANDLE DATA (Last 20 candles):\n"
            text += "Time | Open | High | Low | Close | Volume\n"
            text += "-" * 60 + "\n"
            
            last_20 = candles[-20:] if len(candles) > 20 else candles
            
            for candle in last_20:
                ts = candle.get('timestamp', '')
                text += f"{ts} | "
                text += f"₹{candle.get('open', 0):.2f} | "
                text += f"₹{candle.get('high', 0):.2f} | "
                text += f"₹{candle.get('low', 0):.2f} | "
                text += f"₹{candle.get('close', 0):.2f} | "
                text += f"{candle.get('volume', 0)}\n"
            
            return text
            
        except Exception as e:
            logger.error(f"❌ Error formatting candle data: {e}")
            return ""
    
    async def gemini_flash_scan(self, symbol, chart_buf, option_text, candle_text):
        """Gemini Flash - Quick scan for patterns"""
        try:
            logger.info(f"🔍 Gemini Flash scanning {symbol}...")
            
            # Upload image
            chart_buf.seek(0)
            image_bytes = chart_buf.read()
            
            prompt = f"""You are a F&O trading expert. Analyze this chart and option chain data.

{option_text}

{candle_text}

TASK: Identify if this stock has a tradeable setup.
Look for:
1. Chart patterns (breakout, support/resistance, trend)
2. Volume confirmation
3. Option chain signals (OI buildup, PCR ratio)

Respond in JSON format:
{{
    "tradeable": true/false,
    "pattern": "pattern name",
    "signal": "bullish/bearish/neutral",
    "confidence": 0-100,
    "reason": "brief reason"
}}
"""
            
            response = self.gemini_flash.generate_content([
                prompt,
                {"mime_type": "image/png", "data": image_bytes}
            ])
            
            result_text = response.text.replace('```json', '').replace('```', '').strip()
            result = json.loads(result_text)
            logger.info(f"✅ Gemini Flash: {symbol} - Tradeable: {result.get('tradeable')}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Gemini Flash error for {symbol}: {e}")
            return {"tradeable": False, "reason": str(e), "confidence": 0}
    
    async def gemini_pro_analyze(self, symbol, chart_buf, option_text, candle_text, flash_result):
        """Gemini Pro - Deep analysis"""
        try:
            logger.info(f"🎯 Gemini Pro analyzing {symbol}...")
            
            chart_buf.seek(0)
            image_bytes = chart_buf.read()
            
            prompt = f"""You are an expert F&O trader. Gemini Flash identified this as tradeable: {flash_result}

{option_text}

{candle_text}

TASK: Create a detailed trading strategy.
Provide:
1. Entry point (strike price)
2. Target price
3. Stop loss
4. Risk:Reward ratio
5. Time frame (exit by what time)
6. Key levels to watch

Respond in JSON format:
{{
    "entry_strike": strike,
    "entry_price": price,
    "target_price": price,
    "stop_loss": price,
    "risk_reward": ratio,
    "exit_time": "time",
    "confidence": 0-100,
    "strategy": "detailed explanation"
}}
"""
            
            response = self.gemini_pro.generate_content([
                prompt,
                {"mime_type": "image/png", "data": image_bytes}
            ])
            
            result_text = response.text.replace('```json', '').replace('```', '').strip()
            result = json.loads(result_text)
            logger.info(f"✅ Gemini Pro: {symbol} - Entry @ ₹{result.get('entry_price')}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Gemini Pro error for {symbol}: {e}")
            return None
    
    async def gpt4o_validate(self, symbol, chart_buf, option_text, candle_text, gemini_result):
        """GPT-4o Vision - Final validation"""
        try:
            logger.info(f"🤖 GPT-4o validating {symbol}...")
            
            chart_buf.seek(0)
            image_base64 = base64.b64encode(chart_buf.read()).decode('utf-8')
            
            prompt = f"""You are a F&O trading validator. Gemini Pro suggested this trade: {gemini_result}

{option_text}

{candle_text}

TASK: Validate this trade setup. Check:
1. Chart pattern accuracy
2. Risk:Reward makes sense
3. Entry/Exit points are logical
4. Greeks and IV are favorable

Respond in JSON format:
{{
    "valid": true/false,
    "confidence": 0-100,
    "adjustments": "any suggested changes",
    "final_verdict": "go/no-go with reason"
}}
"""
            
            response = openai_client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/png;base64,{image_base64}"
                                }
                            }
                        ]
                    }
                ],
                max_tokens=1000
            )
            
            result_text = response.choices[0].message.content.replace('```json', '').replace('```', '').strip()
            result = json.loads(result_text)
            logger.info(f"✅ GPT-4o: {symbol} - Valid: {result.get('valid')}, Confidence: {result.get('confidence')}%")
            return result
            
        except Exception as e:
            logger.error(f"❌ GPT-4o error for {symbol}: {e}")
            return None
    
    async def process_stock(self, symbol):
        """Complete pipeline for one stock"""
        try:
            if symbol not in self.security_id_map:
                logger.warning(f"⚠️ {symbol} not found in security map")
                return None
            
            info = self.security_id_map[symbol]
            security_id = info['security_id']
            segment = info['segment']
            
            # Get expiry
            expiry = self.get_nearest_expiry(security_id, segment)
            if not expiry:
                logger.warning(f"⚠️ {symbol}: No expiry found")
                return None
            
            # Get option chain
            oc_data = self.get_option_chain(security_id, segment, expiry)
            if not oc_data:
                logger.warning(f"⚠️ {symbol}: No option chain data")
                return None
            
            spot_price = oc_data.get('last_price', 0)
            
            # Pre-filter
            passed, filter_result = self.pre_filter_stock(symbol, oc_data)
            if not passed:
                logger.info(f"❌ {symbol} filtered out: {filter_result}")
                return None
            
            # Get candles
            candles = self.get_historical_data(security_id, segment, symbol)
            if not candles or len(candles) < 10:
                logger.warning(f"⚠️ {symbol}: Insufficient candle data")
                return None
            
            # Create chart
            chart_buf = self.create_candlestick_chart(candles, symbol, spot_price)
            if not chart_buf:
                logger.warning(f"⚠️ {symbol}: Could not create chart")
                return None
            
            # Format data for AI
            option_text = self.format_option_data_for_ai(symbol, oc_data, spot_price)
            candle_text = self.format_candle_data_for_ai(candles)
            
            # Stage 1: Gemini Flash
            flash_result = await self.gemini_flash_scan(symbol, chart_buf, option_text, candle_text)
            if not flash_result.get('tradeable', False):
                logger.info(f"❌ {symbol} not tradeable per Gemini Flash")
                return None
            
            # Stage 2: Gemini Pro
            pro_result = await self.gemini_pro_analyze(symbol, chart_buf, option_text, candle_text, flash_result)
            if not pro_result:
                logger.warning(f"⚠️ {symbol}: Gemini Pro analysis failed")
                return None
            
            # Stage 3: GPT-4o
            gpt_result = await self.gpt4o_validate(symbol, chart_buf, option_text, candle_text, pro_result)
            if not gpt_result or not gpt_result.get('valid', False):
                logger.info(f"❌ {symbol} rejected by GPT-4o")
                return None
            
            # Compile final trade signal
            trade_signal = {
                'symbol': symbol,
                'spot_price': spot_price,
                'flash': flash_result,
                'pro': pro_result,
                'gpt': gpt_result,
                'filter': filter_result,
                'expiry': expiry,
                'chart': chart_buf
            }
            
            logger.info(f"🎯 {symbol} TRADE SIGNAL GENERATED!")
            return trade_signal
            
        except Exception as e:
            logger.error(f"❌ Error processing {symbol}: {e}")
            return None
