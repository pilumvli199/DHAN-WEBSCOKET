import asyncio
import os
from telegram import Bot
import requests
from datetime import datetime, timedelta
import logging
import json
import websockets
from typing import Dict, List

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

# Dhan API URLs
DHAN_API_BASE = "https://api.dhan.co"
DHAN_OHLC_URL = f"{DHAN_API_BASE}/v2/marketfeed/ohlc"
DHAN_OPTION_CHAIN_URL = f"{DHAN_API_BASE}/v2/optionchain"
DHAN_EXPIRY_LIST_URL = f"{DHAN_API_BASE}/v2/optionchain/expirylist"
DHAN_HISTORICAL_URL = f"{DHAN_API_BASE}/v2/charts/historical"

# Dhan WebSocket URL
DHAN_WS_URL = "wss://api-feed.dhan.co"

# Nifty 50 Config
NIFTY_50_SECURITY_ID = 13
NIFTY_SEGMENT = "IDX_I"

# WebSocket message types
WS_CONNECT = 2
WS_SUBSCRIBE = 15
WS_UNSUBSCRIBE = 16

# ========================
# WEBSOCKET BOT CODE
# ========================

class NiftyWebSocketBot:
    def __init__(self):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.running = True
        self.headers = {
            'access-token': DHAN_ACCESS_TOKEN,
            'client-id': DHAN_CLIENT_ID,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.current_expiry = None
        self.ws = None
        self.last_ltp_data = None
        self.last_message_time = 0
        self.message_interval = 60  # Send telegram message every 60 seconds
        logger.info("WebSocket Bot initialized successfully")
    
    def get_nearest_expiry(self):
        """Nearest expiry date मिळवतो"""
        try:
            payload = {
                "UnderlyingScrip": NIFTY_50_SECURITY_ID,
                "UnderlyingSeg": NIFTY_SEGMENT
            }
            
            response = requests.post(
                DHAN_EXPIRY_LIST_URL,
                json=payload,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success' and 'data' in data:
                    expiries = data['data']
                    if expiries:
                        self.current_expiry = expiries[0]
                        logger.info(f"Nearest expiry: {self.current_expiry}")
                        return self.current_expiry
            
            return None
        except Exception as e:
            logger.error(f"Error getting expiry: {e}")
            return None
    
    def get_nifty_ltp(self):
        """REST API - Nifty 50 LTP (fallback)"""
        try:
            payload = {
                "IDX_I": [NIFTY_50_SECURITY_ID]
            }
            
            response = requests.post(
                DHAN_OHLC_URL,
                json=payload,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'success' and 'data' in data:
                    idx_data = data['data'].get('IDX_I', {})
                    nifty_data = idx_data.get(str(NIFTY_50_SECURITY_ID), {})
                    
                    if nifty_data and 'last_price' in nifty_data:
                        ltp = nifty_data['last_price']
                        ohlc = nifty_data.get('ohlc', {})
                        
                        result = {
                            'ltp': ltp,
                            'open': ohlc.get('open', 0),
                            'high': ohlc.get('high', 0),
                            'low': ohlc.get('low', 0),
                            'close': ohlc.get('close', 0)
                        }
                        
                        if result['close'] > 0:
                            result['change'] = ltp - result['close']
                            result['change_pct'] = (result['change'] / result['close']) * 100
                        else:
                            result['change'] = 0
                            result['change_pct'] = 0
                        
                        return result
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting LTP: {e}")
            return None
    
    def get_historical_data(self, days=5):
        """Historical data"""
        try:
            to_date = datetime.now().strftime("%Y-%m-%d")
            from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            
            payload = {
                "securityId": str(NIFTY_50_SECURITY_ID),
                "exchangeSegment": NIFTY_SEGMENT,
                "instrument": "INDEX",
                "expiryCode": 0,
                "oi": False,
                "fromDate": from_date,
                "toDate": to_date
            }
            
            response = requests.post(
                DHAN_HISTORICAL_URL,
                json=payload,
                headers=self.headers,
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if 'open' in data and 'close' in data:
                    timestamps = data.get('timestamp', [])
                    opens = data.get('open', [])
                    highs = data.get('high', [])
                    lows = data.get('low', [])
                    closes = data.get('close', [])
                    volumes = data.get('volume', [])
                    
                    parsed_data = []
                    for i in range(len(timestamps)):
                        parsed_data.append({
                            'date': datetime.fromtimestamp(timestamps[i]).strftime('%Y-%m-%d'),
                            'open': opens[i] if i < len(opens) else 0,
                            'high': highs[i] if i < len(highs) else 0,
                            'low': lows[i] if i < len(lows) else 0,
                            'close': closes[i] if i < len(closes) else 0,
                            'volume': volumes[i] if i < len(volumes) else 0
                        })
                    
                    return parsed_data[-5:]  # Last 5 days
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting historical: {e}")
            return None
    
    def get_option_chain_with_greeks(self):
        """Option Chain with Greeks & Volume"""
        try:
            if not self.current_expiry:
                self.get_nearest_expiry()
            
            if not self.current_expiry:
                return None
            
            payload = {
                "UnderlyingScrip": NIFTY_50_SECURITY_ID,
                "UnderlyingSeg": NIFTY_SEGMENT,
                "Expiry": self.current_expiry
            }
            
            response = requests.post(
                DHAN_OPTION_CHAIN_URL,
                json=payload,
                headers=self.headers,
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'success' and 'data' in data:
                    spot_price = data['data'].get('last_price', 0)
                    oc_data = data['data'].get('oc', {})
                    
                    if not oc_data:
                        return None
                    
                    strikes = sorted([float(s) for s in oc_data.keys()])
                    atm_strike = min(strikes, key=lambda x: abs(x - spot_price))
                    atm_index = strikes.index(atm_strike)
                    
                    # 5 strikes around ATM
                    start_idx = max(0, atm_index - 5)
                    end_idx = min(len(strikes), atm_index + 6)
                    selected_strikes = strikes[start_idx:end_idx]
                    
                    option_data = []
                    for strike in selected_strikes:
                        strike_key = f"{strike:.6f}"
                        strike_data = oc_data.get(strike_key, {})
                        
                        ce_data = strike_data.get('ce', {})
                        pe_data = strike_data.get('pe', {})
                        ce_greeks = ce_data.get('greeks', {})
                        pe_greeks = pe_data.get('greeks', {})
                        
                        option_data.append({
                            'strike': strike,
                            'ce_ltp': ce_data.get('last_price', 0),
                            'ce_oi': ce_data.get('oi', 0),
                            'ce_volume': ce_data.get('volume', 0),
                            'ce_iv': ce_data.get('implied_volatility', 0),
                            'ce_delta': ce_greeks.get('delta', 0),
                            'ce_theta': ce_greeks.get('theta', 0),
                            'ce_gamma': ce_greeks.get('gamma', 0),
                            'ce_vega': ce_greeks.get('vega', 0),
                            'pe_ltp': pe_data.get('last_price', 0),
                            'pe_oi': pe_data.get('oi', 0),
                            'pe_volume': pe_data.get('volume', 0),
                            'pe_iv': pe_data.get('implied_volatility', 0),
                            'pe_delta': pe_greeks.get('delta', 0),
                            'pe_theta': pe_greeks.get('theta', 0),
                            'pe_gamma': pe_greeks.get('gamma', 0),
                            'pe_vega': pe_greeks.get('vega', 0),
                            'is_atm': (strike == atm_strike)
                        })
                    
                    return {
                        'spot': spot_price,
                        'atm': atm_strike,
                        'expiry': self.current_expiry,
                        'options': option_data
                    }
            
            return None
            
        except Exception as e:
            logger.error(f"Error option chain: {e}")
            return None
    
    async def connect_websocket(self):
        """WebSocket connection बनवतो"""
        try:
            # Dhan WebSocket needs headers
            headers = {
                "Authorization": f"{DHAN_ACCESS_TOKEN}",
                "client-id": DHAN_CLIENT_ID
            }
            
            logger.info(f"🔌 Connecting to {DHAN_WS_URL}")
            self.ws = await websockets.connect(
                DHAN_WS_URL,
                extra_headers=headers,
                ping_interval=20,
                ping_timeout=10
            )
            logger.info("✅ WebSocket connected")
            
            # Wait for connection confirmation
            await asyncio.sleep(1)
            
            # Subscribe to Nifty 50 Index
            # Dhan format: {"RequestCode":15,"InstrumentType":1,"SecurityId":"13"}
            subscribe_msg = {
                "RequestCode": 15,
                "InstrumentType": 1,
                "SecurityId": str(NIFTY_50_SECURITY_ID)
            }
            
            await self.ws.send(json.dumps(subscribe_msg))
            logger.info(f"📊 Subscribed to Nifty 50 (ID: {NIFTY_50_SECURITY_ID})")
            logger.info(f"📤 Sent: {subscribe_msg}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ WebSocket connection error: {e}")
            logger.exception("Full traceback:")
            return False
    
    def parse_websocket_message(self, data: Dict) -> Dict:
        """WebSocket message parse करतो"""
        try:
            logger.info(f"🔍 Parsing data keys: {data.keys() if isinstance(data, dict) else type(data)}")
            
            # Dhan WebSocket format variations
            if isinstance(data, dict):
                # Try different field names
                ltp = (data.get('LTP') or 
                       data.get('ltp') or 
                       data.get('last_price') or 
                       data.get('lastPrice') or 
                       data.get('close') or 0)
                
                open_price = (data.get('open') or 
                             data.get('Open') or 
                             data.get('openPrice') or 0)
                
                high = (data.get('high') or 
                       data.get('High') or 
                       data.get('highPrice') or 0)
                
                low = (data.get('low') or 
                      data.get('Low') or 
                      data.get('lowPrice') or 0)
                
                close = (data.get('close') or 
                        data.get('Close') or 
                        data.get('prev_close') or 
                        data.get('prevClose') or 0)
                
                # If no LTP found, return None
                if ltp == 0:
                    logger.warning(f"⚠️ No LTP found in data: {list(data.keys())}")
                    return None
                
                result = {
                    'ltp': float(ltp),
                    'open': float(open_price),
                    'high': float(high),
                    'low': float(low),
                    'close': float(close)
                }
                
                if result['close'] > 0:
                    result['change'] = result['ltp'] - result['close']
                    result['change_pct'] = (result['change'] / result['close']) * 100
                else:
                    result['change'] = 0
                    result['change_pct'] = 0
                
                logger.info(f"✅ Parsed LTP: {result['ltp']:.2f}")
                return result
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Error parsing WS message: {e}")
            logger.exception("Parse error details:")
            return None
    
    async def handle_websocket_messages(self):
        """WebSocket messages handle करतो"""
        try:
            async for message in self.ws:
                try:
                    logger.info(f"📥 Raw WS Message: {message[:200]}")  # First 200 chars
                    
                    data = json.loads(message)
                    logger.info(f"📦 Parsed Data: {data}")
                    
                    # Dhan WebSocket response format check
                    # Format 1: {"type":"ticker","data":{...}}
                    # Format 2: Direct data object
                    
                    actual_data = data
                    if isinstance(data, dict):
                        if 'data' in data:
                            actual_data = data['data']
                        elif 'type' in data and data['type'] == 'ticker':
                            actual_data = data.get('data', data)
                    
                    # Parse the message
                    parsed = self.parse_websocket_message(actual_data)
                    
                    if parsed:
                        self.last_ltp_data = parsed
                        
                        # Send telegram message at intervals
                        current_time = datetime.now().timestamp()
                        if current_time - self.last_message_time >= self.message_interval:
                            await self.send_nifty_ltp(parsed)
                            self.last_message_time = current_time
                            logger.info(f"📊 LTP Updated: {parsed['ltp']:.2f}")
                    else:
                        logger.warning(f"⚠️ Could not parse: {actual_data}")
                
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON: {message[:100]} | Error: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    logger.exception("Full error:")
                    
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"⚠️ WebSocket connection closed: {e}")
            return False
        except Exception as e:
            logger.error(f"WebSocket handler error: {e}")
            logger.exception("Full traceback:")
            return False
        
        return True
    
    async def send_option_chain_message(self, option_data):
        """Option Chain telegram message"""
        try:
            message = f"📊 *OPTION CHAIN*\n"
            message += f"📅 Expiry: {option_data['expiry']}\n"
            message += f"💰 Spot: ₹{option_data['spot']:,.2f}\n"
            message += f"🎯 ATM: ₹{option_data['atm']:,.0f}\n\n"
            
            message += "```\n"
            message += "Strike   CE-LTP  CE-OI  CE-Vol  PE-LTP  PE-OI  PE-Vol\n"
            message += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            
            for opt in option_data['options']:
                atm = "🔸" if opt['is_atm'] else "  "
                
                ce_ltp = f"{opt['ce_ltp']:6.1f}" if opt['ce_ltp'] > 0 else "  -   "
                ce_oi = f"{opt['ce_oi']/1000:5.0f}K" if opt['ce_oi'] > 0 else "  -  "
                ce_vol = f"{opt['ce_volume']/1000:5.0f}K" if opt['ce_volume'] > 0 else "  -  "
                
                pe_ltp = f"{opt['pe_ltp']:6.1f}" if opt['pe_ltp'] > 0 else "  -   "
                pe_oi = f"{opt['pe_oi']/1000:5.0f}K" if opt['pe_oi'] > 0 else "  -  "
                pe_vol = f"{opt['pe_volume']/1000:5.0f}K" if opt['pe_volume'] > 0 else "  -  "
                
                message += f"{atm}{opt['strike']:5.0f} {ce_ltp} {ce_oi} {ce_vol}  {pe_ltp} {pe_oi} {pe_vol}\n"
            
            message += "```"
            
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=message,
                parse_mode='Markdown'
            )
            logger.info("✅ Option chain sent")
            
        except Exception as e:
            logger.error(f"Error sending OC: {e}")
    
    async def send_greeks_message(self, option_data):
        """Greeks telegram message"""
        try:
            atm_opt = next((o for o in option_data['options'] if o['is_atm']), None)
            if not atm_opt:
                return
            
            message = f"🎲 *GREEKS - ATM {atm_opt['strike']:.0f}*\n\n"
            message += "```\n"
            message += "         CALL         PUT\n"
            message += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            message += f"Delta:  {atm_opt['ce_delta']:7.4f}   {atm_opt['pe_delta']:7.4f}\n"
            message += f"Theta:  {atm_opt['ce_theta']:7.2f}   {atm_opt['pe_theta']:7.2f}\n"
            message += f"Gamma:  {atm_opt['ce_gamma']:7.5f}   {atm_opt['pe_gamma']:7.5f}\n"
            message += f"Vega:   {atm_opt['ce_vega']:7.2f}   {atm_opt['pe_vega']:7.2f}\n"
            message += f"IV:     {atm_opt['ce_iv']:6.2f}%   {atm_opt['pe_iv']:6.2f}%\n"
            message += "```"
            
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=message,
                parse_mode='Markdown'
            )
            logger.info("✅ Greeks sent")
            
        except Exception as e:
            logger.error(f"Error Greeks: {e}")
    
    async def send_historical_message(self, hist_data):
        """Historical data telegram message"""
        try:
            if not hist_data:
                return
            
            message = f"📈 *HISTORICAL DATA (Last {len(hist_data)} Days)*\n\n"
            message += "```\n"
            message += "Date         Open     High      Low    Close     Vol\n"
            message += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            
            for day in hist_data:
                date = day['date']
                message += f"{date} {day['open']:7.2f} {day['high']:7.2f} "
                message += f"{day['low']:7.2f} {day['close']:7.2f} "
                message += f"{day['volume']/1000000:5.1f}M\n"
            
            message += "```"
            
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=message,
                parse_mode='Markdown'
            )
            logger.info("✅ Historical sent")
            
        except Exception as e:
            logger.error(f"Error historical: {e}")
    
    async def send_nifty_ltp(self, data):
        """LTP telegram message"""
        try:
            emoji = "🟢" if data['change'] >= 0 else "🔴"
            sign = "+" if data['change'] >= 0 else ""
            
            msg = f"📊 *NIFTY 50* (Live)\n"
            msg += f"💰 {data['ltp']:,.2f} {emoji} {sign}{data['change']:,.2f} ({sign}{data['change_pct']:.2f}%)"
            
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=msg,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            logger.error(f"Error LTP: {e}")
    
    async def option_chain_task(self):
        """Background task for option chain updates"""
        while self.running:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                
                oc = self.get_option_chain_with_greeks()
                if oc:
                    await self.send_option_chain_message(oc)
                    await asyncio.sleep(2)
                    await self.send_greeks_message(oc)
                    
            except Exception as e:
                logger.error(f"Option chain task error: {e}")
    
    async def run(self):
        """Main WebSocket loop with REST API fallback"""
        logger.info("🚀 WebSocket Bot started!")
        
        await self.send_startup_message()
        
        # Initial setup
        self.get_nearest_expiry()
        
        # Historical data
        await asyncio.sleep(2)
        hist = self.get_historical_data(5)
        if hist:
            await self.send_historical_message(hist)
        
        # Start option chain background task
        asyncio.create_task(self.option_chain_task())
        
        # Try WebSocket first
        ws_failed_count = 0
        use_rest_fallback = False
        
        while self.running:
            try:
                if not use_rest_fallback and ws_failed_count < 3:
                    # Try WebSocket
                    logger.info("🔌 Attempting WebSocket connection...")
                    if await self.connect_websocket():
                        ws_failed_count = 0
                        # Handle messages
                        success = await self.handle_websocket_messages()
                        if not success:
                            ws_failed_count += 1
                    else:
                        ws_failed_count += 1
                    
                    if ws_failed_count >= 3:
                        logger.warning("⚠️ WebSocket failed 3 times, switching to REST API fallback")
                        use_rest_fallback = True
                
                if use_rest_fallback:
                    # REST API fallback
                    logger.info("🔄 Using REST API mode...")
                    await self.rest_api_loop()
                
                logger.warning("⚠️ Reconnecting in 5 seconds...")
                await asyncio.sleep(5)
                
            except KeyboardInterrupt:
                logger.info("⛔ Shutting down...")
                self.running = False
                break
            except Exception as e:
                logger.error(f"Main loop error: {e}")
                logger.exception("Error details:")
                await asyncio.sleep(5)
        
        # Cleanup
        if self.ws:
            await self.ws.close()
    
    async def rest_api_loop(self):
        """REST API fallback loop"""
        logger.info("📡 REST API mode activated")
        for i in range(60):  # Run for 60 iterations then retry WebSocket
            try:
                nifty = self.get_nifty_ltp()
                if nifty:
                    await self.send_nifty_ltp(nifty)
                    self.last_ltp_data = nifty
                
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"REST API error: {e}")
                break
        
        logger.info("🔄 Retrying WebSocket...")
    
    async def send_startup_message(self):
        """Startup message"""
        try:
            msg = "🤖 *Nifty WebSocket Bot v3*\n\n"
            msg += "✅ Real-time LTP via WebSocket\n"
            msg += "✅ Option Chain - Every 5 min\n"
            msg += "✅ Greeks (Δ Θ Γ V)\n"
            msg += "✅ Historical Data (5 days)\n\n"
            msg += "⚡ WebSocket-powered\n"
            msg += "🚂 Railway.app"
            
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=msg,
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Startup: {e}")


if __name__ == "__main__":
    try:
        if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN]):
            logger.error("❌ Missing env vars!")
            exit(1)
        
        bot = NiftyWebSocketBot()
        asyncio.run(bot.run())
    except Exception as e:
        logger.error(f"Fatal: {e}")
        exit(1)
