import asyncio
import os
from telegram import Bot
import requests
from datetime import datetime
import logging
import pandas as pd
from io import StringIO

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
DHAN_INSTRUMENTS_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# Stock/Index List - Symbol mapping
STOCKS_INDICES = {
    # Indices
    "NIFTY 50": {"symbol": "NIFTY 50", "segment": "IDX_I"},
    "NIFTY BANK": {"symbol": "NIFTY BANK", "segment": "IDX_I"},
    "SENSEX": {"symbol": "SENSEX", "segment": "IDX_I"},
    
    # Stocks
    "RELIANCE": {"symbol": "RELIANCE", "segment": "NSE_EQ"},
    "HDFCBANK": {"symbol": "HDFCBANK", "segment": "NSE_EQ"},
    "ICICIBANK": {"symbol": "ICICIBANK", "segment": "NSE_EQ"},
    "BAJFINANCE": {"symbol": "BAJFINANCE", "segment": "NSE_EQ"},
    "INFY": {"symbol": "INFY", "segment": "NSE_EQ"},
    "TATAMOTORS": {"symbol": "TATAMOTORS", "segment": "NSE_EQ"},
    "AXISBANK": {"symbol": "AXISBANK", "segment": "NSE_EQ"},
    "SBIN": {"symbol": "SBIN", "segment": "NSE_EQ"},
    "LTIM": {"symbol": "LTIM", "segment": "NSE_EQ"},
    "ADANIENT": {"symbol": "ADANIENT", "segment": "NSE_EQ"},
    "KOTAKBANK": {"symbol": "KOTAKBANK", "segment": "NSE_EQ"},
    "LT": {"symbol": "LT", "segment": "NSE_EQ"},
    "MARUTI": {"symbol": "MARUTI", "segment": "NSE_EQ"},
    "TECHM": {"symbol": "TECHM", "segment": "NSE_EQ"},
    "LICI": {"symbol": "LICI", "segment": "NSE_EQ"},
    "HINDUNILVR": {"symbol": "HINDUNILVR", "segment": "NSE_EQ"},
    "NTPC": {"symbol": "NTPC", "segment": "NSE_EQ"},
    "BHARTIARTL": {"symbol": "BHARTIARTL", "segment": "NSE_EQ"},
    "POWERGRID": {"symbol": "POWERGRID", "segment": "NSE_EQ"},
    "ONGC": {"symbol": "ONGC", "segment": "NSE_EQ"},
    "PERSISTENT": {"symbol": "PERSISTENT", "segment": "NSE_EQ"},
    "DRREDDY": {"symbol": "DRREDDY", "segment": "NSE_EQ"},
    "M&M": {"symbol": "M&M", "segment": "NSE_EQ"},
    "WIPRO": {"symbol": "WIPRO", "segment": "NSE_EQ"},
    "DMART": {"symbol": "DMART", "segment": "NSE_EQ"},
    "TRENT": {"symbol": "TRENT", "segment": "NSE_EQ"},
}

# ========================
# BOT CODE
# ========================

class DhanOptionChainBot:
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
        logger.info("Bot initialized successfully")
    
    async def load_security_ids(self):
        """Dhan मधून security IDs load करतो"""
        try:
            logger.info("Loading security IDs from Dhan...")
            response = requests.get(DHAN_INSTRUMENTS_URL, timeout=30)
            
            if response.status_code == 200:
                # CSV parse करतो
                df = pd.read_csv(StringIO(response.text))
                
                for symbol, info in STOCKS_INDICES.items():
                    segment = info['segment']
                    symbol_name = info['symbol']
                    
                    # Security ID शोधतो
                    if segment == "IDX_I":
                        # Index साठी
                        mask = (df['SEM_SEGMENT'] == 'I') & (df['SEM_TRADING_SYMBOL'] == symbol_name)
                    else:
                        # Stock साठी
                        mask = (df['SEM_SEGMENT'] == 'E') & (df['SEM_TRADING_SYMBOL'] == symbol_name) & (df['SEM_EXM_EXCH_ID'] == 'NSE')
                    
                    filtered = df[mask]
                    
                    if not filtered.empty:
                        sec_id = filtered.iloc[0]['SEM_SMST_SECURITY_ID']
                        self.security_id_map[symbol] = {
                            'security_id': int(sec_id),
                            'segment': segment,
                            'trading_symbol': symbol_name
                        }
                        logger.info(f"✅ {symbol}: Security ID = {sec_id}")
                    else:
                        logger.warning(f"❌ {symbol}: Security ID नाही मिळाला!")
                
                logger.info(f"Total {len(self.security_id_map)} securities loaded")
                return True
            else:
                logger.error(f"Failed to load instruments: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error loading security IDs: {e}")
            return False
    
    def get_nearest_expiry(self, security_id, segment):
        """सर्वात जवळचा expiry काढतो"""
        try:
            payload = {
                "UnderlyingScrip": security_id,
                "UnderlyingSeg": segment
            }
            
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
                        return expiries[0]  # पहिला expiry = nearest
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting expiry: {e}")
            return None
    
    def get_option_chain(self, security_id, segment, expiry):
        """Option chain data घेतो"""
        try:
            payload = {
                "UnderlyingScrip": security_id,
                "UnderlyingSeg": segment,
                "Expiry": expiry
            }
            
            response = requests.post(
                DHAN_OPTION_CHAIN_URL,
                json=payload,
                headers=self.headers,
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('data'):
                    return data['data']
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting option chain: {e}")
            return None
    
    def format_option_chain_message(self, symbol, data, expiry):
        """Option chain साठी सुंदर message format"""
        try:
            spot_price = data.get('last_price', 0)
            oc_data = data.get('oc', {})
            
            if not oc_data:
                return None
            
            # ATM strike शोधतो
            strikes = sorted([float(s) for s in oc_data.keys()])
            atm_strike = min(strikes, key=lambda x: abs(x - spot_price))
            
            # ATM च्या आजूबाजूचे 5 strikes घेतो (एकूण 11)
            atm_idx = strikes.index(atm_strike)
            start_idx = max(0, atm_idx - 5)
            end_idx = min(len(strikes), atm_idx + 6)
            selected_strikes = strikes[start_idx:end_idx]
            
            # Message तयार करतो
            msg = f"📊 *{symbol} OPTION CHAIN*\n"
            msg += f"📅 Expiry: {expiry}\n"
            msg += f"💰 Spot: ₹{spot_price:,.2f}\n"
            msg += f"🎯 ATM: ₹{atm_strike:,.0f}\n\n"
            
            msg += "```\n"
            msg += "Strike   CE-LTP  CE-OI  CE-Vol  PE-LTP  PE-OI  PE-Vol\n"
            msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            
            for strike in selected_strikes:
                strike_key = f"{strike:.6f}"
                strike_data = oc_data.get(strike_key, {})
                
                ce = strike_data.get('ce', {})
                pe = strike_data.get('pe', {})
                
                ce_ltp = ce.get('last_price', 0)
                ce_oi = ce.get('oi', 0)
                ce_vol = ce.get('volume', 0)
                
                pe_ltp = pe.get('last_price', 0)
                pe_oi = pe.get('oi', 0)
                pe_vol = pe.get('volume', 0)
                
                # ATM mark करतो
                atm_mark = "🔸" if strike == atm_strike else "  "
                
                msg += f"{atm_mark}{strike:6.0f}  {ce_ltp:6.1f} {ce_oi/1000:6.0f}K {ce_vol/1000:6.0f}K  {pe_ltp:6.1f} {pe_oi/1000:6.0f}K {pe_vol/1000:6.0f}K\n"
            
            msg += "```\n\n"
            
            # Greeks आणि IV (ATM साठी)
            atm_data = oc_data.get(f"{atm_strike:.6f}", {})
            if atm_data:
                ce_greeks = atm_data.get('ce', {}).get('greeks', {})
                pe_greeks = atm_data.get('pe', {}).get('greeks', {})
                ce_iv = atm_data.get('ce', {}).get('implied_volatility', 0)
                pe_iv = atm_data.get('pe', {}).get('implied_volatility', 0)
                
                msg += "📈 *ATM Greeks & IV:*\n"
                msg += f"CE: Δ={ce_greeks.get('delta', 0):.3f} Θ={ce_greeks.get('theta', 0):.2f} IV={ce_iv:.1f}%\n"
                msg += f"PE: Δ={pe_greeks.get('delta', 0):.3f} Θ={pe_greeks.get('theta', 0):.2f} IV={pe_iv:.1f}%\n"
            
            return msg
            
        except Exception as e:
            logger.error(f"Error formatting message for {symbol}: {e}")
            return None
    
    async def send_option_chain_batch(self, symbols_batch):
        """एका batch चे option chain data पाठवतो"""
        for symbol in symbols_batch:
            try:
                if symbol not in self.security_id_map:
                    logger.warning(f"Skipping {symbol} - No security ID")
                    continue
                
                info = self.security_id_map[symbol]
                security_id = info['security_id']
                segment = info['segment']
                
                # Nearest expiry शोधतो
                expiry = self.get_nearest_expiry(security_id, segment)
                if not expiry:
                    logger.warning(f"{symbol}: Expiry नाही मिळाला")
                    continue
                
                logger.info(f"Fetching option chain for {symbol} (Expiry: {expiry})...")
                
                # Option chain data घेतो
                oc_data = self.get_option_chain(security_id, segment, expiry)
                if not oc_data:
                    logger.warning(f"{symbol}: Option chain data नाही मिळाला")
                    continue
                
                # Message format करतो
                message = self.format_option_chain_message(symbol, oc_data, expiry)
                if message:
                    await self.bot.send_message(
                        chat_id=TELEGRAM_CHAT_ID,
                        text=message,
                        parse_mode='Markdown'
                    )
                    logger.info(f"✅ {symbol} option chain sent")
                
                # Rate limit साठी थांबतो (3 seconds per request as per Dhan)
                await asyncio.sleep(3)
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                await asyncio.sleep(3)
    
    async def run(self):
        """Main loop - every 5 minutes option chain पाठवतो"""
        logger.info("🚀 Bot started! Loading security IDs...")
        
        # Security IDs load करतो
        success = await self.load_security_ids()
        if not success:
            logger.error("Failed to load security IDs. Exiting...")
            return
        
        await self.send_startup_message()
        
        # Symbols ला batches मध्ये divide करतो (5 per batch)
        all_symbols = list(self.security_id_map.keys())
        batch_size = 5
        batches = [all_symbols[i:i+batch_size] for i in range(0, len(all_symbols), batch_size)]
        
        logger.info(f"Total {len(all_symbols)} symbols in {len(batches)} batches")
        
        while self.running:
            try:
                timestamp = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                logger.info(f"\n{'='*50}")
                logger.info(f"Starting option chain update cycle at {timestamp}")
                logger.info(f"{'='*50}")
                
                # प्रत्येक batch process करतो
                for batch_num, batch in enumerate(batches, 1):
                    logger.info(f"\n📦 Processing Batch {batch_num}/{len(batches)}: {batch}")
                    await self.send_option_chain_batch(batch)
                    
                    # Batches मध्ये 5 second gap
                    if batch_num < len(batches):
                        logger.info(f"Waiting 5 seconds before next batch...")
                        await asyncio.sleep(5)
                
                logger.info("\n✅ All batches completed!")
                logger.info("⏳ Waiting 5 minutes for next cycle...\n")
                
                # 5 minutes wait
                await asyncio.sleep(300)
                
            except KeyboardInterrupt:
                logger.info("Bot stopped by user")
                self.running = False
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(60)
    
    async def send_startup_message(self):
        """Bot सुरू झाल्यावर message पाठवतो"""
        try:
            msg = "🤖 *Dhan Option Chain Bot Started!*\n\n"
            msg += f"📊 Tracking {len(self.security_id_map)} stocks/indices\n"
            msg += "⏱️ Updates every 5 minutes\n"
            msg += "📈 Option Chain: CE/PE LTP, OI, Volume, Greeks, IV\n\n"
            msg += "✅ Powered by DhanHQ API v2\n"
            msg += "🚂 Deployed on Railway.app\n\n"
            msg += "_Market Hours: 9:15 AM - 3:30 PM (Mon-Fri)_"
            
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=msg,
                parse_mode='Markdown'
            )
            logger.info("Startup message sent")
        except Exception as e:
            logger.error(f"Error sending startup message: {e}")


# ========================
# BOT RUN करा
# ========================
if __name__ == "__main__":
    try:
        # Environment variables check
        if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN]):
            logger.error("❌ Missing environment variables!")
            logger.error("Please set: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN")
            exit(1)
        
        bot = DhanOptionChainBot()
        asyncio.run(bot.run())
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        exit(1)
