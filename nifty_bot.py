#!/usr/bin/env python3
# updated_bot.py - Nifty + Sensex + multiple stocks LTP bot (Dhan API v2, Telegram)
# Behaves mostly like your original; now fetches a list of equities + indices and posts a single consolidated message every minute.

import asyncio
import os
from telegram import Bot
import requests
from datetime import datetime
import logging

# -------------------------
# Logging setup
# -------------------------
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# -------------------------
# Configuration (env)
# -------------------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

# Dhan API URLs (same as before)
DHAN_API_BASE = "https://api.dhan.co"
DHAN_LTP_URL = f"{DHAN_API_BASE}/v2/marketfeed/ltp"   # symbol-based LTP endpoint (best effort)
DHAN_OHLC_URL = f"{DHAN_API_BASE}/v2/marketfeed/ohlc" # for index OHLC (used for NIFTY in fallback)

# ========================
# Instruments (user list)
# Map display name -> exchange symbol (NSE symbol)
# If your Dhan API needs numeric security IDs, add them to EXTRA_INDEX_IDS below.
# ========================
INSTRUMENTS = {
    "RELIANCE": "RELIANCE",
    "HDFC Bank": "HDFCBANK",
    "ICICI Bank": "ICICIBANK",
    "Bajaj Finance": "BAJFINANCE",
    "Infosys": "INFY",
    "Tata Motors": "TATAMOTORS",
    "Axis Bank": "AXISBANK",
    "State Bank of India": "SBIN",
    "LTIMindtree": "LTIM",
    "Adani Enterprises": "ADANIENT",
    "Kotak Mahindra Bank": "KOTAKBANK",
    "Larsen & Toubro": "LT",
    "Maruti Suzuki": "MARUTI",
    "Tech Mahindra": "TECHM",
    "LIC of India": "LICI",
    "Hindustan Unilever": "HINDUNILVR",
    "NTPC Ltd": "NTPC",
    "Bharti Airtel": "BHARTIARTL",
    "Power Grid": "POWERGRID",
    "ONGC": "ONGC",
    "Persistent Systems": "PERSISTENT",
    "DRREDDY": "DRREDDY",
    "M&M": "M&M",  # Many APIs use 'M&M' or 'M&M.NS' - keep it as M&M for now; adjust if needed
    "Wipro": "WIPRO",
    "Dmart": "AVENUE_SUPERMARTS",  # user wrote Dmart (AVENUE SUPERMARTS) -> try this symbol
    "Trent Ltd": "TRENT",
    "Poonawalla": "POONAWALLA",  # example symbol — verify with your broker if different
    # Add more if needed
}

# Indices: Nifty 50 and Sensex (we'll try symbol-based fetch first, then fallback to OHLC index id)
INDEX_SYMBOLS = {
    "NIFTY 50": "NIFTY 50",   # many APIs accept an index symbol; fallback to numeric ID below
    "SENSEX": "SENSEX"
}

# If Dhan requires numeric index IDs (like you used NIFTY security id = 13), place them here.
# Keep blank or update to accurate IDs if you know them.
EXTRA_INDEX_IDS = {
    "NIFTY 50": 13,   # you used 13 earlier
    # "SENSEX": <put_sensex_id_if_known>
}

# -------------------------
# Bot class
# -------------------------
class MultiLTPBot:
    def __init__(self):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.running = True
        self.headers = {
            'access-token': DHAN_ACCESS_TOKEN,
            'client-id': DHAN_CLIENT_ID,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        logger.info("Bot initialized successfully with %d instruments", len(INSTRUMENTS))
    
    def _build_payload_for_symbols(self):
        # Build symbol list for Dhan LTP endpoint
        symbols = []
        # Add equities
        for display, sym in INSTRUMENTS.items():
            symbols.append(sym)
        # Add index symbols (attempt)
        for name, sym in INDEX_SYMBOLS.items():
            symbols.append(sym)
        return {"S": symbols}
    
    def fetch_symbol_ltps(self):
        """
        Try symbol-based LTP fetch from DHAN_LTP_URL.
        Returns a dict: { symbol_or_display: { 'last_price':..., 'prev_close':..., 'ohlc':{...}, ... } }
        Best-effort parsing — DHAN responses can vary so we attempt several common shapes.
        """
        payload = self._build_payload_for_symbols()
        try:
            resp = requests.post(DHAN_LTP_URL, json=payload, headers=self.headers, timeout=10)
            logger.info("LTP API status: %s", resp.status_code)
            logger.debug("LTP raw response: %s", resp.text)
            if resp.status_code != 200:
                logger.warning("Non-200 from LTP: %s", resp.status_code)
                return {}
            j = resp.json()
            
            result = {}
            # Many DHAN-like APIs return {"status":"success","data":{"S": {"RELIANCE": {...}, "INFY": {...}}}}
            data = j.get("data") or {}
            
            # Try first: data.get("S")
            s_block = data.get("S") if isinstance(data, dict) else None
            if s_block and isinstance(s_block, dict):
                for sym, info in s_block.items():
                    result[sym] = info
                return result
            
            # Second try: maybe top-level keys are symbols
            # e.g. data may be {"RELIANCE": {...}, "INFY": {...}}
            if isinstance(data, dict):
                # filter keys that match requested symbols
                requested = set(self._build_payload_for_symbols().get("S", []))
                for k, v in data.items():
                    if k in requested:
                        result[k] = v
                if result:
                    return result
            
            # Third try: some apis place payload under j directly
            requested = set(self._build_payload_for_symbols().get("S", []))
            for k, v in j.items():
                if k in requested and isinstance(v, dict):
                    result[k] = v
            if result:
                return result
            
            # If nothing found, return empty dict (caller can fallback to index OHLC)
            logger.warning("Symbol LTP parsing did not find expected structure.")
            return {}
        
        except requests.exceptions.Timeout:
            logger.error("LTP request timeout")
            return {}
        except requests.exceptions.RequestException as e:
            logger.error(f"LTP request error: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error parsing LTP: {e}")
            return {}
    
    def fetch_index_ohlc(self, index_id):
        """
        Fetch OHLC for an index using DHAN_OHLC_URL and numeric index id (like earlier).
        Returns parsed dict or None.
        """
        payload = {
            "IDX_I": [index_id]
        }
        try:
            resp = requests.post(DHAN_OHLC_URL, json=payload, headers=self.headers, timeout=10)
            logger.info("Index OHLC status: %s (id=%s)", resp.status_code, index_id)
            logger.debug("Index OHLC raw: %s", resp.text)
            if resp.status_code != 200:
                return None
            j = resp.json()
            if j.get("status") == "success" and "data" in j:
                idx_block = j["data"].get("IDX_I", {})
                item = idx_block.get(str(index_id)) or idx_block.get(index_id)
                return item
            return None
        except Exception as e:
            logger.error(f"Error fetching index ohlc: {e}")
            return None
    
    def build_report_from_data(self, symbol_ltps, index_ohlcs):
        """
        Build summary lines for each instrument.
        symbol_ltps: dict from symbol -> info (best-effort fields)
        index_ohlcs: dict name->ohlc (from extra index OHLC fetch)
        """
        lines = []
        timestamp = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        header = f"📊 *MULTI INSTRUMENT LTP*\n🕐 {timestamp}\n\n"
        lines.append(header)
        
        # Helper to extract values robustly for a given info dict
        def parse_info(info):
            # Typical fields we try: last_price, ltp, lt, last, prev_close, close, ohlc dict
            ltp = None
            prev_close = None
            ohlc = {}
            if not info:
                return {}
            # common keys
            for k in ("last_price", "ltp", "last", "lastTradedPrice"):
                if k in info and isinstance(info[k], (int, float)):
                    ltp = info[k]
                    break
            # numeric in string form?
            if ltp is None:
                for k in ("last_price", "ltp", "last", "lastTradedPrice"):
                    if k in info:
                        try:
                            ltp = float(info[k])
                            break
                        except:
                            pass
            # prev close
            for k in ("prev_close", "close", "prevClose", "yClose"):
                if k in info and isinstance(info[k], (int, float)):
                    prev_close = info[k]
                    break
            if prev_close is None:
                for k in ("prev_close", "close", "prevClose", "yClose"):
                    if k in info:
                        try:
                            prev_close = float(info[k])
                            break
                        except:
                            pass
            # ohlc
            for k in ("ohlc", "OHLC"):
                if k in info and isinstance(info[k], dict):
                    ohlc = info[k]
                    break
            return {
                "ltp": ltp,
                "prev_close": prev_close,
                "ohlc": ohlc
            }
        
        # Add equities / symbols
        for display_name, sym in INSTRUMENTS.items():
            info = symbol_ltps.get(sym) or symbol_ltps.get(display_name) or symbol_ltps.get(sym.upper())
            parsed = parse_info(info)
            ltp = parsed.get("ltp")
            prev = parsed.get("prev_close")
            ohlc = parsed.get("ohlc") or {}
            change = None
            change_pct = None
            if ltp is not None and prev:
                try:
                    change = ltp - prev
                    change_pct = (change / prev) * 100 if prev != 0 else None
                except Exception:
                    change = None
            # Build line
            if ltp is not None:
                sign = "+" if (change is not None and change >= 0) else ""
                emoji = "🟢" if (change is not None and change >= 0) else ("🔴" if change is not None else "")
                line = f"*{display_name}* ({sym})\n• LTP: ₹{ltp:,.2f}"
                if change is not None:
                    line += f"  {emoji} {sign}{change:,.2f} ({sign}{change_pct:.2f}%)"
                # include prev close and intraday if available
                prev_close_val = prev or ohlc.get("close") or ohlc.get("prev_close")
                if prev_close_val:
                    line += f"\n• Prev Close: ₹{prev_close_val:,.2f}"
                o_open = ohlc.get("open")
                o_high = ohlc.get("high")
                o_low = ohlc.get("low")
                if o_open:
                    line += f"\n• Open: ₹{o_open:,.2f}"
                if o_high:
                    line += f"\n• High: ₹{o_high:,.2f}"
                if o_low:
                    line += f"\n• Low: ₹{o_low:,.2f}"
            else:
                line = f"*{display_name}* ({sym})\n• data unavailable"
            lines.append(line + "\n")
        
        # Add indices (try parsed symbol LTP first, then fallback to index OHLC if provided)
        for idx_name, idx_sym in INDEX_SYMBOLS.items():
            info = symbol_ltps.get(idx_sym) or symbol_ltps.get(idx_name)
            if info:
                # parse similar to equities
                parsed = {}
                # try last_price or ltp
                ltp = None
                prev = None
                if isinstance(info, dict):
                    for k in ("last_price", "ltp", "last"):
                        if k in info:
                            try:
                                ltp = float(info[k])
                                break
                            except:
                                pass
                    for k in ("prev_close", "close", "prevClose"):
                        if k in info:
                            try:
                                prev = float(info[k])
                                break
                            except:
                                pass
                change = (ltp - prev) if (ltp is not None and prev) else None
                change_pct = (change / prev * 100) if (change is not None and prev) else None
                sign = "+" if (change is not None and change >= 0) else ""
                emoji = "🟢" if (change is not None and change >= 0) else ("🔴" if change is not None else "")
                if ltp is not None:
                    line = f"*{idx_name}*\n• LTP: {ltp:,.2f}"
                    if change is not None:
                        line += f"  {emoji} {sign}{change:,.2f} ({sign}{change_pct:.2f}%)"
                else:
                    line = f"*{idx_name}*\n• data unavailable"
                lines.append(line + "\n")
            else:
                # fallback to index_ohlcs dict passed in
                ohlc_item = index_ohlcs.get(idx_name)
                if ohlc_item:
                    ltp = ohlc_item.get("last_price") or ohlc_item.get("last") or ohlc_item.get("ltp")
                    prev = ohlc_item.get("close") or ohlc_item.get("prev_close")
                    try:
                        ltp_val = float(ltp) if ltp is not None else None
                    except:
                        ltp_val = None
                    try:
                        prev_val = float(prev) if prev is not None else None
                    except:
                        prev_val = None
                    change = (ltp_val - prev_val) if (ltp_val is not None and prev_val is not None) else None
                    change_pct = (change / prev_val * 100) if (change is not None and prev_val) else None
                    sign = "+" if (change is not None and change >= 0) else ""
                    emoji = "🟢" if (change is not None and change >= 0) else ("🔴" if change is not None else "")
                    if ltp_val is not None:
                        line = f"*{idx_name}*\n• LTP: {ltp_val:,.2f}"
                        if change is not None:
                            line += f"  {emoji} {sign}{change:,.2f} ({sign}{change_pct:.2f}%)"
                    else:
                        line = f"*{idx_name}*\n• data unavailable"
                    lines.append(line + "\n")
                else:
                    lines.append(f"*{idx_name}*\n• data unavailable\n")
        
        footer = "_Updated every minute_ ⏱️"
        lines.append(footer)
        return "\n".join(lines)
    
    async def send_message(self, message):
        try:
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=message,
                parse_mode='Markdown'
            )
            logger.info("Summary message sent")
        except Exception as e:
            logger.error(f"Error sending message: {e}")
    
    async def send_startup_message(self):
        try:
            msg = "🤖 *Nifty & Stocks LTP Bot Started!*\n\n"
            msg += "तुम्हाला आता दर मिनिटाला निवडक स्टॉक्स आणि इंडेक्सचे Live LTP मिळतील! 📈\n\n"
            msg += "Instruments:\n"
            for dn, s in INSTRUMENTS.items():
                msg += f"• {dn} ({s})\n"
            msg += "\nIndices:\n"
            for n in INDEX_SYMBOLS.keys():
                msg += f"• {n}\n"
            msg += "\n✅ Powered by Dhan API v2 (REST)\n"
            msg += "_Market Hours: 9:15 AM - 3:30 PM (Mon-Fri)_"
            
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=msg,
                parse_mode='Markdown'
            )
            logger.info("Startup message sent")
        except Exception as e:
            logger.error(f"Error sending startup message: {e}")
    
    async def run(self):
        logger.info("🚀 Multi-instrument LTP Bot started")
        await self.send_startup_message()
        while self.running:
            try:
                # 1) Try symbol-based LTP
                symbol_ltps = self.fetch_symbol_ltps()  # dict keyed by symbol
                index_ohlcs = {}
                
                # 2) For indices where we have numeric IDs, fetch OHLC as fallback
                for idx_name, idx_id in EXTRA_INDEX_IDS.items():
                    if idx_id:
                        item = self.fetch_index_ohlc(idx_id)
                        if item:
                            index_ohlcs[idx_name] = item
                
                # 3) Build and send report
                message = self.build_report_from_data(symbol_ltps, index_ohlcs)
                await self.send_message(message)
                
                await asyncio.sleep(60)  # wait 1 minute
            except KeyboardInterrupt:
                logger.info("Stopped by user")
                self.running = False
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                # keep running after a short pause
                await asyncio.sleep(60)


# -------------------------
# Run bot
# -------------------------
if __name__ == "__main__":
    try:
        if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN]):
            logger.error("❌ Missing environment variables!")
            logger.error("Please set: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN")
            exit(1)
        
        bot = MultiLTPBot()
        asyncio.run(bot.run())
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        exit(1)
