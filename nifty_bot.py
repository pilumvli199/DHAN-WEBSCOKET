#!/usr/bin/env python3
# multi_ltp_heuristic.py - improved heuristic parsing for Dhan LTP raw responses
import asyncio
import os
from telegram import Bot
import requests
from datetime import datetime
import logging
import json
import math
import time

# Logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# ENV
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

DHAN_API_BASE = "https://api.dhan.co"
DHAN_LTP_URL = f"{DHAN_API_BASE}/v2/marketfeed/ltp"
DHAN_OHLC_URL = f"{DHAN_API_BASE}/v2/marketfeed/ohlc"

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
    "DR Reddy's": "DRREDDY",
    "M&M": "M&M",
    "Wipro": "WIPRO",
    "Dmart": "AVENUE_SUPERMARTS",
    "Trent Ltd": "TRENT",
    "Poonawalla": "POONAWALLA",
}

INDEX_SYMBOLS = {"NIFTY 50": "NIFTY 50", "SENSEX": "SENSEX"}
EXTRA_INDEX_IDS = {"NIFTY 50": 13}

# Utilities (same as before)
def escape_markdown_v2(text: str) -> str:
    if not text:
        return text
    to_escape = r'_*[]()~`>#+-=|{}.!'
    escaped = []
    for ch in str(text):
        if ch in to_escape:
            escaped.append('\\' + ch)
        else:
            escaped.append(ch)
    return ''.join(escaped)

def chunk_message(text: str, limit: int = 4000):
    if len(text) <= limit:
        return [text]
    lines = text.splitlines(keepends=True)
    chunks = []
    current = ""
    for ln in lines:
        if len(current) + len(ln) <= limit:
            current += ln
        else:
            if current:
                chunks.append(current)
            if len(ln) > limit:
                for i in range(0, len(ln), limit):
                    chunks.append(ln[i:i+limit])
                current = ""
            else:
                current = ln
    if current:
        chunks.append(current)
    return chunks

# NEW: recursive search helpers for raw JSON
def recursively_collect_pairs(obj, path=()):
    """
    Yield (path_tuple, key, value) for all leaf values inside obj (dict/list/primitive).
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from recursively_collect_pairs(v, path + (str(k),))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from recursively_collect_pairs(v, path + (f"[{i}]",))
    else:
        # primitive leaf
        yield (path, None, obj)

def find_numeric_candidates_for_symbol(raw_json, symbol_variants):
    """
    Search raw_json for numeric leaves whose path or nearby keys include any of the symbol_variants.
    Returns dict of candidate_name -> numeric_value
    """
    candidates = {}
    # We'll scan dict nodes and look at keys/values
    def scan(node, parent_keys):
        if isinstance(node, dict):
            # Check if any key matches symbol variants
            for k, v in node.items():
                k_norm = str(k).upper()
                for sym in symbol_variants:
                    if sym.upper() == k_norm or sym.upper() in k_norm or k_norm in sym.upper():
                        # try extract numeric from v if primitive or find numeric leaves inside v
                        if isinstance(v, (int, float)):
                            candidates[sym] = float(v)
                        elif isinstance(v, str):
                            try:
                                candidates[sym] = float(v.replace(",", ""))
                            except:
                                pass
                        elif isinstance(v, dict) or isinstance(v, list):
                            # search inside for numeric-looking keys like last_price, ltp, last, close
                            for path, _, leaf in recursively_collect_pairs(v):
                                if isinstance(leaf, (int, float)):
                                    candidates.setdefault(sym, float(leaf))
                                elif isinstance(leaf, str):
                                    try:
                                        candidates.setdefault(sym, float(leaf.replace(",", "")))
                                    except:
                                        pass
                    # Recurse
                # Recurse normally
                scan(v, parent_keys + (k,))
        elif isinstance(node, list):
            for item in node:
                scan(item, parent_keys + ("[]",))
        else:
            return
    try:
        scan(raw_json, ())
    except Exception as e:
        logger.debug("Error during raw scan: %s", e)
    return candidates

# Bot class with improved fetch
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
        symbols = []
        for display, sym in INSTRUMENTS.items():
            symbols.append(sym)
        for name, sym in INDEX_SYMBOLS.items():
            symbols.append(sym)
        return {"S": symbols}

    def fetch_symbol_ltps(self):
        """
        Try standard symbol fetch. If response structure is unexpected, attempt heuristic extraction.
        Returns dict keyed by symbol -> info dict OR special markers: {"__raw__": raw_json} or {"__429__": retry_after}
        """
        payload = self._build_payload_for_symbols()
        try:
            resp = requests.post(DHAN_LTP_URL, json=payload, headers=self.headers, timeout=10)
            logger.info("LTP API status: %s", resp.status_code)
            text = resp.text
            logger.debug("LTP raw (truncated): %s", text[:1500])
            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                logger.warning("Dhan LTP 429. Retry-After: %s", ra)
                return {"__429__": ra}
            if resp.status_code != 200:
                logger.warning("Non-200 from LTP: %s", resp.status_code)
                return {}
            j = resp.json()
            result = {}

            # Common shape check
            data = j.get("data") or j
            s_block = data.get("S") if isinstance(data, dict) else None
            if s_block and isinstance(s_block, dict):
                for sym, info in s_block.items():
                    result[sym] = info
                return result

            # Try other expected shapes quickly
            requested = set(self._build_payload_for_symbols().get("S", []))
            if isinstance(data, dict):
                for k, v in data.items():
                    if k in requested:
                        result[k] = v
            if result:
                return result

            # If still nothing - fallback to heuristic on the full JSON:
            logger.info("Falling back to heuristic raw parsing of response JSON")
            # keep raw j for inspection by caller
            raw_json = j

            # For each instrument symbol try to extract candidate numeric value(s)
            heuristic_map = {}
            for display_name, sym in INSTRUMENTS.items():
                # build symbol variants to search for (lots of possible forms)
                variants = set()
                variants.add(sym)
                variants.add(display_name)
                # also try uppercase/no-spaces versions
                variants.add(sym.replace(" ", "").upper())
                variants.add(display_name.replace(" ", "").upper())
                # try with NSE suffixes commonly used
                variants.add(sym + ".NS")
                variants.add(sym + "NSE")
                # attempt to find any numeric leaf that looks like price
                candidates = find_numeric_candidates_for_symbol(raw_json, variants)
                if candidates:
                    # choose the largest numeric candidate (likely LTP) OR the last one found
                    # heuristic: prefer keys named like last/ltp/last_price if possible — but we used generic search, so pick max
                    try:
                        chosen = max(candidates.values())
                    except:
                        chosen = list(candidates.values())[0]
                    heuristic_map[sym] = {"last_price": chosen}
            if heuristic_map:
                logger.info("Heuristic extraction found %d instruments", len(heuristic_map))
                return heuristic_map

            # if no candidates, return raw for inspection
            logger.warning("Heuristic parsing found no numeric candidates; returning raw JSON for debugging")
            return {"__raw__": j}

        except requests.exceptions.Timeout:
            logger.error("LTP request timeout")
            return {}
        except requests.exceptions.RequestException as e:
            logger.error(f"LTP request error: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error in fetch_symbol_ltps: {e}")
            return {}

    def fetch_index_ohlc(self, index_id):
        payload = {"IDX_I": [index_id]}
        try:
            resp = requests.post(DHAN_OHLC_URL, json=payload, headers=self.headers, timeout=10)
            logger.info("Index OHLC status: %s (id=%s)", resp.status_code, index_id)
            logger.debug("Index OHLC raw: %s", resp.text[:1500])
            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                logger.warning("Index OHLC 429. Retry-After: %s", ra)
                return {"__429__": ra}
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

    # build_report_from_data, send_text_safe, send_startup_message, run remain identical to previous
    # For brevity I reuse simpler versions here (keeps MarkdownV2 escaping and chunking)

    def build_report_from_data(self, symbol_ltps, index_ohlcs):
        lines = []
        timestamp = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        header = f"📊 *MULTI INSTRUMENT LTP*\n🕐 {timestamp}\n\n"
        lines.append(header)

        def parse_info(info):
            ltp = None
            prev_close = None
            ohlc = {}
            if not info or not isinstance(info, dict):
                return {}
            for k in ("last_price", "ltp", "last", "lastTradedPrice"):
                if k in info:
                    try:
                        ltp = float(info[k]); break
                    except:
                        pass
            for k in ("prev_close", "close", "prevClose", "yClose"):
                if k in info:
                    try:
                        prev_close = float(info[k]); break
                    except:
                        pass
            for k in ("ohlc", "OHLC"):
                if k in info and isinstance(info[k], dict):
                    ohlc = info[k]; break
            return {"ltp": ltp, "prev_close": prev_close, "ohlc": ohlc}

        for display_name, sym in INSTRUMENTS.items():
            info = symbol_ltps.get(sym) or symbol_ltps.get(display_name) or symbol_ltps.get(sym.upper())
            parsed = parse_info(info)
            ltp = parsed.get("ltp")
            prev = parsed.get("prev_close")
            ohlc = parsed.get("ohlc") or {}
            change = None
            change_pct = None
            if (ltp is not None) and (prev is not None):
                try:
                    change = ltp - prev
                    change_pct = (change / prev) * 100 if prev != 0 else None
                except:
                    change = None
            if ltp is not None:
                sign = "+" if (change is not None and change >= 0) else ""
                emoji = "🟢" if (change is not None and change >= 0) else ("🔴" if change is not None else "")
                line = f"*{display_name}* ({sym})\n• LTP: ₹{ltp:,.2f}"
                if change is not None:
                    line += f"  {emoji} {sign}{change:,.2f} ({sign}{change_pct:.2f}%)"
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

        for idx_name, idx_sym in INDEX_SYMBOLS.items():
            info = symbol_ltps.get(idx_sym) or symbol_ltps.get(idx_name)
            if info and isinstance(info, dict):
                ltp = None
                prev = None
                for k in ("last_price", "ltp", "last"):
                    if k in info:
                        try:
                            ltp = float(info[k]); break
                        except: pass
                for k in ("prev_close", "close", "prevClose"):
                    if k in info:
                        try:
                            prev = float(info[k]); break
                        except: pass
                change = (ltp - prev) if (ltp is not None and prev is not None) else None
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

    async def send_text_safe(self, message_text: str):
        chunks = chunk_message(message_text, limit=3900)
        for chunk in chunks:
            escaped = escape_markdown_v2(chunk)
            try:
                await self.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=escaped, parse_mode='MarkdownV2')
                await asyncio.sleep(0.2)
            except Exception as e:
                logger.error("MarkdownV2 send failed, falling back to plain text: %s", e)
                try:
                    await self.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=chunk, parse_mode=None)
                except Exception as e2:
                    logger.error("Plain text send also failed: %s", e2)

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
            await self.send_text_safe(msg)
            logger.info("Startup message sent")
        except Exception as e:
            logger.error(f"Error sending startup message: {e}")

    async def run(self):
        logger.info("🚀 Multi-instrument LTP Bot started")
        await self.send_startup_message()
        while self.running:
            try:
                symbol_ltps = self.fetch_symbol_ltps()
                # handle rate limit marker
                if isinstance(symbol_ltps, dict) and "__429__" in symbol_ltps:
                    ra = symbol_ltps.get("__429__")
                    sleep_for = int(ra) if ra and str(ra).isdigit() else 10
                    logger.warning("Sleeping %s seconds due to DHAN LTP 429", sleep_for)
                    await asyncio.sleep(sleep_for)
                    continue

                index_ohlcs = {}
                for idx_name, idx_id in EXTRA_INDEX_IDS.items():
                    if idx_id:
                        item = self.fetch_index_ohlc(idx_id)
                        if isinstance(item, dict) and "__429__" in item:
                            ra = item.get("__429__")
                            sleep_for = int(ra) if ra and str(ra).isdigit() else 10
                            logger.warning("Sleeping %s seconds due to DHAN index 429", sleep_for)
                            await asyncio.sleep(sleep_for)
                            continue
                        if item:
                            index_ohlcs[idx_name] = item

                message = self.build_report_from_data(symbol_ltps, index_ohlcs)
                await self.send_text_safe(message)
                await asyncio.sleep(60)
            except KeyboardInterrupt:
                logger.info("Stopped by user")
                self.running = False
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(10)

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
