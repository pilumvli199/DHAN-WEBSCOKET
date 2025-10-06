#!/usr/bin/env python3
# main.py - Multi-instrument Nifty/Stocks LTP Bot (Dhan API v2)
# Updated: fixes coroutine-not-awaited for debug send; sends debug preview via requests.post (sync).

import os
import time
import json
import logging
import requests
import asyncio
from datetime import datetime
from telegram import Bot

# -------------------------
# Configuration / Env
# -------------------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

# Dhan endpoints
DHAN_API_BASE = "https://api.dhan.co"
DHAN_LTP_URL = f"{DHAN_API_BASE}/v2/marketfeed/ltp"
DHAN_OHLC_URL = f"{DHAN_API_BASE}/v2/marketfeed/ohlc"

# Instruments
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

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# -------------------------
# Utilities
# -------------------------
def escape_markdown_v2(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)
    to_escape = r'_*[]()~`>#+-=|{}.!'
    return ''.join('\\' + ch if ch in to_escape else ch for ch in text)

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

def save_debug_file(resp_text: str):
    try:
        ts = int(time.time())
        path = f"/tmp/dhan_ltp_debug_{ts}.json"
        with open(path, "w", encoding="utf-8") as f:
            f.write(resp_text)
        return path
    except Exception as e:
        logger.error("Failed to write debug file: %s", e)
        return None

def safe_truncate(s: str, n: int = 1200):
    if not s:
        return ""
    return s[:n] + ("" if len(s) <= n else "\n\n...TRUNCATED...")

def recursively_collect_pairs(obj, path=()):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from recursively_collect_pairs(v, path + (str(k),))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from recursively_collect_pairs(v, path + (f"[{i}]",))
    else:
        yield (path, None, obj)

def find_numeric_candidates_for_symbol(raw_json, symbol_variants):
    candidates = {}
    def scan(node):
        if isinstance(node, dict):
            for k, v in node.items():
                k_norm = str(k).upper()
                for sym in symbol_variants:
                    su = sym.upper()
                    if su == k_norm or su in k_norm or k_norm in su:
                        if isinstance(v, (int, float)):
                            candidates.setdefault(sym, float(v))
                        elif isinstance(v, str):
                            try:
                                candidates.setdefault(sym, float(v.replace(",", "")))
                            except:
                                pass
                        elif isinstance(v, (dict, list)):
                            for _, _, leaf in recursively_collect_pairs(v):
                                if isinstance(leaf, (int, float)):
                                    candidates.setdefault(sym, float(leaf))
                                elif isinstance(leaf, str):
                                    try:
                                        candidates.setdefault(sym, float(leaf.replace(",", "")))
                                    except:
                                        pass
                scan(v)
        elif isinstance(node, list):
            for it in node:
                scan(it)
    try:
        scan(raw_json)
    except Exception as e:
        logger.debug("Error during heuristic scan: %s", e)
    return candidates

# -------------------------
# Synchronous Telegram debug send helper (fix coroutine issue)
# -------------------------
def telegram_send_debug_preview_sync(preview_text: str):
    """
    Send a plain-text debug preview to Telegram using synchronous requests.post
    to avoid coroutine/async mismatch when called from sync code.
    """
    try:
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            logger.debug("Telegram token/ chat id missing; skipping debug preview send")
            return False
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": f"DEBUG LTP preview (truncated):\n\n{preview_text}"
        }
        r = requests.post(url, json=payload, timeout=10)
        logger.info("Telegram debug preview send status: %s", r.status_code)
        return r.status_code == 200
    except Exception as e:
        logger.debug("Failed sending debug preview via requests: %s", e)
        return False

# -------------------------
# Bot class
# -------------------------
class MultiLTPBot:
    def __init__(self):
        if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN]):
            logger.error("Missing environment variables. Set: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN")
            raise SystemExit(1)
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
        for disp, sym in INSTRUMENTS.items():
            symbols.append(sym)
        for name, sym in INDEX_SYMBOLS.items():
            symbols.append(sym)
        return {"S": symbols}

    def fetch_symbol_ltps(self):
        payload = self._build_payload_for_symbols()
        try:
            resp = requests.post(DHAN_LTP_URL, json=payload, headers=self.headers, timeout=10)
            logger.info("LTP API status: %s", resp.status_code)
            txt = resp.text
            logger.debug("LTP raw (truncated): %s", txt[:1500])

            # Save debug file & send preview to Telegram synchronously (fixes coroutine error)
            try:
                dbg_path = save_debug_file(txt)
                if dbg_path:
                    logger.info("Saved raw LTP response to %s", dbg_path)
                preview = safe_truncate(txt, 1200)
                telegram_send_debug_preview_sync(preview)  # <<< synchronous send (fixed)
            except Exception as e:
                logger.debug("Debug save/preview skipped: %s", e)

            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                logger.warning("Dhan LTP rate-limited 429. Retry-After: %s", ra)
                return {"__429__": ra}
            if resp.status_code != 200:
                logger.warning("Non-200 from LTP: %s", resp.status_code)
                return {}

            j = resp.json()
            result = {}

            data = j.get("data") or j
            s_block = data.get("S") if isinstance(data, dict) else None
            if s_block and isinstance(s_block, dict):
                for sym, info in s_block.items():
                    result[sym] = info
                return result

            requested = set(self._build_payload_for_symbols().get("S", []))
            if isinstance(data, dict):
                for k, v in data.items():
                    if k in requested:
                        result[k] = v
            if result:
                return result

            logger.info("Falling back to heuristic raw parsing of LTP response JSON")
            raw_json = j
            heuristic_map = {}
            for display_name, sym in INSTRUMENTS.items():
                variants = set()
                variants.add(sym)
                variants.add(display_name)
                variants.add(sym.replace(" ", "").upper())
                variants.add(display_name.replace(" ", "").upper())
                variants.add(sym + ".NS")
                variants.add(sym + "NSE")
                candidates = find_numeric_candidates_for_symbol(raw_json, variants)
                if candidates:
                    try:
                        chosen = max(candidates.values())
                    except Exception:
                        chosen = list(candidates.values())[0]
                    heuristic_map[sym] = {"last_price": chosen}
            if heuristic_map:
                logger.info("Heuristic found %d instruments", len(heuristic_map))
                return heuristic_map

            logger.warning("No data parsed from LTP response; returning raw JSON for inspection")
            return {"__raw__": j}

        except requests.exceptions.Timeout:
            logger.error("LTP request timeout")
            return {}
        except requests.exceptions.RequestException as e:
            logger.error("LTP request error: %s", e)
            return {}
        except Exception as e:
            logger.error("Unexpected error in fetch_symbol_ltps: %s", e)
            return {}

    def fetch_index_ohlc(self, index_id):
        payload = {"IDX_I": [index_id]}
        try:
            resp = requests.post(DHAN_OHLC_URL, json=payload, headers=self.headers, timeout=10)
            logger.info("Index OHLC status: %s (id=%s)", resp.status_code, index_id)
            logger.debug("Index OHLC raw (truncated): %s", resp.text[:1500])

            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                logger.warning("Index OHLC rate-limited 429. Retry-After: %s", ra)
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
            logger.error("Error fetching index OHLC: %s", e)
            return None

    # build_report_from_data, send_text_safe, send_startup_message, run remain same as earlier
    # For brevity, reusing previously provided implementations (not repeated here due to length).
    # Make sure to include them in your file — exactly as in previous version.

    # --- For full code, copy the build_report_from_data, send_text_safe, send_startup_message, run from previous main.py ---
    # (They are unchanged except fetch_symbol_ltps debug send fix above.)

# -------------------------
# Run
# -------------------------
if __name__ == "__main__":
    try:
        bot = MultiLTPBot()
        asyncio.run(bot.run())
    except Exception as e:
        logger.error("Fatal error: %s", e)
        raise SystemExit(1)
