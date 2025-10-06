import asyncio
import os
from telegram import Bot
from datetime import datetime, timedelta
import logging
from dhanhq import dhanhq
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import io
import time

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

# Complete Stock List with Security IDs
STOCK_MAPPING = {
    "RELIANCE": "1333",
    "HDFCBANK": "1333",
    "ICICIBANK": "4963",
    "BAJFINANCE": "16675",
    "INFY": "1594",
    "TATAMOTORS": "3456",
    "AXISBANK": "5900",
    "SBIN": "3045",
    "LTIM": "11532",
    "ADANIENT": "25",
    "KOTAKBANK": "1922",
    "LT": "11483",
    "MARUTI": "10999",
    "TECHM": "13538",
    "LICI": "21808",
    "HINDUNILVR": "1394",
    "NTPC": "11630",
    "BHARTIARTL": "16669",
    "POWERGRID": "14977",
    "ONGC": "2475",
    "PERSISTENT": "13913",
    "DRREDDY": "3666",
    "M&M": "2031",
    "WIPRO": "3787",
    "DMART": "22",
    "TRENT": "1681",
    "POONAWALLA": "11915"
}

# Watchlist with all your stocks
WATCHLIST = {
    "IDX_I": {
        "13": "Nifty 50",
        "26": "Sensex"
    },
    "NSE_EQ": STOCK_MAPPING
}

# ========================
# MULTI-STOCK BOT
# ========================

class MultiStockDhanBot:
    def __init__(self):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.running = True
        
        # Dhan API - only takes access token
        self.dhan = dhanhq(DHAN_ACCESS_TOKEN)
        
        self.current_expiry = {}
        self.last_chart_time = {}
        
        logger.info("Multi-Stock Bot initialized with {} stocks".format(len(STOCK_MAPPING)))
    
    def get_live_data_single(self, sec_id, segment, name):
        """Get live data for single security"""
        try:
            response = self.dhan.get_ltp_data(
                exchange_segment=segment,
                security_id=str(sec_id)
            )
            
            if isinstance(response, dict) and 'data' in response:
                data = response['data']
                ltp = data.get('LTP', 0)
                prev_close = data.get('prev_close', 0)
                
                if ltp > 0:
                    change = ltp - prev_close if prev_close > 0 else 0
                    change_pct = (change / prev_close * 100) if prev_close > 0 else 0
                    
                    return {
                        'name': name,
                        'ltp': float(ltp),
                        'open': float(data.get('open', 0)),
                        'high': float(data.get('high', 0)),
                        'low': float(data.get('low', 0)),
                        'close': float(prev_close),
                        'volume': data.get('volume', 0),
                        'change': change,
                        'change_pct': change_pct
                    }
            
            return None
            
        except Exception as e:
            logger.error(f"Error fetching {name}: {e}")
            return None
    
    def get_live_data_batch(self):
        """Fetch live data for all instruments"""
        try:
            parsed_data = {'IDX_I': {}, 'NSE_EQ': {}}
            
            # Fetch Indices
            logger.info("Fetching indices data...")
            for sec_id, name in WATCHLIST['IDX_I'].items():
                data = self.get_live_data_single(sec_id, 'IDX_I', name)
                if data:
                    parsed_data['IDX_I'][sec_id] = data
                time.sleep(0.1)
            
            # Fetch Stocks
            logger.info(f"Fetching {len(WATCHLIST['NSE_EQ'])} stocks data...")
            count = 0
            for name, sec_id in WATCHLIST['NSE_EQ'].items():
                data = self.get_live_data_single(sec_id, 'NSE_EQ', name)
                if data:
                    parsed_data['NSE_EQ'][sec_id] = data
                    count += 1
                time.sleep(0.1)  # Rate limiting
            
            logger.info(f"Successfully fetched data for {count} stocks and {len(parsed_data['IDX_I'])} indices")
            return parsed_data if (parsed_data['IDX_I'] or parsed_data['NSE_EQ']) else None
            
        except Exception as e:
            logger.error(f"Error fetching batch data: {e}", exc_info=True)
            return None
    
    def get_intraday_data(self, sec_id, segment="NSE_EQ", name="Stock"):
        """Get 5-minute intraday data for last 5 days"""
        try:
            instrument_type = "INDEX" if segment == "IDX_I" else "EQUITY"
            
            # Get last 5 days of 5-min data
            to_date = datetime.now().strftime("%Y-%m-%d")
            from_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            
            response = self.dhan.intraday_minute_data(
                security_id=str(sec_id),
                exchange_segment=segment,
                instrument_type=instrument_type
            )
            
            if isinstance(response, dict) and 'data' in response:
                data = response['data']
                if 'open' in data and 'close' in data:
                    timestamps = data.get('timestamp', [])
                    opens = data.get('open', [])
                    highs = data.get('high', [])
                    lows = data.get('low', [])
                    closes = data.get('close', [])
                    volumes = data.get('volume', [])
                    
                    parsed = []
                    for i in range(len(timestamps)):
                        parsed.append({
                            'time': datetime.fromtimestamp(timestamps[i]),
                            'open': opens[i] if i < len(opens) else 0,
                            'high': highs[i] if i < len(highs) else 0,
                            'low': lows[i] if i < len(lows) else 0,
                            'close': closes[i] if i < len(closes) else 0,
                            'volume': volumes[i] if i < len(volumes) else 0
                        })
                    
                    # Filter last 5 days
                    cutoff = datetime.now() - timedelta(days=5)
                    filtered = [p for p in parsed if p['time'] >= cutoff]
                    
                    logger.info(f"Fetched {len(filtered)} candles for {name}")
                    return filtered if len(filtered) > 0 else None
            
            return None
            
        except Exception as e:
            logger.error(f"Error fetching intraday for {name}: {e}")
            return None
    
    def generate_candlestick_chart(self, candle_data, title="Stock Chart"):
        """Generate candlestick chart from 5-min data"""
        try:
            if not candle_data or len(candle_data) < 2:
                return None
            
            # Take last 100 candles for better visibility
            data = candle_data[-100:] if len(candle_data) > 100 else candle_data
            
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), 
                                           gridspec_kw={'height_ratios': [3, 1]})
            
            # Candlestick chart
            for i, candle in enumerate(data):
                o, h, l, c = candle['open'], candle['high'], candle['low'], candle['close']
                
                color = '#26a69a' if c >= o else '#ef5350'
                
                # Draw high-low line
                ax1.plot([i, i], [l, h], color=color, linewidth=1, solid_capstyle='round')
                
                # Draw body
                height = abs(c - o)
                bottom = min(o, c)
                rect = Rectangle((i - 0.3, bottom), 0.6, height, 
                                facecolor=color, edgecolor=color)
                ax1.add_patch(rect)
            
            # Format price axis
            ax1.set_xlim(-1, len(data))
            ax1.set_ylabel('Price (₹)', fontsize=10, fontweight='bold')
            ax1.set_title(title, fontsize=14, fontweight='bold', pad=20)
            ax1.grid(True, alpha=0.3, linestyle='--')
            ax1.set_xticks([])
            
            # Volume bars
            colors = ['#26a69a' if data[i]['close'] >= data[i]['open'] else '#ef5350' 
                     for i in range(len(data))]
            ax2.bar(range(len(data)), [d['volume'] for d in data], 
                   color=colors, alpha=0.5, width=0.8)
            ax2.set_ylabel('Volume', fontsize=9)
            ax2.set_xlim(-1, len(data))
            ax2.grid(True, alpha=0.3, linestyle='--')
            
            # Time labels
            step = max(1, len(data) // 10)
            indices = list(range(0, len(data), step))
            labels = [data[i]['time'].strftime('%d/%m %H:%M') for i in indices]
            ax2.set_xticks(indices)
            ax2.set_xticklabels(labels, rotation=45, ha='right', fontsize=8)
            
            plt.tight_layout()
            
            # Save to bytes
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
            buf.seek(0)
            plt.close()
            
            return buf
            
        except Exception as e:
            logger.error(f"Error generating chart: {e}", exc_info=True)
            return None
    
    def get_option_chain(self, underlying_id, segment="IDX_I"):
        """Get option chain for index"""
        try:
            # Get expiry if not cached
            if underlying_id not in self.current_expiry:
                try:
                    expiry_response = self.dhan.get_expiry_list(
                        exchange_segment=segment,
                        security_id=str(underlying_id)
                    )
                    
                    if isinstance(expiry_response, dict) and 'data' in expiry_response:
                        expiries = expiry_response['data']
                        if expiries and len(expiries) > 0:
                            self.current_expiry[underlying_id] = expiries[0]
                    elif isinstance(expiry_response, list) and expiry_response:
                        self.current_expiry[underlying_id] = expiry_response[0]
                        
                except Exception as e:
                    logger.error(f"Error getting expiry: {e}")
                    return None
            
            if underlying_id not in self.current_expiry:
                logger.error(f"No expiry found for {underlying_id}")
                return None
            
            # Get option chain
            response = self.dhan.get_option_chain(
                exchange_segment=segment,
                security_id=str(underlying_id),
                expiry_code=str(self.current_expiry[underlying_id])
            )
            
            if isinstance(response, dict) and 'data' in response:
                data = response['data']
                spot_price = data.get('spot_price', 0)
                oc_data = data.get('option_chain', {})
                
                if not oc_data:
                    return None
                
                strikes = sorted([float(s) for s in oc_data.keys()])
                if not strikes:
                    return None
                    
                atm_strike = min(strikes, key=lambda x: abs(x - spot_price))
                atm_index = strikes.index(atm_strike)
                
                # 10 strikes around ATM
                start_idx = max(0, atm_index - 5)
                end_idx = min(len(strikes), atm_index + 6)
                selected_strikes = strikes[start_idx:end_idx]
                
                option_data = []
                for strike in selected_strikes:
                    strike_key = str(int(strike))
                    strike_data = oc_data.get(strike_key, {})
                    
                    ce_data = strike_data.get('call_options', {})
                    pe_data = strike_data.get('put_options', {})
                    
                    option_data.append({
                        'strike': strike,
                        'ce_ltp': ce_data.get('ltp', 0),
                        'ce_oi': ce_data.get('oi', 0),
                        'ce_volume': ce_data.get('volume', 0),
                        'pe_ltp': pe_data.get('ltp', 0),
                        'pe_oi': pe_data.get('oi', 0),
                        'pe_volume': pe_data.get('volume', 0),
                        'is_atm': (strike == atm_strike)
                    })
                
                return {
                    'spot': spot_price,
                    'atm': atm_strike,
                    'expiry': self.current_expiry[underlying_id],
                    'options': option_data
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error option chain: {e}", exc_info=True)
            return None
    
    async def send_live_summary(self, live_data):
        """Send live market summary"""
        try:
            message = "📊 *LIVE MARKET UPDATE*\n\n"
            
            # Indices
            if 'IDX_I' in live_data and live_data['IDX_I']:
                message += "📈 *INDICES*\n```\n"
                for sec_id, data in live_data['IDX_I'].items():
                    emoji = "🟢" if data['change'] >= 0 else "🔴"
                    sign = "+" if data['change'] >= 0 else ""
                    message += f"{data['name']:<15} ₹{data['ltp']:>8,.1f} {emoji} {sign}{data['change_pct']:>6.2f}%\n"
                message += "```\n\n"
            
            # Top gainers/losers
            if 'NSE_EQ' in live_data and live_data['NSE_EQ']:
                stocks = list(live_data['NSE_EQ'].values())
                stocks.sort(key=lambda x: x['change_pct'], reverse=True)
                
                message += "🚀 *TOP 5 GAINERS*\n```\n"
                for stock in stocks[:5]:
                    message += f"{stock['name']:<15} ₹{stock['ltp']:>7,.1f} 🟢 +{stock['change_pct']:>5.2f}%\n"
                message += "```\n\n"
                
                message += "📉 *TOP 5 LOSERS*\n```\n"
                for stock in stocks[-5:]:
                    message += f"{stock['name']:<15} ₹{stock['ltp']:>7,.1f} 🔴 {stock['change_pct']:>6.2f}%\n"
                message += "```"
            
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=message,
                parse_mode='Markdown'
            )
            logger.info("✅ Live summary sent")
            
        except Exception as e:
            logger.error(f"Error sending summary: {e}")
    
    async def send_option_chain_message(self, name, option_data):
        """Send option chain"""
        try:
            message = f"📊 *OPTION CHAIN - {name}*\n"
            message += f"📅 Expiry: {option_data['expiry']}\n"
            message += f"💰 Spot: ₹{option_data['spot']:,.2f}\n"
            message += f"🎯 ATM: ₹{option_data['atm']:,.0f}\n\n"
            
            message += "```\n"
            message += "Strike    CE-LTP   CE-OI    PE-LTP   PE-OI\n"
            message += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            
            for opt in option_data['options']:
                atm = "🔸" if opt['is_atm'] else "  "
                
                ce_ltp = f"{opt['ce_ltp']:7.1f}" if opt['ce_ltp'] > 0 else "   -   "
                ce_oi = f"{opt['ce_oi']/1000:6.0f}K" if opt['ce_oi'] > 0 else "   -  "
                
                pe_ltp = f"{opt['pe_ltp']:7.1f}" if opt['pe_ltp'] > 0 else "   -   "
                pe_oi = f"{opt['pe_oi']/1000:6.0f}K" if opt['pe_oi'] > 0 else "   -  "
                
                message += f"{atm}{opt['strike']:6.0f}  {ce_ltp}  {ce_oi}  {pe_ltp}  {pe_oi}\n"
            
            message += "```"
            
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=message,
                parse_mode='Markdown'
            )
            logger.info(f"✅ Option chain sent for {name}")
            
        except Exception as e:
            logger.error(f"Error sending OC: {e}")
    
    async def send_chart(self, name, sec_id, segment):
        """Send 5-min candlestick chart"""
        try:
            # Check if we sent chart recently (avoid spam)
            key = f"{segment}:{sec_id}"
            now = time.time()
            if key in self.last_chart_time:
                if now - self.last_chart_time[key] < 1500:  # 25 min cooldown
                    return
            
            candle_data = self.get_intraday_data(sec_id, segment, name)
            
            if candle_data and len(candle_data) > 0:
                chart = self.generate_candlestick_chart(
                    candle_data, 
                    f"{name} - 5 Minute Chart (Last 5 Days)"
                )
                
                if chart:
                    await self.bot.send_photo(
                        chat_id=TELEGRAM_CHAT_ID,
                        photo=chart,
                        caption=f"📈 *{name}* - 5 Min Candlestick Chart\n🕐 Last {len(candle_data)} candles",
                        parse_mode='Markdown'
                    )
                    self.last_chart_time[key] = now
                    logger.info(f"✅ Chart sent for {name}")
            
        except Exception as e:
            logger.error(f"Error sending chart for {name}: {e}")
    
    async def run(self):
        """Main loop"""
        logger.info("🚀 Multi-Stock Bot started!")
        
        await self.send_startup_message()
        
        iteration = 0
        
        while self.running:
            try:
                current_time = datetime.now()
                
                # Live data every 1 minute
                logger.info(f"Iteration {iteration}: Fetching live data...")
                live_data = self.get_live_data_batch()
                if live_data:
                    await self.send_live_summary(live_data)
                
                # Option chains every 5 minutes
                if iteration % 5 == 0:
                    logger.info("Fetching option chains...")
                    for idx_id, name in WATCHLIST['IDX_I'].items():
                        oc = self.get_option_chain(idx_id)
                        if oc:
                            await self.send_option_chain_message(name, oc)
                            await asyncio.sleep(2)
                
                # Charts every 30 minutes (5 stocks at a time)
                if iteration % 30 == 0 and iteration > 0:
                    logger.info("Sending charts...")
                    
                    # Send index charts
                    for idx_id, name in WATCHLIST['IDX_I'].items():
                        await self.send_chart(name, idx_id, "IDX_I")
                        await asyncio.sleep(2)
                    
                    # Send 5 random stock charts
                    import random
                    stock_items = list(WATCHLIST['NSE_EQ'].items())
                    random.shuffle(stock_items)
                    
                    for name, sec_id in stock_items[:5]:
                        await self.send_chart(name, sec_id, "NSE_EQ")
                        await asyncio.sleep(2)
                
                iteration += 1
                await asyncio.sleep(60)  # 1 minute interval
                
            except KeyboardInterrupt:
                logger.info("Bot stopped by user")
                self.running = False
                break
            except Exception as e:
                logger.error(f"Main loop error: {e}", exc_info=True)
                await asyncio.sleep(60)
    
    async def send_startup_message(self):
        """Startup message"""
        try:
            msg = f"🤖 *MULTI-STOCK DHAN BOT STARTED*\n\n"
            msg += f"📊 Tracking: {len(STOCK_MAPPING)} stocks + 2 indices\n"
            msg += f"✅ Live updates: Every 1 min\n"
            msg += f"✅ Option chains: Every 5 min\n"
            msg += f"✅ Charts (5-min TF): Every 30 min\n\n"
            msg += f"🎯 *Stocks Tracked:*\n"
            
            stock_names = list(STOCK_MAPPING.keys())
            for i in range(0, len(stock_names), 3):
                batch = stock_names[i:i+3]
                msg += f"• {' | '.join(batch)}\n"
            
            msg += f"\n🚀 *Ready to track markets!*"
            
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=msg,
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Startup message error: {e}")


if __name__ == "__main__":
    try:
        if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DHAN_ACCESS_TOKEN]):
            logger.error("❌ Missing environment variables!")
            exit(1)
        
        logger.info("Initializing bot...")
        bot = MultiStockDhanBot()
        asyncio.run(bot.run())
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        exit(1)
