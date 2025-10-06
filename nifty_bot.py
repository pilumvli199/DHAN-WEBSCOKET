import asyncio
import os
from telegram import Bot
from datetime import datetime, timedelta
import logging
from dhanhq import dhanhq
import matplotlib
matplotlib.use('Agg')  # Non-GUI backend
import matplotlib.pyplot as plt
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

# Watchlist: 50 Stocks + 2 Indices
WATCHLIST = {
    "IDX_I": ["13", "26"],  # Nifty 50, Bank Nifty
    "NSE_EQ": [
        "1333", "11915", "14366", "236", "13"  # HDFC, TCS, Reliance, ITC, Infosys
        # Add 45 more stock IDs here
    ]
}

# ========================
# MULTI-STOCK BOT
# ========================

class MultiStockDhanBot:
    def __init__(self):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.running = True
        
        # Dhan API
        self.dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
        
        self.current_expiry = {}
        self.last_update_time = 0
        
        logger.info("Multi-Stock Bot initialized")
    
    def get_live_data_batch(self):
        """Batch fetch live data for all instruments"""
        try:
            # Convert string IDs to integers for API
            securities = {}
            for segment, ids in WATCHLIST.items():
                securities[segment] = [int(sid) for sid in ids]
            
            logger.info(f"Fetching data for {sum(len(v) for v in securities.values())} instruments")
            
            response = self.dhan.intraday_minute_data(
                security_id=str(securities['IDX_I'][0]),
                exchange_segment='IDX_I',
                instrument_type='INDEX'
            )
            
            logger.info(f"API Response type: {type(response)}")
            logger.info(f"API Response: {str(response)[:500]}")
            
            # Try individual fetches instead
            parsed_data = {}
            
            for segment, ids in WATCHLIST.items():
                parsed_data[segment] = {}
                
                for sec_id in ids:
                    try:
                        # Determine instrument type
                        instrument_type = "INDEX" if segment == "IDX_I" else "EQUITY"
                        
                        # Get LTP
                        ltp_response = self.dhan.get_ltp_data(
                            exchange_segment=segment,
                            security_id=str(sec_id)
                        )
                        
                        if isinstance(ltp_response, dict):
                            data = ltp_response.get('data', {})
                            if isinstance(data, dict):
                                ltp = data.get('LTP', 0)
                                prev_close = data.get('prev_close', 0)
                                
                                if ltp > 0:
                                    change = ltp - prev_close if prev_close > 0 else 0
                                    change_pct = (change / prev_close * 100) if prev_close > 0 else 0
                                    
                                    parsed_data[segment][sec_id] = {
                                        'name': data.get('tradingSymbol', sec_id),
                                        'ltp': float(ltp),
                                        'open': float(data.get('open', 0)),
                                        'high': float(data.get('high', 0)),
                                        'low': float(data.get('low', 0)),
                                        'close': float(prev_close),
                                        'volume': data.get('volume', 0),
                                        'change': change,
                                        'change_pct': change_pct
                                    }
                        
                        # Rate limit
                        time.sleep(0.1)
                        
                    except Exception as e:
                        logger.error(f"Error fetching {segment}:{sec_id}: {e}")
                        continue
            
            logger.info(f"Successfully fetched data for {sum(len(v) for v in parsed_data.values())} instruments")
            return parsed_data if parsed_data else None
            
        except Exception as e:
            logger.error(f"Error fetching batch data: {e}", exc_info=True)
            return None
    
    def get_historical_batch(self, security_ids, segment="NSE_EQ", days=5):
        """Batch fetch historical data with rate limiting"""
        historical_data = {}
        
        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=days+2)).strftime("%Y-%m-%d")
        
        for sec_id in security_ids:
            try:
                instrument_type = "INDEX" if segment == "IDX_I" else "EQUITY"
                
                response = self.dhan.historical_daily_data(
                    security_id=str(sec_id),
                    exchange_segment=segment,
                    instrument_type=instrument_type,
                    expiry_code=0,
                    from_date=from_date,
                    to_date=to_date
                )
                
                logger.info(f"Historical response for {sec_id}: {type(response)}")
                
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
                                'date': datetime.fromtimestamp(timestamps[i]),
                                'open': opens[i] if i < len(opens) else 0,
                                'high': highs[i] if i < len(highs) else 0,
                                'low': lows[i] if i < len(lows) else 0,
                                'close': closes[i] if i < len(closes) else 0,
                                'volume': volumes[i] if i < len(volumes) else 0
                            })
                        
                        historical_data[sec_id] = parsed[-5:]  # Last 5 days
                        logger.info(f"Historical data fetched for {sec_id}: {len(parsed)} days")
                
                # Rate limiting: 3 req/sec
                time.sleep(0.35)
                
            except Exception as e:
                logger.error(f"Error fetching historical for {sec_id}: {e}")
        
        return historical_data
    
    def generate_chart(self, historical_data, title="Stock Chart"):
        """Generate PNG chart from historical data"""
        try:
            if not historical_data:
                return None
            
            dates = [d['date'] for d in historical_data]
            closes = [d['close'] for d in historical_data]
            
            # Create figure
            fig, ax = plt.subplots(figsize=(10, 6))
            
            # Plot line chart
            ax.plot(dates, closes, marker='o', linewidth=2, markersize=6, color='#2196F3')
            ax.fill_between(dates, closes, alpha=0.3, color='#2196F3')
            
            # Formatting
            ax.set_title(title, fontsize=14, fontweight='bold')
            ax.set_xlabel('Date', fontsize=10)
            ax.set_ylabel('Price (₹)', fontsize=10)
            ax.grid(True, alpha=0.3)
            ax.tick_params(axis='x', rotation=45)
            
            # Add values on points
            for i, (date, price) in enumerate(zip(dates, closes)):
                ax.text(date, price, f'₹{price:.1f}', 
                       ha='center', va='bottom', fontsize=8)
            
            plt.tight_layout()
            
            # Save to bytes
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
            buf.seek(0)
            plt.close()
            
            return buf
            
        except Exception as e:
            logger.error(f"Error generating chart: {e}")
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
                    
                    logger.info(f"Expiry response type: {type(expiry_response)}")
                    logger.info(f"Expiry response: {expiry_response}")
                    
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
            
            logger.info(f"Option chain response type: {type(response)}")
            
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
                
                # 5 strikes around ATM
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
            message = "📊 *MARKET SUMMARY*\n\n"
            
            # Indices first
            if 'IDX_I' in live_data and live_data['IDX_I']:
                message += "*INDICES*\n```\n"
                for sec_id, data in live_data['IDX_I'].items():
                    emoji = "🟢" if data['change'] >= 0 else "🔴"
                    sign = "+" if data['change'] >= 0 else ""
                    message += f"{data['name']:<12} {data['ltp']:>8,.1f} {emoji} {sign}{data['change_pct']:>5.2f}%\n"
                message += "```\n\n"
            
            # Top gainers/losers
            if 'NSE_EQ' in live_data and live_data['NSE_EQ']:
                stocks = list(live_data['NSE_EQ'].values())
                stocks.sort(key=lambda x: x['change_pct'], reverse=True)
                
                message += "*TOP GAINERS*\n```\n"
                for stock in stocks[:min(5, len(stocks))]:
                    message += f"{stock['name']:<12} {stock['ltp']:>8,.1f} 🟢 +{stock['change_pct']:>5.2f}%\n"
                message += "```\n\n"
                
                if len(stocks) > 5:
                    message += "*TOP LOSERS*\n```\n"
                    for stock in stocks[-5:]:
                        message += f"{stock['name']:<12} {stock['ltp']:>8,.1f} 🔴 {stock['change_pct']:>5.2f}%\n"
                    message += "```"
            
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=message,
                parse_mode='Markdown'
            )
            logger.info("Live summary sent")
            
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
            message += "Strike   CE-LTP  CE-OI   PE-LTP  PE-OI\n"
            message += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            
            for opt in option_data['options']:
                atm = "🔸" if opt['is_atm'] else "  "
                
                ce_ltp = f"{opt['ce_ltp']:6.1f}" if opt['ce_ltp'] > 0 else "  -   "
                ce_oi = f"{opt['ce_oi']/1000:5.0f}K" if opt['ce_oi'] > 0 else "  -  "
                
                pe_ltp = f"{opt['pe_ltp']:6.1f}" if opt['pe_ltp'] > 0 else "  -   "
                pe_oi = f"{opt['pe_oi']/1000:5.0f}K" if opt['pe_oi'] > 0 else "  -  "
                
                message += f"{atm}{opt['strike']:5.0f} {ce_ltp} {ce_oi}  {pe_ltp} {pe_oi}\n"
            
            message += "```"
            
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=message,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            logger.error(f"Error sending OC: {e}")
    
    async def send_chart(self, name, historical_data):
        """Send historical chart as PNG"""
        try:
            chart = self.generate_chart(historical_data, f"{name} - Last 5 Days")
            
            if chart:
                await self.bot.send_photo(
                    chat_id=TELEGRAM_CHAT_ID,
                    photo=chart,
                    caption=f"📈 {name} - Historical Chart (5 Days)"
                )
                logger.info(f"Chart sent for {name}")
            
        except Exception as e:
            logger.error(f"Error sending chart: {e}")
    
    async def run(self):
        """Main loop"""
        logger.info("Multi-Stock Bot started")
        
        await self.send_startup_message()
        
        iteration = 0
        
        while self.running:
            try:
                # Live data every minute
                live_data = self.get_live_data_batch()
                if live_data:
                    await self.send_live_summary(live_data)
                
                # Option chains every 5 minutes
                if iteration % 5 == 0:
                    for idx_id in WATCHLIST.get('IDX_I', []):
                        oc = self.get_option_chain(idx_id)
                        if oc:
                            name = "Nifty 50" if idx_id == "13" else "Bank Nifty"
                            await self.send_option_chain_message(name, oc)
                            await asyncio.sleep(2)
                
                # Historical charts every 30 minutes
                if iteration % 30 == 0 and iteration > 0:
                    logger.info("Fetching historical data for charts...")
                    
                    # Indices
                    for idx_id in WATCHLIST.get('IDX_I', []):
                        hist_data = self.get_historical_batch([idx_id], "IDX_I", 5)
                        if idx_id in hist_data:
                            name = "Nifty 50" if idx_id == "13" else "Bank Nifty"
                            await self.send_chart(name, hist_data[idx_id])
                            await asyncio.sleep(1)
                    
                    # Top 5 stocks
                    stock_ids = WATCHLIST.get('NSE_EQ', [])[:5]
                    hist_data = self.get_historical_batch(stock_ids, "NSE_EQ", 5)
                    for sec_id, data in hist_data.items():
                        await self.send_chart(f"Stock {sec_id}", data)
                        await asyncio.sleep(1)
                
                iteration += 1
                await asyncio.sleep(60)  # 1 minute
                
            except KeyboardInterrupt:
                self.running = False
                break
            except Exception as e:
                logger.error(f"Main loop error: {e}", exc_info=True)
                await asyncio.sleep(60)
    
    async def send_startup_message(self):
        """Startup message"""
        try:
            total = sum(len(v) for v in WATCHLIST.values())
            msg = f"🤖 *Multi-Stock Dhan Bot*\n\n"
            msg += f"📊 Tracking {total} instruments\n"
            msg += f"✅ Live data - Every 1 min\n"
            msg += f"✅ Option chains - Every 5 min\n"
            msg += f"✅ Historical charts - Every 30 min\n\n"
            msg += f"⚡ Individual fetching enabled\n"
            msg += f"🚂 Railway.app"
            
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
            logger.error("Missing env vars")
            exit(1)
        
        bot = MultiStockDhanBot()
        asyncio.run(bot.run())
    except Exception as e:
        logger.error(f"Fatal: {e}", exc_info=True)
        exit(1)
