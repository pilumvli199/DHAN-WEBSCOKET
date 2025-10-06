import os
import asyncio
import json
from datetime import datetime
# Note: dhanhq import may be different depending on package; this file uses the names you provided.
from dhanhq import dhanhq, marketfeed
import requests
from telegram import Bot
from telegram.ext import Application, CommandHandler
import logging

# Logging setup
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Environment variables
DHAN_CLIENT_ID = os.getenv('DHAN_CLIENT_ID')
DHAN_ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Initialize DhanHQ (placeholder - follow dhanhq package usage)
dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)

# Instrument tokens (update with actual tokens)
INSTRUMENTS = {
    'NIFTY50': {
        'security_id': '13',   # NSE NIFTY 50 index (placeholder)
        'exchange': 'IDX_I'
    },
    'TCS': {
        'security_id': '11536',  # TCS stock security ID (placeholder)
        'exchange': 'NSE_EQ'
    }
}

class OptionChainBot:
    def __init__(self):
        self.telegram_bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.latest_data = {}
        self.running = True
        
    async def get_ltp(self, security_id, exchange):
        try:
            # The real dhanhq client method may differ; adapt as needed.
            quote = dhan.get_market_quote(security_id, exchange)
            if quote and isinstance(quote, dict) and quote.get('status') == 'success':
                # adapt key names based on actual response
                return quote['data'].get('LTP') or quote['data'].get('ltp') or None
            return None
        except Exception as e:
            logger.error(f"Error fetching LTP: {e}")
            return None
    
    def get_option_chain(self, symbol, spot_price):
        try:
            if symbol == 'NIFTY50':
                strike_interval = 50
                atm_strike = round(spot_price / strike_interval) * strike_interval
            else:  # TCS (you can customize interval)
                strike_interval = 50
                atm_strike = round(spot_price / strike_interval) * strike_interval
            
            expiry = self.get_nearest_expiry(symbol)
            
            option_data = []
            for i in range(-10, 11):
                strike = atm_strike + (i * strike_interval)
                ce_data = self.get_option_data(symbol, strike, 'CE', expiry)
                pe_data = self.get_option_data(symbol, strike, 'PE', expiry)
                
                option_data.append({
                    'strike': strike,
                    'CE': ce_data,
                    'PE': pe_data,
                    'is_atm': i == 0
                })
            return option_data
        except Exception as e:
            logger.error(f"Error fetching option chain: {e}")
            return []
    
    def get_option_data(self, symbol, strike, option_type, expiry):
        try:
            # Placeholder - replace with real option security IDs from DhanHQ
            security_id = f"{symbol}_{strike}_{option_type}_{expiry}"
            return {
                'ltp': 0.0,
                'volume': 0,
                'oi': 0,
                'iv': 0
            }
        except Exception as e:
            logger.error(f"Error fetching option data: {e}")
            return None
    
    def get_nearest_expiry(self, symbol):
        # Placeholder expiry; update logic to compute nearest weekly/monthly expiry
        return "2025-10-03"
    
    def format_message(self, symbol, spot_price, option_chain):
        msg = f"🔔 *{symbol} Option Chain Update*\\n"
        msg += f"📊 *Spot Price:* ₹{spot_price:.2f}\\n"
        msg += f"⏰ *Time:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\\n"
        msg += "━━━━━━━━━━━━━━━━━━━━\\n\\n"
        
        msg += "```\\n"
        msg += f"{'Strike':<8} {'CE LTP':<10} {'PE LTP':<10}\\n"
        msg += "-" * 35 + "\\n"
        
        for opt in option_chain:
            strike_marker = "➤" if opt['is_atm'] else " "
            ce_ltp = opt['CE']['ltp'] if opt['CE'] else 0.0
            pe_ltp = opt['PE']['ltp'] if opt['PE'] else 0.0
            msg += f"{strike_marker}{opt['strike']:<7} {ce_ltp:<10.2f} {pe_ltp:<10.2f}\\n"
        
        msg += "```\\n"
        return msg
    
    async def send_telegram_message(self, message):
        try:
            await self.telegram_bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=message,
                parse_mode='Markdown'
            )
            logger.info("Message sent to Telegram")
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")
    
    async def process_and_send_data(self):
        while self.running:
            try:
                for symbol, details in INSTRUMENTS.items():
                    ltp = await self.get_ltp(details['security_id'], details['exchange'])
                    if ltp:
                        logger.info(f"{symbol} LTP: {ltp}")
                        option_chain = self.get_option_chain(symbol, ltp)
                        message = self.format_message(symbol, ltp, option_chain)
                        await self.send_telegram_message(message)
                    else:
                        logger.info(f"No LTP for {symbol} this cycle")
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(10)
    
    async def start_command(self, update, context):
        await update.message.reply_text(
            "🚀 DhanHQ Option Chain Bot Started!\\n"
            "You'll receive updates every 60 seconds.\\n\\n"
            "Commands:\\n"
            "/start - Start bot\\n"
            "/stop - Stop updates\\n"
            "/status - Check bot status"
        )
    
    async def stop_command(self, update, context):
        self.running = False
        await update.message.reply_text("⏹️ Bot stopped!")
    
    async def status_command(self, update, context):
        status = "🟢 Running" if self.running else "🔴 Stopped"
        await update.message.reply_text(f"Bot Status: {status}")
    
    async def run(self):
        app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        app.add_handler(CommandHandler("start", self.start_command))
        app.add_handler(CommandHandler("stop", self.stop_command))
        app.add_handler(CommandHandler("status", self.status_command))
        
        await app.initialize()
        await app.start()
        logger.info("Bot started successfully!")
        await self.process_and_send_data()
        await app.stop()

async def main():
    bot = OptionChainBot()
    await bot.run()

if __name__ == "__main__":
    asyncio.run(main())
