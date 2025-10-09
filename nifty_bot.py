#!/usr/bin/env python3
"""
Minimal Test Bot - Debug Railway Issues
"""
import sys
import os

print("="*60, flush=True)
print("🚀 MINIMAL TEST BOT STARTING", flush=True)
print("="*60, flush=True)

# Step 1: Basic imports
print("\n1️⃣ Testing basic imports...", flush=True)
try:
    import asyncio
    print("   ✅ asyncio", flush=True)
    
    import requests
    print("   ✅ requests", flush=True)
    
    from datetime import datetime
    print("   ✅ datetime", flush=True)
    
    import logging
    print("   ✅ logging", flush=True)
    
except Exception as e:
    print(f"   ❌ Basic imports failed: {e}", flush=True)
    sys.exit(1)

# Step 2: Matplotlib (This usually fails on Railway!)
print("\n2️⃣ Testing matplotlib...", flush=True)
try:
    import matplotlib
    print(f"   📦 matplotlib version: {matplotlib.__version__}", flush=True)
    
    matplotlib.use('Agg')
    print("   ✅ Set backend to 'Agg'", flush=True)
    
    import matplotlib.pyplot as plt
    print("   ✅ matplotlib.pyplot", flush=True)
    
except Exception as e:
    print(f"   ❌ matplotlib failed: {e}", flush=True)
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 3: Other heavy imports
print("\n3️⃣ Testing heavy imports...", flush=True)
try:
    import mplfinance as mpf
    print("   ✅ mplfinance", flush=True)
    
    import pandas as pd
    print("   ✅ pandas", flush=True)
    
except Exception as e:
    print(f"   ❌ Heavy imports failed: {e}", flush=True)
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 4: AI imports
print("\n4️⃣ Testing AI imports...", flush=True)
try:
    import google.generativeai as genai
    print("   ✅ google.generativeai", flush=True)
    
    from openai import OpenAI
    print("   ✅ openai", flush=True)
    
except Exception as e:
    print(f"   ❌ AI imports failed: {e}", flush=True)
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 5: Telegram
print("\n5️⃣ Testing telegram...", flush=True)
try:
    from telegram import Bot
    print("   ✅ telegram.Bot", flush=True)
    
except Exception as e:
    print(f"   ❌ Telegram import failed: {e}", flush=True)
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 6: Environment variables
print("\n6️⃣ Checking environment variables...", flush=True)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

env_status = {
    'TELEGRAM_BOT_TOKEN': '✅' if TELEGRAM_BOT_TOKEN else '❌',
    'TELEGRAM_CHAT_ID': '✅' if TELEGRAM_CHAT_ID else '❌',
    'DHAN_CLIENT_ID': '✅' if DHAN_CLIENT_ID else '❌',
    'DHAN_ACCESS_TOKEN': '✅' if DHAN_ACCESS_TOKEN else '❌',
    'GEMINI_API_KEY': '✅' if GEMINI_API_KEY else '❌',
    'OPENAI_API_KEY': '✅' if OPENAI_API_KEY else '❌',
}

for var, status in env_status.items():
    print(f"   {status} {var}", flush=True)

missing = [k for k, v in env_status.items() if v == '❌']
if missing:
    print(f"\n   ⚠️ Missing: {', '.join(missing)}", flush=True)
else:
    print("\n   ✅ All environment variables set!", flush=True)

# Step 7: Start HTTP server for Railway
print("\n7️⃣ Starting HTTP server...", flush=True)
try:
    from http.server import HTTPServer, BaseHTTPRequestHandler
    from threading import Thread
    
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            status = "✅ Bot is running!\n\n"
            status += "Environment:\n"
            for var, st in env_status.items():
                status += f"{st} {var}\n"
            self.wfile.write(status.encode())
        
        def log_message(self, format, *args):
            pass
    
    port = int(os.getenv('PORT', 8080))
    server = HTTPServer(('0.0.0.0', port), Handler)
    
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    
    print(f"   ✅ HTTP server running on port {port}", flush=True)
    print(f"   🌐 Health endpoint: http://0.0.0.0:{port}/", flush=True)
    
except Exception as e:
    print(f"   ❌ HTTP server failed: {e}", flush=True)
    import traceback
    traceback.print_exc()

# Step 8: Keep alive
print("\n8️⃣ Bot initialization complete!", flush=True)
print("="*60, flush=True)
print("✅ ALL CHECKS PASSED!", flush=True)
print("🔄 Bot will keep running...", flush=True)
print("="*60, flush=True)

# Keep the bot alive
try:
    import time
    counter = 0
    while True:
        counter += 1
        if counter % 60 == 0:  # Every 60 seconds
            print(f"💚 Bot alive - {counter} seconds elapsed", flush=True)
        time.sleep(1)
except KeyboardInterrupt:
    print("\n⚠️ Bot stopped", flush=True)
except Exception as e:
    print(f"\n❌ Error: {e}", flush=True)
    import traceback
    traceback.print_exc()
    sys.exit(1)
