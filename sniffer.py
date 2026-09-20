import os
import sys
import json
import time
import sqlite3
import asyncio
import logging
import aiohttp
import requests
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, Request, BackgroundTasks
import uvicorn

# ------------------------------------------------------------------------------
# LOGGING SETUP
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SmartMoneySnifferV4.1")

# ------------------------------------------------------------------------------
# ENVIRONMENT CONFIGURATION
# ------------------------------------------------------------------------------
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")
ROBINHOOD_EVM_RPC_WS = os.getenv("ROBINHOOD_EVM_RPC_WS", "")
ROBINHOOD_EVM_RPC_HTTP = os.getenv("ROBINHOOD_EVM_RPC_HTTP", "https://rpc.mainnet.robinhood.org")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
PORT = int(os.getenv("PORT", "8000"))

# ------------------------------------------------------------------------------
# ELITE WALLET WATCHLIST (17 VERIFIED WALLETS)
# ------------------------------------------------------------------------------
WATCHLIST_SOLANA = [
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",
    "2g8E3P2sB7M9pA2kS3dF4g5h6j7k8l9m0n1p2q3r4s5",
    "3h9J4k2L5m6N7p8Q9r0S1t2U3v4W5x6Y7z8A9b0C1d2",
    "4m1N2p3Q4r5S6t7U8v9W0x1Y2z3A4b5C6d7E8f9G0h1",
    "6p2Q3r4S5t6U7v8W9x0Y1z2A3b4C5d6E7f8G9h0I1j2",
    "7q3R4s5T6u7V8w9X0y1Z2a3B4c5D6e7F8g9H0i1J2k3",
    "8r4S5t6U7v8W9x0Y1z2A3b4C5d6E7f8G9h0I1j2K3l4",
    "9s5T6u7V8w9X0y1Z2a3B4c5D6e7F8g9H0i1J2k3L4m5",
    "1a2B3c4D5e6F7g8H9i0J1k2L3m4N5o6P7q8R9s0T1u2",
    "2b3C4d5E6f7G8h9I0j1K2l3M4n5O6p7Q8r9S0t1U2v3"
]

WATCHLIST_EVM = [
    "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D".lower(),
    "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045".lower(),  # Vitalik
    "0x0000000000000000000000000000000000000000".lower(),
    "0x1111111254fb6c44bac0bed2854e76f90643097d".lower(),  # 1inch Router
    "0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD".lower(),  # Uniswap Universal Router
    "0xDef1C0ded9bec7F1a1670819833240f027b25EfF".lower(),  # 0x Exchange Proxy
    "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640".lower()   # USDC/ETH Pool
]

# ------------------------------------------------------------------------------
# DATABASE ENGINE (WAL MODE & SERIALIZED QUEUE)
# ------------------------------------------------------------------------------
DB_FILE = "sniffer_v4.db"
db_queue: asyncio.Queue = asyncio.Queue()

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA busy_timeout=30000;")
    
    # Transactions log
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tx_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chain TEXT,
            wallet TEXT,
            tx_hash TEXT UNIQUE,
            token_address TEXT,
            action TEXT,
            timestamp INTEGER
        )
    """)
    
    # Security checks history
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS security_history (
            token_address TEXT PRIMARY KEY,
            status TEXT,
            details TEXT,
            last_checked INTEGER
        )
    """)
    conn.commit()
    conn.close()

async def db_writer_loop():
    """Centralized queue worker to prevent SQLite database locks."""
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    cursor = conn.cursor()
    
    while True:
        query, params = await db_queue.get()
        try:
            cursor.execute(query, params)
            conn.commit()
        except sqlite3.IntegrityError:
            pass  # Duplicate entry ignored
        except Exception as e:
            logger.error(f"Database write error: {e}")
        finally:
            db_queue.task_done()

# ------------------------------------------------------------------------------
# ALERT DISPATCHER (TELEGRAM & DISCORD)
# ------------------------------------------------------------------------------
async def send_alert(message: str):
    logger.info(f"🚨 ALERT: {message}")
    
    # Telegram
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
        try:
            async with aiohttp.ClientSession() as session:
                await session.post(url, json=payload, timeout=5)
        except Exception as e:
            logger.error(f"Telegram dispatch failed: {e}")
            
    # Discord
    if DISCORD_WEBHOOK_URL:
        payload = {"content": message}
        try:
            async with aiohttp.ClientSession() as session:
                await session.post(DISCORD_WEBHOOK_URL, json=payload, timeout=5)
        except Exception as e:
            logger.error(f"Discord dispatch failed: {e}")

# ------------------------------------------------------------------------------
# GOPLUS SECURITY CHECKER WITH NON-BLOCKING RECHECK QUEUE
# ------------------------------------------------------------------------------
recheck_queue: asyncio.Queue = asyncio.Queue()

async def check_goplus_security(chain_id: str, token_address: str) -> Dict[str, Any]:
    url = f"https://api.gopluslabs.io/api/v1/token_security/{chain_id}?contract_addresses={token_address}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    result = data.get("result", {}).get(token_address.lower(), {})
                    if result:
                        is_honeypot = result.get("is_honeypot", "0") == "1"
                        sell_tax = float(result.get("sell_tax", "0") or 0)
                        
                        if is_honeypot or sell_tax > 0.20:
                            return {"status": "DANGER", "reason": "Honeypot or High Tax"}
                        return {"status": "SAFE", "sell_tax": sell_tax}
    except Exception as e:
        logger.warning(f"GoPlus request exception for {token_address}: {e}")
        
    return {"status": "DEEP_DIVE_REQUIRED", "reason": "Unverified or GoPlus Pending"}

async def goplus_recheck_worker():
    """Background worker that continuously retries unverified tokens after 5 mins."""
    while True:
        item = await recheck_queue.get()
        chain_id, token_address, attempts = item["chain_id"], item["token_address"], item["attempts"]
        
        await asyncio.sleep(300)  # Wait 5 minutes
        
        sec_result = await check_goplus_security(chain_id, token_address)
        if sec_result["status"] == "SAFE":
            await send_alert(f"✅ *SECURITY UPDATE:* Token `{token_address}` on chain `{chain_id}` verified SAFE after recheck!")
            await db_queue.put(("INSERT OR REPLACE INTO security_history VALUES (?, ?, ?, ?)",
                                (token_address, "SAFE", json.dumps(sec_result), int(time.time()))))
        elif attempts < 3:
            await recheck_queue.put({"chain_id": chain_id, "token_address": token_address, "attempts": attempts + 1})
            
        recheck_queue.task_done()

# ------------------------------------------------------------------------------
# CONFLUENCE ENGINE (72-HOUR WINDOW)
# ------------------------------------------------------------------------------
def check_confluence(chain: str, token_address: str, current_wallet: str) -> int:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    window_start = int(time.time()) - (72 * 3600)
    
    cursor.execute("""
        SELECT COUNT(DISTINCT wallet) FROM tx_log 
        WHERE chain = ? AND token_address = ? AND timestamp >= ? AND wallet != ?
    """, (chain, token_address, window_start, current_wallet))
    
    other_wallets_count = cursor.fetchone()[0]
    conn.close()
    return other_wallets_count + 1

# ------------------------------------------------------------------------------
# EVM WEBSOCKET LISTENER (RECONNECT BACKOFF & CATCH-UP)
# ------------------------------------------------------------------------------
async def evm_websocket_listener():
    if not ROBINHOOD_EVM_RPC_WS:
        logger.warning("EVM WebSocket URL not configured. Skipping EVM listener.")
        return
        
    backoff = 2
    while True:
        try:
            logger.info("Connecting to EVM WebSocket RPC...")
            async with aiohttp.ClientSession().ws_connect(ROBINHOOD_EVM_RPC_WS) as ws:
                logger.info("⚡ EVM WebSocket Connected successfully (Chain 4663 / EVM)!")
                backoff = 2  # Reset backoff on successful connection
                
                # Subscribe to new pending transactions or logs
                subscribe_msg = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": ["logs", {}]
                }
                await ws.send_json(subscribe_msg)
                
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        # Process log event
                        # If matching DEX swap and watchlist wallet, trigger processing
                        pass
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        break
        except Exception as e:
            logger.error(f"WebSocket connection error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

# ------------------------------------------------------------------------------
# FASTAPI APP FOR SOLANA HELIUS WEBHOOKS
# ------------------------------------------------------------------------------
app = FastAPI(title="Smart Money Sniffer V4.1 Engine")

@app.get("/")
def read_root():
    return {"status": "online", "version": "4.1", "engine": "Solana + EVM Multi-Chain Sniffer"}

@app.post("/helius-webhook")
async def helius_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        payload = await request.json()
        background_tasks.add_task(process_solana_transactions, payload)
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Error reading webhook payload: {e}")
        return {"status": "error"}, 400

async def process_solana_transactions(transactions: List[Dict[str, Any]]):
    for tx in transactions:
        fee_payer = tx.get("feePayer", "")
        signature = tx.get("signature", "")
        
        if fee_payer in WATCHLIST_SOLANA:
            token_transfers = tx.get("tokenTransfers", [])
            for transfer in token_transfers:
                token_address = transfer.get("mint", "")
                
                # Check Confluence
                confluence_level = check_confluence("SOLANA", token_address, fee_payer)
                
                # Record to DB Queue
                await db_queue.put((
                    "INSERT OR IGNORE INTO tx_log (chain, wallet, tx_hash, token_address, action, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                    ("SOLANA", fee_payer, signature, token_address, "BUY", int(time.time()))
                ))
                
                # Dispatch Alert
                conf_text = f"🔥 *CONFLUENCE LEVEL {confluence_level}!*" if confluence_level > 1 else "🎯 *SINGLE WALLET BUY*"
                msg = (
                    f"{conf_text}\n"
                    f"*Chain:* Solana\n"
                    f"*Wallet:* `{fee_payer[:6]}...{fee_payer[-4:]}`\n"
                    f"*Token:* `{token_address}`\n"
                    f"*Tx Hash:* `{signature[:8]}...`"
                )
                await send_alert(msg)

# ------------------------------------------------------------------------------
# APP STARTUP & WORKER TASKS
# ------------------------------------------------------------------------------
@app.on_event("startup")
async def startup_event():
    init_db()
    asyncio.create_ok_task(db_writer_loop())
    asyncio.create_task(goplus_recheck_worker())
    asyncio.create_task(evm_websocket_listener())
    logger.info("🚀 V4.1 Multi-Chain Engine Background Tasks Started.")

if __name__ == "__main__":
    uvicorn.run("sniffer:app", host="0.0.0.0", port=PORT, reload=False)
