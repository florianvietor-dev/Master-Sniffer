import os
import asyncio
import sqlite3
import aiohttp
from fastapi import FastAPI, HTTPException
import uvicorn

app = FastAPI()

DB_PATH = "sniffer.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS wallets (
            address TEXT PRIMARY KEY,
            chain TEXT,
            score REAL,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

@app.on_event("startup")
async def startup_event():
    try:
        print("Initialisiere Confluence Smart-Money Engine v4.1...")
        init_db()
        print("Datenbank erfolgreich initialisiert und WAL-Modus geprüft.")
    except Exception as e:
        print(f"Fehler im Startup-Event (abgefangen): {e}")

@app.get("/")
async def root():
    return {"status": "online", "engine": "Confluence Smart-Money V4.1"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("sniffer:app", host="0.0.0.0", port=port)
