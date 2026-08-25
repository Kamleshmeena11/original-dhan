
import os
import sys
import time
import logging
import asyncio
from datetime import datetime
import pandas as pd

# Dhan v2.2.0+ imports
from dhanhq import DhanContext, FullDepth
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- Configuration & Credentials ---
CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN")

# TCS on NSE Equity. FullDepth only supports NSE_EQ (1) and NSE_FNO (2) -
# index instruments (IDX) have no order book, so this can't be an index.
SECURITY_ID = "11536"          # TCS (NSE_EQ), per Dhan's instrument master
DEPTH_LEVEL = 200               # 200-level Full Market Depth
INSTRUMENTS = [(FullDepth.NSE, SECURITY_ID)]

# 200-level depth only allows ONE instrument per FullDepth connection -
# if you need depth on more than one symbol, run a separate connection
# (and separate output file) per instrument.

CSV_FILENAME = "tcs_depth_200_raw.csv"
CSV_COLUMNS = ["timestamp", "exchange_segment", "security_id", "side", "level", "price", "quantity", "orders"]

row_buffer = []


def upload_to_drive():
    if not os.path.exists(CSV_FILENAME):
        logger.info(f"Local file {CSV_FILENAME} does not exist yet. Skipping sync...")
        return
    if not all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN]):
        logger.warning("Google Drive credentials missing. Skipping cloud backup.")
        return
    try:
        creds = Credentials(
            token=None,
            refresh_token=GOOGLE_REFRESH_TOKEN,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET
        )
        service = build("drive", "v3", credentials=creds)
        query = f"name = '{CSV_FILENAME}' and trashed = false"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])
        media = MediaFileUpload(CSV_FILENAME, mimetype="text/csv", resumable=True)
        if files:
            file_id = files[0]["id"]
            logger.info(f"Updating existing Google Drive file: {file_id}")
            service.files().update(fileId=file_id, media_body=media).execute()
        else:
            logger.info("Creating new file on Google Drive...")
            file_metadata = {"name": CSV_FILENAME}
            new_file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
            logger.info(f"File created successfully with ID: {new_file.get('id')}")
    except Exception as e:
        logger.error(f"Error syncing to Google Drive: {e}")


def on_depth_update(update):
    """Turns one Bid or Ask depth packet into flat rows and buffers them.

    Each packet is saved exactly as received (no aggregation) - this is
    raw tick-level order book data, not OHLC bars. Timestamp is LOCAL
    receive time: the 200-depth packet header carries no exchange
    timestamp field to bucket on (unlike Ticker/Full mode's LTT).
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    side = "bid" if update["type"] == "Bid" else "ask"
    for level, entry in enumerate(update["depth"], start=1):
        row_buffer.append({
            "timestamp": ts,
            "exchange_segment": update["exchange_segment"],
            "security_id": update["security_id"],
            "side": side,
            "level": level,
            "price": entry["price"],
            "quantity": entry["quantity"],
            "orders": entry["orders"],
        })


def flush_buffer_to_csv():
    global row_buffer
    if not row_buffer:
        return
    rows, row_buffer = row_buffer, []
    df = pd.DataFrame(rows, columns=CSV_COLUMNS)
    file_exists = os.path.isfile(CSV_FILENAME)
    df.to_csv(CSV_FILENAME, mode="a", index=False, header=not file_exists)
    logger.info(f"Flushed {len(rows)} depth rows to {CSV_FILENAME}")


async def depth_receive_loop(feed):
    """Owns the FullDepth receive loop. FullDepth has no on_connect/on_message
    callback hooks like MarketFeed - connect() opens the socket and
    subscribes, then get_instrument_data() must be iterated (it's an
    async generator, one Bid or Ask packet per yield) to actually pull data.

    Reconnects use exponential backoff, same rationale as the original
    Ticker feed: avoid hammering Dhan's WS gateway (max 5 connections per
    client, rate-limited reconnects).
    """
    backoff = 3
    max_backoff = 60
    await feed.connect()
    logger.info(f"Connected to Dhan {DEPTH_LEVEL}-level depth feed for security {SECURITY_ID}.")

    while True:
        try:
            async for update in feed.get_instrument_data():
                on_depth_update(update)
                backoff = 3  # reset after any successful read
        except Exception as e:
            logger.error(f"Depth feed receive error: {e}")

            try:
                await feed.disconnect()
            except Exception:
                pass  # already dead, nothing to clean up

            if "429" in str(e):
                backoff = max(backoff, 30)  # respect Dhan's rate limit explicitly

            logger.error(f"Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

            try:
                await feed.connect()
            except Exception as reconnect_err:
                logger.error(f"Reconnect failed: {reconnect_err}")


async def flush_timer_loop():
    while True:
        await asyncio.sleep(1)
        flush_buffer_to_csv()


async def google_drive_sync_loop():
    while True:
        await asyncio.sleep(10)
        upload_to_drive()


async def main():
    if not CLIENT_ID or not ACCESS_TOKEN:
        logger.error("Dhan credentials (DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN) are missing!")
        sys.exit(1)

    dhan_context = DhanContext(client_id=CLIENT_ID, access_token=ACCESS_TOKEN)
    feed = FullDepth(
        dhan_context=dhan_context,
        instruments=INSTRUMENTS,
        depth_level=DEPTH_LEVEL
    )

    tasks = [
        asyncio.create_task(depth_receive_loop(feed)),
        asyncio.create_task(flush_timer_loop()),
        asyncio.create_task(google_drive_sync_loop())
    ]

    logger.info("Starting loops and WebSocket client connection...")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script stopped manually by user.")
