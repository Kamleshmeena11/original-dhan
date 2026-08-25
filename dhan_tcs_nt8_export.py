import os
import sys
import logging
import asyncio
from datetime import datetime, timedelta
import pandas as pd

from dhanhq import DhanContext, MarketFeed, FullDepth
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

# TCS on NSE Equity. FullDepth only supports NSE_EQ / NSE_FNO (no indices -
# an index has no order book), and 200-level depth allows one instrument
# per connection.
SECURITY_ID = "11536"
DEPTH_LEVEL = 200
TICKER_INSTRUMENTS = [(MarketFeed.NSE, SECURITY_ID, MarketFeed.Ticker)]
DEPTH_INSTRUMENTS = [(FullDepth.NSE, SECURITY_ID)]

TICK_FILENAME = "TCS_tick.txt"
DEPTH_FILENAME = "TCS_depth_L2.txt"

# Dhan's LTT (Ticker) is UTC epoch seconds - NSE/NT8 charts normally run in
# IST. Adjust or remove this if your NinjaTrader instance is configured for
# a different timezone.
IST_OFFSET = timedelta(hours=5, minutes=30)

tick_buffer = []
depth_buffer = []

# Last known best bid/ask, kept updated from the depth feed's level-0
# entries, so the tick file can populate Bid/Ask alongside each trade print.
best_bid = {"price": None, "quantity": None}
best_ask = {"price": None, "quantity": None}

# Previous depth snapshot per side, used to turn Dhan's full snapshots into
# proper Add/Update/Remove deltas instead of re-declaring "Add" every time.
prev_depth = {"bid": [], "ask": []}


def fmt_price(x):
    """29561.50 -> '29561.5', 29564.00 -> '29564' - matches NT8 export style."""
    x = round(float(x), 2)
    if x == int(x):
        return str(int(x))
    return f"{x:.2f}".rstrip("0").rstrip(".")


def nt_timestamp_from_epoch(epoch_seconds):
    """Exchange timestamp (Dhan LTT, whole seconds only) -> (yyyyMMdd, HHmmss).
    Sub-second precision isn't in Dhan's Ticker packet, so the offset field
    is filled from local receive time (see nt_offset_now)."""
    dt = datetime.utcfromtimestamp(epoch_seconds) + IST_OFFSET
    return dt.strftime("%Y%m%d"), dt.strftime("%H%M%S")


def nt_timestamp_now():
    """No exchange timestamp is available on depth packets at all - use
    local receive time for both the date/time and the offset."""
    dt = datetime.utcnow() + IST_OFFSET
    return dt.strftime("%Y%m%d"), dt.strftime("%H%M%S"), dt.microsecond * 10


def nt_offset_now():
    return datetime.utcnow().microsecond * 10


def flush_buffers():
    global tick_buffer, depth_buffer
    if tick_buffer:
        rows, tick_buffer = tick_buffer, []
        with open(TICK_FILENAME, "a") as f:
            f.write("\n".join(rows) + "\n")
        logger.info(f"Flushed {len(rows)} rows to {TICK_FILENAME}")
    if depth_buffer:
        rows, depth_buffer = depth_buffer, []
        with open(DEPTH_FILENAME, "a") as f:
            f.write("\n".join(rows) + "\n")
        logger.info(f"Flushed {len(rows)} rows to {DEPTH_FILENAME}")


def upload_to_drive(filename):
    if not os.path.exists(filename):
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
        query = f"name = '{filename}' and trashed = false"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])
        media = MediaFileUpload(filename, mimetype="text/plain", resumable=True)
        if files:
            service.files().update(fileId=files[0]["id"], media_body=media).execute()
        else:
            file_metadata = {"name": filename}
            service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        logger.info(f"Synced {filename} to Google Drive")
    except Exception as e:
        logger.error(f"Error syncing {filename} to Google Drive: {e}")


# ---------------------------------------------------------------------
# Ticker feed -> raw tick file (Timestamp;Last;Bid;Ask;Volume) + L1 "Last"
# ---------------------------------------------------------------------

def on_ticker_message(instance, message):
    try:
        if not message or message.get("type") != "Ticker Data":
            return
        last = float(message.get("LTP"))
        date_str, time_str = nt_timestamp_from_epoch_from_ticker(message)
        offset = nt_offset_now()

        bid = best_bid["price"] if best_bid["price"] is not None else last
        ask = best_ask["price"] if best_ask["price"] is not None else last
        volume = 1  # Ticker mode carries no traded quantity; switch to
                    # MarketFeed.Quote (has LTQ) if you need real size here.

        tick_buffer.append(
            f"{date_str} {time_str} {offset:07d};{fmt_price(last)};{fmt_price(bid)};{fmt_price(ask)};{volume}"
        )
        depth_buffer.append(
            f"L1;2;{date_str}{time_str};{offset};{fmt_price(last)};{volume}"
        )
    except Exception as e:
        logger.error(f"Error handling ticker message: {e}")


def nt_timestamp_from_epoch_from_ticker(message):
    # dhanhq's process_ticker() already formats LTT down to 'HH:MM:SS' via
    # utc_time(), losing the date. Re-derive both from local time instead,
    # since Dhan's LTT is whole-second UTC epoch with no sub-second part
    # anyway - see the module docstring note above on offset precision.
    return nt_timestamp_now()[0], nt_timestamp_now()[1]


async def ticker_receive_loop(feed):
    backoff = 3
    max_backoff = 60
    await feed.connect()
    logger.info("Connected to Dhan Ticker feed for TCS.")
    while True:
        try:
            data = await feed.get_instrument_data()
            on_ticker_message(feed, data)
            backoff = 3
        except Exception as e:
            logger.error(f"Ticker feed receive error: {e}")
            try:
                await feed.disconnect()
            except Exception:
                pass
            if "429" in str(e):
                backoff = max(backoff, 30)
            logger.error(f"Reconnecting Ticker feed in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
            try:
                await feed.connect()
            except Exception as reconnect_err:
                logger.error(f"Ticker reconnect failed: {reconnect_err}")


# ---------------------------------------------------------------------
# 200-level depth feed -> L2 lines (diffed vs previous snapshot) + L1
# lines for best bid/ask, all written to the depth file
# ---------------------------------------------------------------------

def on_depth_update(update):
    side_key = "bid" if update["type"] == "Bid" else "ask"
    side_code = 1 if side_key == "bid" else 0
    date_str, time_str, offset = nt_timestamp_now()

    new_depth = update["depth"]  # list of {"price","quantity","orders"}
    old_depth = prev_depth[side_key]
    max_len = max(len(old_depth), len(new_depth))

    for i in range(max_len):
        new_e = new_depth[i] if i < len(new_depth) else None
        old_e = old_depth[i] if i < len(old_depth) else None

        if new_e is not None and old_e is None:
            op = 0  # Add
        elif new_e is None and old_e is not None:
            op = 2  # Remove
            new_e = old_e  # emit the removed price/size
        elif new_e["price"] != old_e["price"] or new_e["quantity"] != old_e["quantity"]:
            op = 1  # Update
        else:
            continue  # unchanged level, skip

        depth_buffer.append(
            f"L2;{side_code};{date_str}{time_str};{offset};{op};{i};;{fmt_price(new_e['price'])};{new_e['quantity']}"
        )

    prev_depth[side_key] = new_depth

    if new_depth:
        top = new_depth[0]
        cache = best_bid if side_key == "bid" else best_ask
        if cache["price"] != top["price"] or cache["quantity"] != top["quantity"]:
            cache["price"], cache["quantity"] = top["price"], top["quantity"]
            depth_buffer.append(
                f"L1;{side_code};{date_str}{time_str};{offset};{fmt_price(top['price'])};{top['quantity']}"
            )


async def depth_receive_loop(feed):
    backoff = 3
    max_backoff = 60
    await feed.connect()
    logger.info(f"Connected to Dhan {DEPTH_LEVEL}-level depth feed for TCS.")
    while True:
        try:
            async for update in feed.get_instrument_data():
                on_depth_update(update)
                backoff = 3
        except Exception as e:
            logger.error(f"Depth feed receive error: {e}")
            try:
                await feed.disconnect()
            except Exception:
                pass
            if "429" in str(e):
                backoff = max(backoff, 30)
            logger.error(f"Reconnecting depth feed in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
            try:
                await feed.connect()
            except Exception as reconnect_err:
                logger.error(f"Depth reconnect failed: {reconnect_err}")


async def flush_timer_loop():
    while True:
        await asyncio.sleep(1)
        flush_buffers()


async def google_drive_sync_loop():
    while True:
        await asyncio.sleep(10)
        upload_to_drive(TICK_FILENAME)
        upload_to_drive(DEPTH_FILENAME)


async def main():
    if not CLIENT_ID or not ACCESS_TOKEN:
        logger.error("Dhan credentials (DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN) are missing!")
        sys.exit(1)

    dhan_context = DhanContext(client_id=CLIENT_ID, access_token=ACCESS_TOKEN)

    ticker_feed = MarketFeed(dhan_context=dhan_context, instruments=TICKER_INSTRUMENTS, version="v2")
    depth_feed = FullDepth(dhan_context=dhan_context, instruments=DEPTH_INSTRUMENTS, depth_level=DEPTH_LEVEL)

    tasks = [
        asyncio.create_task(ticker_receive_loop(ticker_feed)),
        asyncio.create_task(depth_receive_loop(depth_feed)),
        asyncio.create_task(flush_timer_loop()),
        asyncio.create_task(google_drive_sync_loop())
    ]

    logger.info("Starting loops and WebSocket client connections...")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script stopped manually by user.")
        flush_buffers()
