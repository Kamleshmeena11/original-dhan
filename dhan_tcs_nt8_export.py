"""
Diagnostic script - run this BY ITSELF (not the collector) to isolate why
both the Ticker and 200-depth feeds are getting reset immediately after
connecting.

It runs four checks, in order, and prints a plain verdict after each:

  1. REST call to Dhan's fund-limit endpoint  -> is the token valid at all?
  2. Outbound IP this machine is connecting from -> for IP-allowlist checks
  3. Ticker feed ALONE (no depth feed running concurrently)
  4. 200-depth feed ALONE (no ticker feed running concurrently)

Steps 3/4 print the *real* close code/reason from the websockets library
instead of the generic "no close frame received or sent" string, which is
just str(exception) losing the actual close code.
"""
import os
import sys
import asyncio
import requests
import websockets

from dhanhq import DhanContext, MarketFeed, FullDepth

CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")
SECURITY_ID = "11536"  # TCS


def check_rest_api():
    print("\n--- 1. REST API check (is the token valid at all?) ---")
    try:
        resp = requests.get(
            "https://api.dhan.co/v2/fundlimit",
            headers={"access-token": ACCESS_TOKEN, "client-id": CLIENT_ID},
            timeout=10,
        )
        print(f"HTTP {resp.status_code}")
        print(resp.text[:500])
        if resp.status_code == 200:
            print("VERDICT: token is valid for REST calls.")
        else:
            print("VERDICT: token itself is rejected (bad/expired token, wrong client-id, or account issue).")
    except Exception as e:
        print(f"REST call failed outright: {e}")


def check_outbound_ip():
    print("\n--- 2. Outbound IP this machine is using ---")
    try:
        ip = requests.get("https://api.ipify.org", timeout=10).text
        print(f"Outbound IP: {ip}")
        print("If Dhan requires IP allowlisting for market data feeds, this is the IP to check/register.")
    except Exception as e:
        print(f"Could not determine outbound IP: {e}")


async def check_ticker_alone():
    print("\n--- 3. Ticker feed ALONE ---")
    dhan_context = DhanContext(client_id=CLIENT_ID, access_token=ACCESS_TOKEN)
    feed = MarketFeed(
        dhan_context=dhan_context,
        instruments=[(MarketFeed.NSE, SECURITY_ID, MarketFeed.Ticker)],
        version="v2",
    )
    try:
        await feed.connect()
        print("Connected. Waiting for one message...")
        data = await asyncio.wait_for(feed.get_instrument_data(), timeout=15)
        print(f"Received: {data}")
        print("VERDICT: Ticker feed works fine in isolation.")
    except websockets.exceptions.ConnectionClosed as e:
        print(f"ConnectionClosed - code={e.code} reason={e.reason!r} rcvd={e.rcvd} sent={e.sent}")
        print("VERDICT: server reset the connection (abnormal close, no close frame either direction "
              "means TCP-level reset, not an application-level close) - almost always auth/entitlement/IP, not a code bug.")
    except asyncio.TimeoutError:
        print("VERDICT: connected but no data arrived in 15s (market closed, or subscribed to wrong instrument).")
    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}")
    finally:
        try:
            await feed.disconnect()
        except Exception:
            pass


async def check_depth_alone():
    print("\n--- 4. 200-depth feed ALONE ---")
    dhan_context = DhanContext(client_id=CLIENT_ID, access_token=ACCESS_TOKEN)
    feed = FullDepth(
        dhan_context=dhan_context,
        instruments=[(FullDepth.NSE, SECURITY_ID)],
        depth_level=200,
    )
    try:
        await feed.connect()
        print("Connected. Waiting for one message...")
        gen = feed.get_instrument_data()
        update = await asyncio.wait_for(gen.__anext__(), timeout=15)
        print(f"Received: {update}")
        print("VERDICT: 200-depth feed works fine in isolation.")
    except websockets.exceptions.ConnectionClosed as e:
        print(f"ConnectionClosed - code={e.code} reason={e.reason!r} rcvd={e.rcvd} sent={e.sent}")
        print("VERDICT: server reset the connection - if step 3 (plain Ticker) SUCCEEDED but this fails, "
              "it's specifically a 200-depth entitlement problem, not your token/IP in general.")
    except asyncio.TimeoutError:
        print("VERDICT: connected but no data arrived in 15s (market closed, or subscribed to wrong instrument).")
    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}")
    finally:
        try:
            await feed.disconnect()
        except Exception:
            pass


async def main():
    if not CLIENT_ID or not ACCESS_TOKEN:
        print("DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN not set in environment.")
        sys.exit(1)

    check_rest_api()
    check_outbound_ip()
    await check_ticker_alone()
    await asyncio.sleep(2)  # let the first connection fully close before opening the next
    await check_depth_alone()

    print("\n--- How to read this ---")
    print("REST ok + Ticker ok + Depth fails      -> 200-depth entitlement not enabled on your account.")
    print("REST ok + Ticker fails + Depth fails   -> IP restriction or a broader account/session issue "
          "(e.g. too many concurrent WS connections already open elsewhere).")
    print("REST fails                             -> the access token itself is invalid/expired - regenerate it.")


if __name__ == "__main__":
    asyncio.run(main())
