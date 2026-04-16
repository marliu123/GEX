import asyncio
import os
from datetime import date
from decimal import Decimal

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from tastytrade import Session, DXLinkStreamer
from tastytrade.instruments import get_option_chain
from tastytrade.dxfeed import Greeks, Quote, Summary

load_dotenv()
CLIENT_SECRET = os.environ["TASTYTRADE_CLIENT_SECRET"]
REFRESH_TOKEN = os.environ["TASTYTRADE_REFRESH_TOKEN"]

app = FastAPI()


async def fetch_gex(symbol: str = "SPY", expiry_index: int = 0, strikes_each_side: int = 20):
    session = Session(CLIENT_SECRET, REFRESH_TOKEN)

    # Spot price
    async with DXLinkStreamer(session) as streamer:
        await streamer.subscribe(Quote, [symbol])
        async for quote in streamer.listen(Quote):
            spot = float(quote.bid_price + quote.ask_price) / 2
            break

    # Option chain + expiry
    chain = await get_option_chain(session, symbol)
    available = sorted(chain.keys())
    expiry = available[expiry_index]
    contracts = chain[expiry]

    # Strike window
    spot_d = Decimal(str(spot))
    all_strikes = sorted(set(c.strike_price for c in contracts))
    strikes_below = [s for s in all_strikes if s < spot_d][-strikes_each_side:]
    strikes_above = [s for s in all_strikes if s >= spot_d][:strikes_each_side]
    selected_strikes = set(strikes_below + strikes_above)
    contracts_near_spot = [c for c in contracts if c.strike_price in selected_strikes]
    streamer_symbols = [c.streamer_symbol for c in contracts_near_spot]

    # Collect OI and Greeks (wait 3s after first full snapshot for fresh values)
    oi = {}
    greeks_data = {}
    n = len(contracts_near_spot)

    async with DXLinkStreamer(session) as streamer:
        await streamer.subscribe(Summary, streamer_symbols)
        await streamer.subscribe(Greeks, streamer_symbols)

        async def collect_summary():
            async for summary in streamer.listen(Summary):
                oi[summary.event_symbol] = int(summary.open_interest or 0)

        deadline = [None]

        async def collect_greeks():
            async for greek in streamer.listen(Greeks):
                greeks_data[greek.event_symbol] = greek
                if len(greeks_data) >= n and deadline[0] is None:
                    deadline[0] = asyncio.get_event_loop().time() + 3.0
                if deadline[0] and asyncio.get_event_loop().time() >= deadline[0]:
                    break

        await asyncio.gather(
            asyncio.wait_for(collect_summary(), timeout=15),
            collect_greeks(),
            return_exceptions=True,
        )

    # Aggregate per strike
    symbol_to_contract = {c.streamer_symbol: c for c in contracts_near_spot}
    per_strike = {}
    for sym, g in greeks_data.items():
        c = symbol_to_contract[sym]
        strike = float(c.strike_price)
        per_strike.setdefault(strike, {
            "strike": strike,
            "call_gex": 0.0, "put_gex": 0.0,
            "call_oi": 0, "put_oi": 0,
            "call_gamma": 0.0, "put_gamma": 0.0,
        })
        open_interest = oi.get(sym, 0)
        gex = float(g.gamma) * open_interest * (spot ** 2)
        if c.option_type.value == 'C':
            per_strike[strike]["call_gex"] = gex
            per_strike[strike]["call_oi"] = open_interest
            per_strike[strike]["call_gamma"] = float(g.gamma)
        else:
            per_strike[strike]["put_gex"] = -gex
            per_strike[strike]["put_oi"] = open_interest
            per_strike[strike]["put_gamma"] = float(g.gamma)

    strikes = sorted(per_strike.values(), key=lambda x: x["strike"])
    for s in strikes:
        s["net_gex"] = s["call_gex"] + s["put_gex"]

    total_call_gex = sum(s["call_gex"] for s in strikes)
    total_put_gex = sum(s["put_gex"] for s in strikes)
    net_gex = total_call_gex + total_put_gex
    call_wall = max(strikes, key=lambda x: x["net_gex"])["strike"] if strikes else None
    put_wall = min(strikes, key=lambda x: x["net_gex"])["strike"] if strikes else None

    # Zero gamma: linear interpolation between sign flips
    zero_gamma = None
    for i in range(1, len(strikes)):
        a, b = strikes[i - 1], strikes[i]
        if (a["net_gex"] <= 0 <= b["net_gex"]) or (a["net_gex"] >= 0 >= b["net_gex"]):
            if b["net_gex"] != a["net_gex"]:
                frac = -a["net_gex"] / (b["net_gex"] - a["net_gex"])
                zero_gamma = a["strike"] + frac * (b["strike"] - a["strike"])
            break

    return {
        "symbol": symbol,
        "spot": spot,
        "expiry": str(expiry),
        "available_expiries": [str(d) for d in available[:10]],
        "strikes": strikes,
        "summary": {
            "total_call_gex": total_call_gex,
            "total_put_gex": total_put_gex,
            "net_gex": net_gex,
            "call_wall": call_wall,
            "put_wall": put_wall,
            "zero_gamma": zero_gamma,
        },
    }


@app.get("/api/gex")
async def get_gex(symbol: str = "SPY", expiry_index: int = 0, strikes_each_side: int = 20):
    return await fetch_gex(symbol, expiry_index, strikes_each_side)


app.mount("/", StaticFiles(directory="static", html=True), name="static")
