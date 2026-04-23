import asyncio
import os
from tastytrade import Session
from tastytrade.instruments import get_option_chain
from tastytrade.dxfeed import Greeks, Quote, Summary
from tastytrade import DXLinkStreamer
from datetime import date
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()
CLIENT_SECRET = os.environ["TASTYTRADE_CLIENT_SECRET"]
REFRESH_TOKEN = os.environ["TASTYTRADE_REFRESH_TOKEN"]


async def main():
    session = Session(CLIENT_SECRET, REFRESH_TOKEN)
    print("✅ Connected")

    # Get current SPY spot price
    async with DXLinkStreamer(session) as streamer:
        await streamer.subscribe(Quote, ['SPX'])
        async for quote in streamer.listen(Quote):
            spot = float(quote.bid_price + quote.ask_price) / 2
            print(f"✅ SPX spot: {spot:.2f}")
            break

    # Pull the SPY option chain
    chain = await get_option_chain(session, 'SPX')

    # 0 = soonest expiry (today/0DTE), 1 = next expiry, 2 = one after, etc.
    EXPIRY_INDEX = 1

    available = sorted(chain.keys())
    print(f"Available expiries: {available[:5]}")
    expiry = available[EXPIRY_INDEX]
    contracts = chain[expiry]
    print(f"✅ Using expiry: {expiry} ({len(contracts)} contracts)")

    # Pick 20 strikes below and 20 above spot (40 unique strikes, both calls and puts)
    spot_d = Decimal(str(spot))
    all_strikes = sorted(set(c.strike_price for c in contracts))
    strikes_below = [s for s in all_strikes if s < spot_d][-20:]
    strikes_above = [s for s in all_strikes if s >= spot_d][:20]
    selected_strikes = set(strikes_below + strikes_above)
    contracts_near_spot = [c for c in contracts if c.strike_price in selected_strikes]
    streamer_symbols = [c.streamer_symbol for c in contracts_near_spot]
    print(f"✅ Watching {len(selected_strikes)} strikes ({len(contracts_near_spot)} contracts)\n")

    # Collect OI from Summary and Greeks for the 20 strikes
    oi = {}
    greeks_data = {}

    async with DXLinkStreamer(session) as streamer:
        await streamer.subscribe(Summary, streamer_symbols)
        await streamer.subscribe(Greeks, streamer_symbols)

        n = len(contracts_near_spot)

        async for summary in streamer.listen(Summary):
            oi[summary.event_symbol] = int(summary.open_interest or 0)
            if len(oi) >= n:
                break

        async for greek in streamer.listen(Greeks):
            greeks_data[greek.event_symbol] = greek
            if len(greeks_data) >= n:
                break

    # Per-contract detail table
    symbol_to_contract = {c.streamer_symbol: c for c in contracts_near_spot}
    print(f"\n{'Symbol':35} {'Strike':>8}  {'Type':>4}  {'OI':>10}  {'Gamma':>10}  {'Delta':>8}")
    print("-" * 85)
    for sym in sorted(greeks_data, key=lambda s: (symbol_to_contract[s].strike_price, symbol_to_contract[s].option_type)):
        c = symbol_to_contract[sym]
        g = greeks_data[sym]
        print(f"{sym:35} {c.strike_price:>8}  {c.option_type:>4}  {oi.get(sym, 0):>10,}  {g.gamma:>12.8f}  {g.delta:>8.4f}")

    # Compute net GEX and net DEX per strike (call + put combined)
    # GEX = sign × gamma × OI × spot²     (calls +, puts -)
    # DEX = delta × OI × 100 × spot        (put delta already negative, so add)
    gex_by_strike = {}
    dex_by_strike = {}
    for sym, g in greeks_data.items():
        c = symbol_to_contract[sym]
        strike = float(c.strike_price)
        open_interest = oi.get(sym, 0)
        sign = 1 if c.option_type.value == 'C' else -1
        gex = sign * float(g.gamma) * open_interest * (spot ** 2)
        dex = float(g.delta) * open_interest * 100 * spot
        gex_by_strike[strike] = gex_by_strike.get(strike, 0) + gex
        dex_by_strike[strike] = dex_by_strike.get(strike, 0) + dex

    print(f"\n{'Strike':>8}  {'Net GEX ($)':>18}  {'Net DEX ($)':>18}")
    print("-" * 52)
    total_gex = 0
    total_dex = 0
    for strike in sorted(gex_by_strike):
        gex = gex_by_strike[strike]
        dex = dex_by_strike[strike]
        total_gex += gex
        total_dex += dex
        print(f"{strike:>8.2f}  {gex:>18,.0f}  {dex:>18,.0f}")
    print("-" * 52)
    print(f"{'TOTAL':>8}  {total_gex:>18,.0f}  {total_dex:>18,.0f}")

asyncio.run(main())
