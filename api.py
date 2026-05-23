import asyncio
import json
import os
from datetime import date
from decimal import Decimal

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

from tastytrade import Session, DXLinkStreamer
from tastytrade.instruments import get_option_chain
from tastytrade.dxfeed import Greeks, Quote, Summary

from claude_agent_sdk import query, ClaudeAgentOptions, AssistantMessage, TextBlock

load_dotenv()
CLIENT_SECRET = os.environ["TASTYTRADE_CLIENT_SECRET"]
REFRESH_TOKEN = os.environ["TASTYTRADE_REFRESH_TOKEN"]

app = FastAPI()

# Caches
_session = None
_chain_cache = {}   # symbol -> (chain, timestamp)
CHAIN_TTL = 300     # 5 min
_session_lock = asyncio.Lock()
_chain_lock = asyncio.Lock()


async def get_session():
    global _session
    async with _session_lock:
        if _session is None:
            _session = Session(CLIENT_SECRET, REFRESH_TOKEN)
        return _session


async def get_chain(session, symbol):
    import time
    async with _chain_lock:
        cached = _chain_cache.get(symbol)
        if cached and (time.time() - cached[1] < CHAIN_TTL):
            return cached[0]
        chain = await get_option_chain(session, symbol)
        _chain_cache[symbol] = (chain, time.time())
        return chain


async def fetch_gex(symbol: str = "SPY", expiry_index: int = 0, strikes_each_side: int = 20):
    session = await get_session()
    chain = await get_chain(session, symbol)

    available = sorted(chain.keys())
    expiry = available[expiry_index]
    contracts = chain[expiry]

    # Spot price
    async with DXLinkStreamer(session) as streamer:
        await streamer.subscribe(Quote, [symbol])
        async for quote in streamer.listen(Quote):
            spot = float(quote.bid_price + quote.ask_price) / 2
            break

    # Strike window
    spot_d = Decimal(str(spot))
    all_strikes = sorted(set(c.strike_price for c in contracts))
    strikes_below = [s for s in all_strikes if s < spot_d][-strikes_each_side:]
    strikes_above = [s for s in all_strikes if s >= spot_d][:strikes_each_side]
    selected_strikes = set(strikes_below + strikes_above)
    contracts_near_spot = [c for c in contracts if c.strike_price in selected_strikes]
    streamer_symbols = [c.streamer_symbol for c in contracts_near_spot]
    n = len(contracts_near_spot)

    oi = {}
    greeks_data = {}

    async with DXLinkStreamer(session) as streamer:
        await streamer.subscribe(Summary, streamer_symbols)
        await streamer.subscribe(Greeks, streamer_symbols)

        async for summary in streamer.listen(Summary):
            oi[summary.event_symbol] = int(summary.open_interest or 0)
            if len(oi) >= n:
                break

        async for greek in streamer.listen(Greeks):
            greeks_data[greek.event_symbol] = greek
            if len(greeks_data) >= n:
                break

    # Aggregate per strike
    symbol_to_contract = {c.streamer_symbol: c for c in contracts_near_spot}
    per_strike = {}
    for sym, g in greeks_data.items():
        c = symbol_to_contract[sym]
        strike = float(c.strike_price)
        per_strike.setdefault(strike, {
            "strike": strike,
            "call_gex": 0.0, "put_gex": 0.0,
            "call_dex": 0.0, "put_dex": 0.0,
            "call_oi": 0, "put_oi": 0,
            "call_gamma": 0.0, "put_gamma": 0.0,
            "call_delta": 0.0, "put_delta": 0.0,
        })
        open_interest = oi.get(sym, 0)
        gex = float(g.gamma) * open_interest * (spot ** 2)
        # DEX: delta × OI × 100 × spot. Put delta is already negative.
        dex = float(g.delta) * open_interest * 100 * spot
        if c.option_type.value == 'C':
            per_strike[strike]["call_gex"] += gex
            per_strike[strike]["call_dex"] += dex
            per_strike[strike]["call_oi"] += open_interest
            per_strike[strike]["call_gamma"] = float(g.gamma)
            per_strike[strike]["call_delta"] = float(g.delta)
        else:
            per_strike[strike]["put_gex"] += -gex
            per_strike[strike]["put_dex"] += dex  # delta already negative, just add
            per_strike[strike]["put_oi"] += open_interest
            per_strike[strike]["put_gamma"] = float(g.gamma)
            per_strike[strike]["put_delta"] = float(g.delta)

    strikes = sorted(per_strike.values(), key=lambda x: x["strike"])
    for s in strikes:
        s["net_gex"] = s["call_gex"] + s["put_gex"]
        s["net_dex"] = s["call_dex"] + s["put_dex"]

    total_call_gex = sum(s["call_gex"] for s in strikes)
    total_put_gex = sum(s["put_gex"] for s in strikes)
    net_gex = total_call_gex + total_put_gex
    total_call_dex = sum(s["call_dex"] for s in strikes)
    total_put_dex = sum(s["put_dex"] for s in strikes)
    net_dex = total_call_dex + total_put_dex

    # Walls: largest call-only / put-only contribution (SpotGamma convention)
    call_wall = max(strikes, key=lambda x: x["call_gex"])["strike"] if strikes else None
    put_wall = min(strikes, key=lambda x: x["put_gex"])["strike"] if strikes else None
    call_wall_dex = max(strikes, key=lambda x: x["call_dex"])["strike"] if strikes else None
    put_wall_dex = min(strikes, key=lambda x: x["put_dex"])["strike"] if strikes else None

    # Zero gamma: cumulative GEX sign flip (SpotGamma convention)
    zero_gamma = None
    cum = 0.0
    cum_points = []
    for s in strikes:
        cum += s["net_gex"]
        cum_points.append((s["strike"], cum))
    for i in range(1, len(cum_points)):
        s1, c1 = cum_points[i - 1]
        s2, c2 = cum_points[i]
        if (c1 <= 0 <= c2) or (c1 >= 0 >= c2):
            if c2 != c1:
                frac = -c1 / (c2 - c1)
                zero_gamma = s1 + frac * (s2 - s1)
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
            "total_call_dex": total_call_dex,
            "total_put_dex": total_put_dex,
            "net_dex": net_dex,
            "call_wall_dex": call_wall_dex,
            "put_wall_dex": put_wall_dex,
        },
    }


@app.get("/api/gex")
async def get_gex(symbol: str = "SPY", expiry_index: int = 0, strikes_each_side: int = 20):
    return await fetch_gex(symbol, expiry_index, strikes_each_side)


ANALYZE_SYSTEM_PROMPT = """You are an options flow analyst. You are given a JSON snapshot of gamma exposure (GEX) and delta exposure (DEX) per strike for a given symbol and expiry. Write a concise, actionable interpretation for a discretionary trader.

Cover, in this order:
1. **Dealer positioning regime** — Is net GEX positive (dealers long gamma, vol-suppressing, mean-reverting) or negative (dealers short gamma, vol-amplifying, trend-following)? What does this imply for intraday behavior?
2. **Key levels** — Call wall (upside magnet/resistance), put wall (downside support), zero-gamma flip (regime boundary). Reference actual strike numbers and distance from spot in %.
3. **Spot context** — Where is spot sitting relative to walls and zero-gamma? Is price pinned near a large-OI strike?
4. **Notable concentrations** — Any single strike dominating GEX or DEX? Any unusual put/call skew?
5. **Trade-relevant takeaway** — 1-2 sentences: what should a trader watch for or avoid today?

Be specific with numbers. Skip throat-clearing. Use markdown headings and bullet points. Do NOT hedge with "I cannot provide financial advice" — the user is a trader consuming their own data."""


def _format_gex_payload(payload: dict) -> str:
    summary = payload.get("summary", {})
    strikes = payload.get("strikes", [])
    trimmed = [
        {
            "strike": s["strike"],
            "call_gex": round(s["call_gex"], 2),
            "put_gex": round(s["put_gex"], 2),
            "net_gex": round(s["net_gex"], 2),
            "call_dex": round(s["call_dex"], 2),
            "put_dex": round(s["put_dex"], 2),
            "call_oi": s["call_oi"],
            "put_oi": s["put_oi"],
        }
        for s in strikes
    ]
    return json.dumps(
        {
            "symbol": payload.get("symbol"),
            "spot": payload.get("spot"),
            "expiry": payload.get("expiry"),
            "summary": summary,
            "strikes": trimmed,
        },
        indent=2,
    )


@app.post("/api/analyze")
async def analyze(request: Request):
    payload = await request.json()

    if not os.environ.get("CLAUDE_CODE_OAUTH_TOKEN") and not os.environ.get("ANTHROPIC_API_KEY"):
        return StreamingResponse(
            iter([f"data: {json.dumps({'error': 'CLAUDE_CODE_OAUTH_TOKEN is not set on the server. Run `claude setup-token` locally and set it as a Fly.io secret.'})}\n\n"]),
            media_type="text/event-stream",
        )

    prompt = (
        f"Analyze this GEX snapshot and return a markdown-formatted interpretation:\n\n"
        f"```json\n{_format_gex_payload(payload)}\n```"
    )
    options = ClaudeAgentOptions(
        system_prompt=ANALYZE_SYSTEM_PROMPT,
        allowed_tools=[],
        permission_mode="default",
    )

    async def event_stream():
        try:
            async for msg in query(prompt=prompt, options=options):
                if isinstance(msg, AssistantMessage):
                    for block in msg.content:
                        if isinstance(block, TextBlock) and block.text:
                            yield f"data: {json.dumps({'text': block.text})}\n\n"
            yield f"data: {json.dumps({'done': True})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


app.mount("/", StaticFiles(directory="static", html=True), name="static")
