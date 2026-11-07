"""Opt-in, fail-open context from the Crypto-Explosion-Research-Engine (research only, NOT a trading signal).

Set OUR_RECS_URL to a JSON list of {"symbol","rank","p_up10_24h","shariah_listed_v6"}. If unset/unreachable, returns {} and V6 behaves exactly as before.
Intended use: annotate V6 alerts with "also in our daily top-N" for later unified evaluation. It never changes V6 thresholds or signals.
"""
import json, os, urllib.request

def load_our_context(timeout=10):
    url = os.environ.get("OUR_RECS_URL", "").strip()
    if not url:
        return {}
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return {x["symbol"]: x for x in json.load(r)}
    except Exception:
        return {}

def annotate(symbol, ctx):
    x = ctx.get(symbol)
    return "" if not x else f" | engine rank #{x.get('rank')} p10/24h={x.get('p_up10_24h')}"
