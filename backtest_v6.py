#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backtest for Binance Early-Pump Scanner V6.

It re-uses the REAL analyze()/gates/risk code from binance_early_pump_v6.py (it is loaded
from source, nothing is re-implemented), replays it over historical 5m candles, simulates
every signal with SL/TP1/timeout (fees + slippage), compares against random entries on the
same coins/months, and prints win-rate / expectancy with a confidence interval.

Quick start (put this file next to binance_early_pump_v6.py and entry_quality_v6.py):

    pip install numpy pandas requests
    python backtest_v6.py                       # last 12 full months, all USDT spot pairs
    python backtest_v6.py --halal               # only the halal list in shariah_compliant_bases_v6.json
    python backtest_v6.py --months 2026-01:2026-09 --max-symbols 60
    python backtest_v6.py --selftest            # machinery test on synthetic data (no network)

Data: monthly 5m kline zips from https://data.binance.vision (cached in ./bt_data/zips).
If you cannot download, drop files named  SYMBOL-5m-YYYY-MM.zip  into ./bt_data/zips  and run
with --no-download.

What is NOT simulated (needs data that is not archived): order-book imbalance, open interest,
funding rate (so SHORT SQUEEZE lane and the "fast explosive" lane are off), the 1-minute
entry-confirmation filter, cross-exchange check, Telegram latency (use --delay-candles).
"""
import argparse
import csv
import datetime as dt
import io
import json
import logging
import os
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
import zipfile
import zlib
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd

BAR_MS = 300_000
WINDOW = 180            # klines the live scanner requests (KLINES_LIMIT)
DAY_BARS = 288
ARCHIVE_URL = "https://data.binance.vision/data/spot/monthly/klines/{s}/5m/{s}-5m-{m}.zip"
ARCHIVE_LIST = ("https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
                "?delimiter=/&prefix=data/spot/monthly/klines/")
LEVERAGED_SUFFIXES = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")


# ----------------------------------------------------------------------------------------
# Loading the real scanner code
# ----------------------------------------------------------------------------------------
def load_scanner(path):
    path = os.path.abspath(path)
    sys.path.insert(0, os.path.dirname(path))
    with open(path, encoding="utf-8") as f:
        src = f.read()
    cut = src.find("\nload_auto_thresholds()\n")
    if cut < 0:
        raise SystemExit("Could not find the scanner's 'main' section marker; is this the V6 file?")
    src = src[:cut].replace('logging.FileHandler("scanner_v6.log", encoding="utf-8")',
                            "logging.NullHandler()")
    ns = {"__name__": "scanner_core", "__file__": path}
    logging.disable(logging.CRITICAL)          # silence the scanner's import-time Telegram warning
    try:
        exec(compile(src, path, "exec"), ns)
    finally:
        logging.disable(logging.NOTSET)
    logging.getLogger("scanner").setLevel(logging.ERROR)
    return ns


# ----------------------------------------------------------------------------------------
# Data
# ----------------------------------------------------------------------------------------
def month_range(spec):
    a, b = spec.split(":")
    y, m = map(int, a.split("-"))
    y2, m2 = map(int, b.split("-"))
    out = []
    while (y, m) <= (y2, m2):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def default_months(n=12):
    today = dt.datetime.now(dt.timezone.utc)
    y, m = today.year, today.month
    m -= 1
    if m == 0:
        y, m = y - 1, 12
    end = f"{y:04d}-{m:02d}"
    y0, m0 = y, m - (n - 1)
    while m0 <= 0:
        y0, m0 = y0 - 1, m0 + 12
    return month_range(f"{y0:04d}-{m0:02d}:{end}")


def parse_zip(zp):
    try:
        df = pd.read_csv(zp, header=None, usecols=[0, 1, 2, 3, 4, 7, 10])
    except Exception:
        return None
    df = df.apply(pd.to_numeric, errors="coerce").dropna()
    if df.empty:
        return None
    a = df.to_numpy(dtype="float64")
    if a[:, 0].max() > 1e14:          # spot archives from 2025 on use microseconds
        a[:, 0] = np.floor(a[:, 0] / 1000.0)
    return a                          # cols: t, o, h, l, c, quote_vol, taker_buy_quote_vol


def download_month(sym, ym, zdir):
    import requests
    zp = os.path.join(zdir, f"{sym}-5m-{ym}.zip")
    miss = zp + ".missing"
    if os.path.exists(zp):
        return zp
    if os.path.exists(miss):
        return None
    url = ARCHIVE_URL.format(s=sym, m=ym)
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=40)
            if r.status_code == 404:
                open(miss, "w").close()
                return None
            r.raise_for_status()
            tmp = zp + ".part"
            with open(tmp, "wb") as f:
                f.write(r.content)
            os.replace(tmp, zp)
            return zp
        except Exception:
            time.sleep(1.5 * (attempt + 1))
    return None


def load_symbol(sym, months, data_dir, download=True):
    zdir = os.path.join(data_dir, "zips")
    os.makedirs(zdir, exist_ok=True)
    parts = []
    for ym in months:
        zp = os.path.join(zdir, f"{sym}-5m-{ym}.zip")
        if not os.path.exists(zp):
            if not download:
                continue
            zp = download_month(sym, ym, zdir)
            if not zp:
                continue
        a = parse_zip(zp)
        if a is not None:
            parts.append(a)
    if not parts:
        return None
    A = np.vstack(parts)
    A = A[np.argsort(A[:, 0], kind="stable")]
    _, idx = np.unique(A[:, 0], return_index=True)
    return A[idx]


def discover_symbols(source, data_dir):
    """USDT spot pairs. 'archive' includes delisted coins (avoids survivorship bias)."""
    import requests
    syms = set()
    if source == "archive":
        marker = ""
        for _ in range(60):
            r = requests.get(ARCHIVE_LIST + (f"&marker={marker}" if marker else ""), timeout=40)
            r.raise_for_status()
            root = ET.fromstring(r.content)
            ns = {"s": root.tag.split("}")[0].strip("{")}
            for p in root.findall(".//s:CommonPrefixes/s:Prefix", ns):
                name = p.text.rstrip("/").split("/")[-1]
                syms.add(name)
            trunc = root.findtext("s:IsTruncated", default="false", namespaces=ns)
            marker = root.findtext("s:NextMarker", default="", namespaces=ns)
            if trunc != "true" or not marker:
                break
    else:
        r = requests.get("https://data-api.binance.vision/api/v3/exchangeInfo", timeout=40)
        r.raise_for_status()
        for s in r.json()["symbols"]:
            if s.get("status") == "TRADING" and s.get("isSpotTradingAllowed"):
                syms.add(s["symbol"])
    return sorted(s for s in syms if s.endswith("USDT") and not s.endswith(LEVERAGED_SUFFIXES))


# ----------------------------------------------------------------------------------------
# Simulation
# ----------------------------------------------------------------------------------------
def simulate(o, h, l, c, e, sl_pct, tp_pct, max_bars):
    """Enter at open of bar e. SL is checked before TP1 inside the same bar (conservative)."""
    entry = o[e]
    sl = entry * (1 - sl_pct / 100.0)
    tp = entry * (1 + tp_pct / 100.0)
    last = min(e + max_bars - 1, len(o) - 1)
    for j in range(e, last + 1):
        if j > e and o[j] <= sl:
            return "SL", j - e + 1, o[j]
        if l[j] <= sl:
            return "SL", j - e + 1, sl
        if h[j] >= tp:
            return "TP1", j - e + 1, tp
    return "TIMEOUT", last - e + 1, c[last]


def net_return(entry, exit_price, sl_pct, cost_pct):
    gross = (exit_price / entry - 1.0) * 100.0
    net = gross - cost_pct
    return gross, net, net / sl_pct


# ----------------------------------------------------------------------------------------
# Worker
# ----------------------------------------------------------------------------------------
_W = {}


def _init_worker(scanner_path, cfg):
    SC = load_scanner(scanner_path)
    _W["SC"] = SC
    _W["cfg"] = cfg
    btc = None
    if cfg["btc_guard"]:
        btc = load_symbol("BTCUSDT", cfg["months"], cfg["data_dir"], cfg["download"])
    if btc is not None:
        c = btc[:, 4]
        n = len(c)
        bad = np.zeros(n, dtype=bool)
        for j in range(13, n):
            m15 = (c[j - 1] / c[j - 4] - 1) * 100
            m1h = (c[j - 1] / c[j - 13] - 1) * 100
            bad[j] = (m15 <= SC["BTC_CRASH_THRESHOLD_15M"]) or (m1h <= SC["BTC_CRASH_THRESHOLD_1H"])
        _W["btc_t"] = btc[:, 0]
        _W["btc_bad"] = bad
    else:
        _W["btc_t"] = None
        _W["btc_bad"] = None


def process_symbol(sym):
    SC, cfg = _W["SC"], _W["cfg"]
    A = load_symbol(sym, cfg["months"], cfg["data_dir"], cfg["download"])
    if A is None or len(A) < WINDOW + DAY_BARS + 60:
        return sym, [], [], 0
    t, o, h, l, c, qv = A[:, 0], A[:, 1], A[:, 2], A[:, 3], A[:, 4], A[:, 5]
    n = len(t)
    max_bars = int(SC["TRADE_TIMEOUT_MIN"] // 5)
    cost = 2.0 * (cfg["fee"] + cfg["slip"])
    delays = cfg["delays"]
    prim = cfg["delay"]
    dmax = max(delays)

    cs = np.concatenate([[0.0], np.cumsum(qv)])
    idx = np.arange(n)
    qv24 = np.full(n, np.nan)
    qv24[DAY_BARS:] = cs[DAY_BARS:n] - cs[0:n - DAY_BARS]
    chg24 = np.full(n, np.nan)
    chg24[DAY_BARS + 1:] = (c[DAY_BARS:n - 1] / c[0:n - DAY_BARS - 1] - 1) * 100
    g15 = np.full(n, -999.0)
    g15[4:] = (c[3:n - 1] / c[0:n - 4] - 1) * 100
    contiguous = np.zeros(n, dtype=bool)
    contiguous[WINDOW - 1:] = (t[WINDOW - 1:] - t[:n - WINDOW + 1]) == (WINDOW - 1) * BAR_MS

    thr15 = min(SC["EXPLOSIVE_MIN_15M_MOVE"], SC["FINAL_MIN_15M_MOVE"],
                SC["PERSISTENT_MIN_15M_MOVE"], SC["REVERSAL_MIN_15M_MOVE"])
    in_range = (idx >= DAY_BARS + 1) & (idx <= n - 1 - dmax - max_bars)
    base_ok = in_range & contiguous & (qv24 >= SC["MIN_24H_QUOTE_VOLUME"])
    scan_idx = np.where(base_ok & (g15 >= thr15))[0]

    M = np.column_stack([t, o, h, l, c, np.zeros(n), t + BAR_MS - 1, qv, np.zeros(n), np.zeros(n),
                         A[:, 6]]).tolist()
    ol, hl, ll, cl = o.tolist(), h.tolist(), l.tolist(), c.tolist()

    cur = {}
    SC["get_json"] = lambda path, params=None, retries=3: cur["rows"]

    btc_t, btc_bad = _W.get("btc_t"), _W.get("btc_bad")

    def btc_blocked(i):
        if btc_t is None:
            return False
        j = int(np.searchsorted(btc_t, t[i]))
        if j >= len(btc_t) or btc_t[j] != t[i]:
            return False
        return bool(btc_bad[j])

    trades = []
    cand = None
    block_until = -1
    expiry_bars = SC["CANDIDATE_EXPIRY_MINUTES"] / 5.0
    for i in scan_idx:
        i = int(i)
        if i < block_until or btc_blocked(i):
            continue
        cur["rows"] = M[i - WINDOW + 1:i + 1]
        x = SC["analyze"](sym, {"change": float(chg24[i]), "quote_volume": float(qv24[i]),
                                "price": cl[i - 1]})
        if not x or x.get("_rejected") or x["score"] < SC["WATCH_SCORE"]:
            continue
        if not SC["final_candidate_eligible"](x):
            continue
        if x["score"] < SC["MIN_CANDIDATE_SCORE"]:
            continue
        if cand is None or (i - cand["last_i"]) > expiry_bars:
            cand = {"count": 1, "last_i": i, "first": x, "best": x["score"]}
            continue
        cand["count"] += 1
        cand["last_i"] = i
        cand["best"] = max(cand["best"], x["score"])
        first = cand["first"]
        confirmed = (x.get("continuation_quality", False) and x["5m"] > -0.3
                     and x["15m"] >= first["15m"] - 1.0
                     and x["volume_1h"] >= first["volume_1h"] * 0.70)
        if not confirmed:
            continue

        e0 = i + prim
        risk = SC["compute_risk_levels"](ol[e0], x.get("atr_pct"), x.get("rsi14"), x["liquidity"])
        if not risk:
            cand = None
            continue
        sl_pct, tp_pct = risk["sl_pct"], risk["tp1_pct"]
        rec = {
            "symbol": sym, "signal_ts": int(t[i]), "month": pd.Timestamp(int(t[i]), unit="ms").strftime("%Y-%m"),
            "lane": SC["final_lane"](x), "setup": x["setup"], "score": x["score"],
            "sl_pct": round(sl_pct, 3), "tp1_pct": round(tp_pct, 3), "tp1_rr": risk["tp1_rr"],
            "g15": round(x["15m"], 2), "vol15": round(x["volume_15m"], 2), "rsi": x.get("rsi14"),
            "liquidity": x["liquidity"], "bars_to_signal": cand["count"],
        }
        for d in delays:
            e = i + d
            outcome, bars, ex = simulate(ol, hl, ll, cl, e, sl_pct, tp_pct, max_bars)
            gross, net, r = net_return(ol[e], ex, sl_pct, cost)
            rec[f"outcome_d{d}"] = outcome
            rec[f"bars_d{d}"] = bars
            rec[f"gross_d{d}"] = round(gross, 4)
            rec[f"net_d{d}"] = round(net, 4)
            rec[f"r_d{d}"] = round(r, 4)
        trades.append(rec)
        cand = None
        block_until = i + DAY_BARS          # active_tracks keeps the symbol blocked ~24h

    # Random-entry baseline: same coin, same month, same SL/TP distances as each signal.
    rng = np.random.default_rng(zlib.crc32(sym.encode()) & 0xFFFFFFFF)
    month_of = pd.to_datetime(t, unit="ms").strftime("%Y-%m").to_numpy()
    base = []
    for k, tr in enumerate(trades):
        pool = np.where(base_ok & (month_of == tr["month"]))[0]
        if len(pool) == 0:
            continue
        for bi in rng.choice(pool, size=min(cfg["baseline_k"], len(pool)), replace=False):
            e = int(bi) + prim
            outcome, bars, ex = simulate(ol, hl, ll, cl, e, tr["sl_pct"], tr["tp1_pct"], max_bars)
            gross, net, r = net_return(ol[e], ex, tr["sl_pct"], cost)
            base.append({"symbol": sym, "month": tr["month"], "signal_ts": int(t[int(bi)]),
                         "outcome": outcome, "net": net, "r": r})
    return sym, trades, base, int(len(scan_idx))


# ----------------------------------------------------------------------------------------
# Statistics / report
# ----------------------------------------------------------------------------------------
def boot_ci(df, col, n_boot=4000, seed=1):
    """95% CI of mean(col), resampling whole UTC days (signals on the same day are correlated)."""
    if len(df) < 2:
        return (np.nan, np.nan)
    day = pd.to_datetime(df["signal_ts"], unit="ms").dt.strftime("%Y-%m-%d")
    g = df.groupby(day.values)[col].agg(["sum", "count"])
    sums, cnts = g["sum"].to_numpy(), g["count"].to_numpy()
    rng = np.random.default_rng(seed)
    pick = rng.integers(0, len(g), size=(n_boot, len(g)))
    means = sums[pick].sum(axis=1) / cnts[pick].sum(axis=1)
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def group_summary(df, key, d):
    return pd.DataFrame({k: summarize(g, d) for k, g in df.groupby(key)}).T


def summarize(df, d):
    oc, r, net, gross = df[f"outcome_d{d}"], df[f"r_d{d}"], df[f"net_d{d}"], df[f"gross_d{d}"]
    return pd.Series({
        "n": len(df),
        "win_TP1_%": round((oc == "TP1").mean() * 100, 1),
        "SL_%": round((oc == "SL").mean() * 100, 1),
        "timeout_%": round((oc == "TIMEOUT").mean() * 100, 1),
        "avg_gross_%": round(gross.mean(), 3),
        "avg_net_%": round(net.mean(), 3),
        "exp_R_net": round(r.mean(), 3),
        "breakeven_win_%": round(100.0 / (1.0 + df["tp1_rr"].mean()), 1),
    })


def report(trades, base, cfg, out_dir, months, n_symbols):
    d = cfg["delay"]
    lines = []

    def P(s=""):
        lines.append(s)
        print(s)

    P("=" * 78)
    P(f"BACKTEST  months={months[0]}..{months[-1]}  symbols_with_data={n_symbols}  "
      f"fee={cfg['fee']}%/side slip={cfg['slip']}%/side  entry_delay={d} bar(s)")
    P("=" * 78)
    if trades.empty:
        P("No signals were produced in this period/universe.")
        return "\n".join(lines)
    n = len(trades)
    P(f"Signals: {n}  (~{n / max(1, len(months)):.1f} per month, "
      f"{trades['symbol'].nunique()} distinct coins)")
    P()
    P("--- Overall -------------------------------------------------------------")
    P(summarize(trades, d).to_string())
    lo, hi = boot_ci(trades, f"r_d{d}")
    P(f"expectancy 95% CI (day-clustered bootstrap): [{lo:+.3f}R , {hi:+.3f}R]")
    if base is not None and len(base):
        b_r = base["r"].mean()
        b_lo, b_hi = boot_ci(base.assign(r_d=base["r"]), "r_d")
        P(f"RANDOM-ENTRY baseline (same coin+month+SL/TP): exp {b_r:+.3f}R  "
          f"CI [{b_lo:+.3f}, {b_hi:+.3f}]  win {(base['outcome'] == 'TP1').mean() * 100:.1f}%  n={len(base)}")
        P(f"EDGE vs random = {trades[f'r_d{d}'].mean() - b_r:+.3f}R per trade")
    P()
    P("--- Sensitivity to entry delay (latency between signal and your fill) -----")
    for dd in cfg["delays"]:
        P(f"delay {dd} bar(s) ({dd * 5} min): exp {trades[f'r_d{dd}'].mean():+.3f}R | "
          f"avg net {trades[f'net_d{dd}'].mean():+.3f}% | win {(trades[f'outcome_d{dd}'] == 'TP1').mean() * 100:.1f}%")
    P()
    P("--- By month (NOT a way to pick months: that would be cherry-picking) -----")
    bym = group_summary(trades, "month", d)
    if base is not None and len(base):
        bym["random_R"] = base.groupby("month")["r"].mean().round(3)
        bym["n"] = bym["n"].astype(int)
    P(bym.to_string())
    P()
    P("--- By lane -------------------------------------------------------------")
    P(group_summary(trades.assign(lane=trades["lane"].fillna("?")), "lane", d).to_string())
    P()
    P("--- By setup ------------------------------------------------------------")
    P(group_summary(trades, "setup", d).to_string())
    P()
    P("--- Out-of-sample check: first half vs second half of the period ---------")
    cut = trades["signal_ts"].median()
    for name, part in (("first half", trades[trades["signal_ts"] <= cut]),
                       ("second half", trades[trades["signal_ts"] > cut])):
        if len(part):
            P(f"{name:11}: n={len(part):4}  win={(part[f'outcome_d{d}'] == 'TP1').mean() * 100:5.1f}%  "
              f"exp={part[f'r_d{d}'].mean():+.3f}R")
    P()
    P("--- Verdict (mechanical, not advice) ------------------------------------")
    mean_r = trades[f"r_d{d}"].mean()
    if n < 100:
        P(f"* Only {n} signals: too few to conclude anything (aim for 100+).")
    if lo > 0:
        P("* Expectancy is positive and the 95% CI is above zero.")
    elif hi < 0:
        P("* Expectancy is negative and the 95% CI is below zero: this setup lost money after costs.")
    else:
        P(f"* Expectancy {mean_r:+.3f}R but the CI includes zero: no proof of an edge.")
    if base is not None and len(base) and mean_r <= base["r"].mean():
        P("* It does not beat random entries on the same coins: the filters add nothing here.")
    P("* Survivorship / data limits: see header of this file. Treat any positive result as a hypothesis")
    P("  to confirm with forward paper trading, never as proof.")
    text = "\n".join(lines)
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "summary.txt"), "w", encoding="utf-8") as f:
        f.write(text)
    trades.to_csv(os.path.join(out_dir, "trades.csv"), index=False)
    if base is not None:
        base.to_csv(os.path.join(out_dir, "baseline.csv"), index=False)
    return text


# ----------------------------------------------------------------------------------------
# Self-test data (synthetic: only checks the machinery, results are meaningless)
# ----------------------------------------------------------------------------------------
def make_synthetic(data_dir, months, nsym=30, seed=7):
    rng = np.random.default_rng(seed)
    zdir = os.path.join(data_dir, "zips")
    os.makedirs(zdir, exist_ok=True)
    edges = [dt.datetime(int(m[:4]), int(m[5:]), 1, tzinfo=dt.timezone.utc) for m in months]
    y, mo = int(months[-1][:4]), int(months[-1][5:])
    edges.append(dt.datetime(y + (mo == 12), mo % 12 + 1, 1, tzinfo=dt.timezone.utc))
    ms = [int(e.timestamp() * 1000) for e in edges]
    total = (ms[-1] - ms[0]) // BAR_MS
    syms = ["BTCUSDT"] + [f"SYN{k:02d}USDT" for k in range(nsym)]
    for sym in syms:
        is_btc = sym == "BTCUSDT"
        ret = rng.normal(0, 0.0025 if is_btc else 0.006, total)
        qv = rng.uniform(30e3, 150e3) * rng.lognormal(0, 0.5, total)
        tb = np.clip(rng.normal(0.5, 0.06, total), 0.2, 0.8)
        if not is_btc:
            for _ in range(rng.poisson(18 * len(months))):
                s = int(rng.integers(400, total - 80))
                L = int(rng.integers(3, 7))
                ret[s:s + L] += rng.uniform(0.003, 0.010)
                qv[s:s + L] *= rng.uniform(4, 9, L)
                tb[s:s + L] = rng.uniform(0.62, 0.78, L)
                ret[s + L:s + L + 12] += rng.choice([-1, 1]) * 0.002
        cl = 10.0 * np.exp(np.cumsum(ret))
        op = np.concatenate([[10.0], cl[:-1]])
        hi = np.maximum(op, cl) * (1 + np.abs(rng.normal(0, 0.0015, total)))
        lo = np.minimum(op, cl) * (1 - np.abs(rng.normal(0, 0.0015, total)))
        t0 = ms[0] + np.arange(total) * BAR_MS
        for k, ym in enumerate(months):
            a, b = (ms[k] - ms[0]) // BAR_MS, (ms[k + 1] - ms[0]) // BAR_MS
            buf = io.StringIO()
            w = csv.writer(buf)
            for j in range(a, b):                     # microsecond timestamps, like 2025+ archives
                w.writerow([int(t0[j]) * 1000, op[j], hi[j], lo[j], cl[j], qv[j] / cl[j],
                            (int(t0[j]) + BAR_MS - 1) * 1000, qv[j], 100, 0, qv[j] * tb[j], 0])
            with zipfile.ZipFile(os.path.join(zdir, f"{sym}-5m-{ym}.zip"), "w", zipfile.ZIP_DEFLATED) as z:
                z.writestr(f"{sym}-5m-{ym}.csv", buf.getvalue())
    return syms


# ----------------------------------------------------------------------------------------
def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scanner", default="binance_early_pump_v6.py")
    ap.add_argument("--months", default=None, help="YYYY-MM:YYYY-MM (default: last 12 full months)")
    ap.add_argument("--data-dir", default="bt_data")
    ap.add_argument("--out", default="backtest_out")
    ap.add_argument("--symbols", default=None, help="text file with one symbol per line")
    ap.add_argument("--symbols-source", choices=["archive", "exchange"], default="archive")
    ap.add_argument("--halal", action="store_true", help="restrict to shariah_compliant_bases_v6.json")
    ap.add_argument("--max-symbols", type=int, default=0)
    ap.add_argument("--fee", type=float, default=0.10, help="%% per side")
    ap.add_argument("--slip", type=float, default=0.05, help="%% per side")
    ap.add_argument("--delay-candles", type=int, default=1,
                    help="entry at open of signal bar + N bars (5 min each); default 1 = realistic latency")
    ap.add_argument("--baseline-k", type=int, default=5)
    ap.add_argument("--no-btc-guard", action="store_true")
    ap.add_argument("--no-download", action="store_true")
    ap.add_argument("--workers", type=int, default=max(1, min(8, (os.cpu_count() or 2))))
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args(argv)

    if a.selftest:
        a.data_dir = tempfile.mkdtemp(prefix="bt_selftest_")
        a.out = os.path.join(a.data_dir, "out")
        months = month_range("2026-01:2026-04")
        syms = make_synthetic(a.data_dir, months)
        a.no_download = True
        print(f"[selftest] synthetic data in {a.data_dir}  (results are meaningless, machinery only)")
    else:
        months = month_range(a.months) if a.months else default_months()
        syms = None

    if syms is None:
        if a.symbols:
            syms = [s.strip().upper() for s in open(a.symbols) if s.strip()]
        else:
            if a.no_download:
                zdir = os.path.join(a.data_dir, "zips")
                syms = sorted({f.split("-5m-")[0] for f in os.listdir(zdir) if f.endswith(".zip")})
            else:
                print("Discovering symbols ...")
                syms = discover_symbols(a.symbols_source, a.data_dir)
        if a.halal:
            sc_dir = os.path.dirname(os.path.abspath(a.scanner))
            with open(os.path.join(sc_dir, "shariah_compliant_bases_v6.json"), encoding="utf-8") as f:
                bases = set(json.load(f)["symbols"])
            syms = [s for s in syms if s[:-4] in bases]
    stable = {"USDC", "FDUSD", "TUSD", "USDP", "DAI", "USDE", "USD1", "RLUSD", "USDD", "EUR", "EURI", "USTC"}
    syms = [s for s in syms if s[:-4] not in stable]
    if a.max_symbols:
        syms = syms[:a.max_symbols]
    print(f"Symbols: {len(syms)} | months: {months[0]}..{months[-1]} | workers: {a.workers}")

    delays = sorted({0, 1, 2, a.delay_candles})
    cfg = {"months": months, "data_dir": a.data_dir, "download": not a.no_download, "fee": a.fee,
           "slip": a.slip, "delay": a.delay_candles, "delays": delays, "baseline_k": a.baseline_k,
           "btc_guard": not a.no_btc_guard}

    all_tr, all_base, done, with_data = [], [], 0, 0
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=a.workers, initializer=_init_worker,
                             initargs=(a.scanner, cfg)) as ex:
        futs = {ex.submit(process_symbol, s): s for s in syms}
        for f in as_completed(futs):
            try:
                sym, tr, ba, nscan = f.result()
            except Exception as e:                       # one bad symbol must not kill the run
                print(f"  ! {futs[f]}: {type(e).__name__}: {e}")
                continue
            done += 1
            with_data += 1 if nscan or tr else 0
            all_tr += tr
            all_base += ba
            if done % 25 == 0 or done == len(syms):
                print(f"  {done}/{len(syms)} symbols | signals so far: {len(all_tr)} | "
                      f"{time.time() - t0:.0f}s")
    trades = pd.DataFrame(all_tr)
    base = pd.DataFrame(all_base)
    if not trades.empty:
        trades = trades.sort_values("signal_ts").reset_index(drop=True)
        trades.insert(1, "signal_time", pd.to_datetime(trades["signal_ts"], unit="ms").dt.strftime("%Y-%m-%d %H:%M"))
    text = report(trades, base, cfg, a.out, months, with_data)
    print(f"\nFiles written to: {os.path.abspath(a.out)}  (summary.txt, trades.csv, baseline.csv)")
    return trades, base, text


if __name__ == "__main__":
    main()
