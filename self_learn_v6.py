"""
المُتعلّم الذاتي V2 (Self-Learning Engine)

يقرأ نتائج الإشارات من early_pump_signal_results.csv ويعدّل عتبات الماسح بحذر
في state/auto_thresholds.json (الماسح بيقراه تلقائيًا في بداية كل تشغيلة).

الفرق عن النسخة الأولى (مهم):
1) تعريف النجاح: "TP1 اتحقق قبل SL" (من عمود outcome اللي الماسح بيحسبه بشموع 1m)،
   مش "MFE >= 3%". النسخة القديمة كانت تعتبر الإشارة ناجحة حتى لو ضربت SL قبلها.
2) القرار على أساس التوقّع (Expectancy بوحدة R)، مش نسبة النجاح لوحدها.
3) العتبة الافتراضية تتقرأ من ملف الماسح نفسه (القديم كان يفترض منتصف النطاق،
   فكان بيقفّز FINAL_SCORE من 80 لـ 83 من أول تشغيلة بدون سبب).
4) التجميع حسب lane (STRICT / EXPLOSIVE / PERSISTENT / REVERSAL / SQUEEZE).
5) كل عتبة بتتحرك خطوة واحدة بس لكل تقييم، حتى لو كذا lane بيشاور عليها.
6) ما بيعدّلش إلا كل MIN_NEW_SIGNALS إشارة جديدة، وبعد MIN_PER_LANE إشارة للـ lane.
7) آمن: نطاقات محدودة + القائمة البيضاء موجودة كمان جوه الماسح.

متغيرات بيئة اختيارية:
  SELF_LEARN_DRY_RUN=1   -> يقيّم ويطبع بس، من غير ما يكتب العتبات.
"""
import ast
import json
import os
from datetime import datetime, timezone

import pandas as pd

TRACK_FILE = "early_pump_signal_results_v6.csv"
SCANNER_FILE = os.getenv("SCANNER_FILE", "binance_early_pump_v6.py")
STATE_DIR = "state"
OUT_FILE = os.path.join(STATE_DIR, "auto_thresholds.json")
PERF_FILE = os.path.join(STATE_DIR, "self_learn_performance.json")
META_FILE = os.path.join(STATE_DIR, "self_learn_meta.json")
DRY_RUN = os.getenv("SELF_LEARN_DRY_RUN", "0") == "1"

WINDOW = 80               # آخر كام إشارة محسومة تتحسب (بيتكيّف مع تغيّر السوق)
MIN_PER_LANE = 15         # أقل عدد إشارات في الـ lane قبل ما نحكم عليه
MIN_NEW_SIGNALS = 10      # أقل عدد إشارات جديدة بين تقييمين
TIGHTEN_BELOW_R = -0.15   # توقّع أقل من كده  -> شدّد
LOOSEN_ABOVE_R = 0.35     # توقّع أعلى من كده -> خفّف شوية
WARN_OVERALL_R = -0.40    # تحذير لو الأداء العام أسوأ من كده (بعد 30 إشارة)

# (الحد الأدنى، الحد الأقصى) لكل عتبة، والقيم الافتراضية داخل النطاقات
BOUNDS = {
    "FINAL_SCORE": (78, 92),
    "PERSISTENT_FINAL_SCORE": (65, 85),
    "REVERSAL_FINAL_SCORE": (68, 88),
    "SQUEEZE_FINAL_SCORE": (65, 85),
    "FINAL_MIN_BUY_PRESSURE": (0.52, 0.68),
    "FINAL_MIN_VOLUME_15M": (3.0, 7.0),
    "FINAL_MIN_ACCELERATION": (1.0, 2.5),
    "EXPLOSIVE_MIN_BUY_PRESSURE": (0.54, 0.68),
    "EXPLOSIVE_MIN_ACCELERATION": (1.2, 3.0),
}
FALLBACK_DEFAULTS = {
    "FINAL_SCORE": 80, "PERSISTENT_FINAL_SCORE": 70, "REVERSAL_FINAL_SCORE": 72,
    "SQUEEZE_FINAL_SCORE": 70, "FINAL_MIN_BUY_PRESSURE": 0.55, "FINAL_MIN_VOLUME_15M": 4.0,
    "FINAL_MIN_ACCELERATION": 1.2, "EXPLOSIVE_MIN_BUY_PRESSURE": 0.58, "EXPLOSIVE_MIN_ACCELERATION": 1.5,
}
STEP = {k: (1 if "SCORE" in k else 0.02 if "PRESSURE" in k else 0.2) for k in BOUNDS}

# أي عتبات بيتحكم فيها كل lane (كلها "حد أدنى": رفعها = تشديد)
LANE_KEYS = {
    "STRICT": ["FINAL_SCORE", "FINAL_MIN_BUY_PRESSURE", "FINAL_MIN_ACCELERATION", "FINAL_MIN_VOLUME_15M"],
    "EXPLOSIVE": ["FINAL_SCORE", "EXPLOSIVE_MIN_BUY_PRESSURE", "EXPLOSIVE_MIN_ACCELERATION"],
    "PERSISTENT": ["PERSISTENT_FINAL_SCORE"],
    "REVERSAL": ["REVERSAL_FINAL_SCORE"],
    "SQUEEZE": ["SQUEEZE_FINAL_SCORE"],
}


def clamp(key, val):
    lo, hi = BOUNDS[key]
    return max(lo, min(hi, round(val, 4)))


def read_defaults():
    """يقرأ القيم الحالية من ملف الماسح بدون ما يشغّله (ast فقط)."""
    out = dict(FALLBACK_DEFAULTS)
    try:
        with open(SCANNER_FILE, encoding="utf-8") as f:
            tree = ast.parse(f.read())
        for node in tree.body:
            if (isinstance(node, ast.Assign) and len(node.targets) == 1
                    and isinstance(node.targets[0], ast.Name) and node.targets[0].id in BOUNDS):
                try:
                    val = ast.literal_eval(node.value)
                except Exception:
                    continue
                if isinstance(val, (int, float)) and not isinstance(val, bool):
                    out[node.targets[0].id] = val
    except Exception as e:
        print(f"[self_learn] تعذّر قراءة {SCANNER_FILE}: {e} — استخدام القيم الاحتياطية")
    return out


def lane_from_setup(setup):
    s = str(setup).upper()
    if "EXPLOSIVE" in s:
        return "EXPLOSIVE"
    if "PERSISTENT" in s:
        return "PERSISTENT"
    if "REVERSAL" in s:
        return "REVERSAL"
    if "SQUEEZE" in s:
        return "SQUEEZE"
    return "STRICT"


def load_decided():
    """يرجّع الإشارات المحسومة (فيها outcome) مرتبة زمنيًا."""
    df = pd.read_csv(TRACK_FILE)
    if df.empty:
        return df
    for col in ("lane", "outcome", "exit_pct", "outcome_min"):
        if col not in df.columns:
            df[col] = None
    df["sl_pct"] = pd.to_numeric(df["sl_pct"], errors="coerce")
    df["tp1_pct"] = pd.to_numeric(df["tp1_pct"], errors="coerce")
    df["exit_pct"] = pd.to_numeric(df["exit_pct"], errors="coerce")

    # صفوف قديمة (قبل V4.4) ملهاش outcome: نستنتجه بتحفّظ من MFE/MAE بعد ما تخلص 24 ساعة.
    # لو TP1 وSL اتلمسوا الاثنين -> نعتبرها SL (ما نعرفش الترتيب).
    legacy = df["outcome"].isna() & (df["status"] == "COMPLETED") & df["mfe_pct"].notna() & df["sl_pct"].notna()
    if legacy.any():
        sl_hit = df["mae_pct"] <= -df["sl_pct"]
        tp_hit = df["mfe_pct"] >= df["tp1_pct"]
        fallback_exit = df["result_4h"].fillna(df["result_24h"])
        df.loc[legacy, "outcome"] = pd.Series(
            ["SL" if s else ("TP1" if t else "TIMEOUT") for s, t in zip(sl_hit, tp_hit)], index=df.index)[legacy]
        ex = pd.Series([-sp if s else (tp if t else fb)
                        for s, t, sp, tp, fb in zip(sl_hit, tp_hit, df["sl_pct"], df["tp1_pct"], fallback_exit)],
                       index=df.index)
        df.loc[legacy, "exit_pct"] = ex[legacy]

    df = df[df["outcome"].isin(["TP1", "SL", "TIMEOUT"]) & df["exit_pct"].notna() & (df["sl_pct"] > 0)].copy()
    df["lane"] = [l if isinstance(l, str) and l else lane_from_setup(s) for l, s in zip(df["lane"], df["setup"])]
    df["exit_r"] = df["exit_pct"] / df["sl_pct"]
    return df.sort_values("final_time_utc").reset_index(drop=True)


def lane_stats(g):
    return {
        "n": int(len(g)),
        "win_rate": round(float((g["outcome"] == "TP1").mean()), 3),
        "sl_rate": round(float((g["outcome"] == "SL").mean()), 3),
        "timeout_rate": round(float((g["outcome"] == "TIMEOUT").mean()), 3),
        "avg_exit_pct": round(float(g["exit_pct"].mean()), 2),
        "expectancy_R": round(float(g["exit_r"].mean()), 3),
    }


def telegram_notify(text):
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat or DRY_RUN:
        return
    try:
        import requests
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                      json={"chat_id": chat, "text": text}, timeout=15)
    except Exception as e:
        print(f"[self_learn] telegram failed: {e}")


def main():
    os.makedirs(STATE_DIR, exist_ok=True)
    if not os.path.exists(TRACK_FILE):
        print(f"[self_learn] لا يوجد {TRACK_FILE} بعد — لا تعديلات.")
        return

    all_decided = load_decided()
    total = len(all_decided)
    meta = {}
    if os.path.exists(META_FILE):
        try:
            with open(META_FILE, encoding="utf-8") as f:
                meta = json.load(f)
        except Exception:
            meta = {}
    last_n = int(meta.get("last_eval_count", 0))

    if total - last_n < MIN_NEW_SIGNALS:
        print(f"[self_learn] إشارات محسومة={total} (آخر تقييم عند {last_n}) — منتظر {MIN_NEW_SIGNALS} جديدة.")
        return

    df = all_decided.tail(WINDOW)
    defaults = read_defaults()
    overrides = {}
    if os.path.exists(OUT_FILE):
        try:
            with open(OUT_FILE, encoding="utf-8") as f:
                overrides = json.load(f)
        except Exception:
            overrides = {}

    overall = lane_stats(df)
    report = {
        "updated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
        "decided_signals_total": total,
        "window": int(len(df)),
        "overall": overall,
        "by_lane": {},
        "adjustments": {},
        "dry_run": DRY_RUN,
        "note": "expectancy_R = متوسط (النتيجة % / نسبة SL). موجب = رابح. التعادل عند فوز ~40% لو TP1=1.5R.",
    }

    votes = {}     # key -> مجموع الأصوات (+1 شدّد / -1 خفّف)
    for lane, grp in df.groupby("lane"):
        st = lane_stats(grp)
        report["by_lane"][lane] = st
        if len(grp) < MIN_PER_LANE:
            st["decision"] = f"عينة صغيرة (<{MIN_PER_LANE})"
            continue
        r = st["expectancy_R"]
        vote = 1 if r < TIGHTEN_BELOW_R else (-1 if r > LOOSEN_ABOVE_R else 0)
        st["decision"] = {1: "تشديد", -1: "تخفيف", 0: "ثبات"}[vote]
        for k in LANE_KEYS.get(lane, []):
            votes[k] = votes.get(k, 0) + vote

    for k, v in votes.items():
        if v == 0 or k not in BOUNDS:
            continue
        cur = overrides.get(k, defaults.get(k, FALLBACK_DEFAULTS[k]))
        new = clamp(k, cur + (STEP[k] if v > 0 else -STEP[k]))
        if k in ("FINAL_SCORE", "PERSISTENT_FINAL_SCORE", "REVERSAL_FINAL_SCORE", "SQUEEZE_FINAL_SCORE"):
            new = int(round(new))
        if new != cur:
            overrides[k] = new
            report["adjustments"][k] = {"from": cur, "to": new, "net_votes": v}

    if total >= 30 and overall["expectancy_R"] <= WARN_OVERALL_R:
        report["warning"] = (f"الأداء العام سلبي (expectancy_R={overall['expectancy_R']}). "
                             "تعديل العتبات لوحده مش هيصلّح ده، راجع منطق الدخول/SL.")

    if not DRY_RUN:
        with open(OUT_FILE, "w", encoding="utf-8") as f:
            json.dump(overrides, f, indent=2)
        with open(PERF_FILE, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        meta["last_eval_count"] = total
        meta["last_eval_utc"] = report["updated_at_utc"]
        with open(META_FILE, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)

    print(f"[self_learn] محسومة={total} | نافذة={len(df)} | فوز TP1={overall['win_rate']:.0%} | "
          f"SL={overall['sl_rate']:.0%} | متوسط خروج={overall['avg_exit_pct']:+.2f}% | R={overall['expectancy_R']:+.2f}")
    for lane, d in report["by_lane"].items():
        print(f"  {lane:10} n={d['n']:3} win={d['win_rate']:.0%} R={d['expectancy_R']:+.2f}  -> {d.get('decision')}")
    if report["adjustments"]:
        print("[self_learn] تعديلات:", json.dumps(report["adjustments"], ensure_ascii=False))
    else:
        print("[self_learn] لا تعديلات هذه الجولة.")
    if "warning" in report:
        print("[self_learn] ⚠️", report["warning"])

    lines = [f"🤖 [V6] 🧠 تقييم التعلّم الذاتي ({total} إشارة محسومة)",
             f"فوز TP1: {overall['win_rate']:.0%} | ضرب SL: {overall['sl_rate']:.0%}",
             f"متوسط الخروج: {overall['avg_exit_pct']:+.2f}% | Expectancy: {overall['expectancy_R']:+.2f}R"]
    for lane, d in report["by_lane"].items():
        lines.append(f"• {lane}: n={d['n']} win={d['win_rate']:.0%} R={d['expectancy_R']:+.2f} ({d.get('decision')})")
    if report["adjustments"]:
        lines.append("تعديلات: " + ", ".join(f"{k} {v['from']}→{v['to']}" for k, v in report["adjustments"].items()))
    if "warning" in report:
        lines.append("⚠️ " + report["warning"])
    telegram_notify("\n".join(lines))


if __name__ == "__main__":
    main()
