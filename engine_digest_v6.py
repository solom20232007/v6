"""Optional daily Telegram digest of the research engine's top picks (research output, NOT a buy/sell signal).

Fail-open: any error exits 0 and never affects the V6 scan. Sends once per new decision bar (dedup in state/engine_digest_state.json).
Env: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, OUR_RECS_URL (raw JSON), DIGEST_HALAL_ONLY (default 1), DIGEST_TOP (default 5).
"""
import json, os, sys, urllib.request

STATE = "state/engine_digest_state.json"

def main():
    url = os.environ.get("OUR_RECS_URL", "").strip()
    tok, chat = os.environ.get("TELEGRAM_BOT_TOKEN", ""), os.environ.get("TELEGRAM_CHAT_ID", "")
    if not (url and tok and chat):
        return
    recs = json.load(urllib.request.urlopen(url, timeout=15))
    if not recs:
        return
    bar = recs[0]["decision_bar_utc"]
    try:
        last = json.load(open(STATE)).get("last_bar")
    except Exception:
        last = None
    if last == bar:
        return
    halal_only = os.environ.get("DIGEST_HALAL_ONLY", "1") != "0"
    top = int(os.environ.get("DIGEST_TOP", "5"))
    pick = [r for r in recs if r.get("shariah_listed_v6") or not halal_only][:top]
    lines = [f"📊 توصيات المحرك البحثي — شمعة {bar[:10]} (ليست إشارة شراء/بيع)"]
    if recs[0].get("market_wide"):
        lines.append("⚠️ سوق صاعد عام اليوم: دقة النموذج قريبة من الصدفة.")
    for r in pick:
        lines.append(f"#{r['rank']} {r['symbol']}  احتمال +10%/24س ≈ {float(r['p_up10_24h'])*100:.0f}%" + ("  ✅حلال(V6)" if r.get("shariah_listed_v6") else ""))
        sg = r.get("recent_surge")
        if sg:
            lines.append(f"   ⚠️ قفزة حجم قبل {sg['hours_ago']} س (ساعة {sg['hour_ret']*100:+.0f}% بحجم {sg['vol_ratio']:.0f}×): لا تلاحقها — تاريخيًا وسيط -8% بعد 24س")
        if r.get("eth_contract") and r.get("onchain_ratio_6h") is not None:
            w = f"، تحويلات حيتان ${r['whale_usd_6h']:,}" if r.get("whale_usd_6h") else ""
            lines.append(f"   🔗 إيثيريوم: نشاط التحويلات 6س = {r['onchain_ratio_6h']}× المعتاد{w} (معلومة غير مختبرة كمؤشر)")
    if not pick:
        lines.append("لا توجد عملات من قائمة الحلال ضمن أعلى 30 اليوم.")
    lines.append("قائمة الحلال من طرف ثالث وغير مراجعة شرعيًا. تأكد بنفسك.")
    body = json.dumps({"chat_id": chat, "text": "\n".join(lines)}).encode()
    req = urllib.request.Request(f"https://api.telegram.org/bot{tok}/sendMessage", data=body, headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req, timeout=15)
    os.makedirs("state", exist_ok=True)
    json.dump({"last_bar": bar}, open(STATE, "w"))

try:
    main()
except Exception as e:
    print("engine digest skipped:", e, file=sys.stderr)
