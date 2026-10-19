# -*- coding: utf-8 -*-
"""
籌碼面監控 —— 觸發式推播
================================================================================
為什麼另外做一支：法人買賣超與融資融券是「規則判斷」，不需要任何 AI 推理，
所以可以放心交給排程自動跑——它不會累、不會看錯，也不會在沒人叫它的時候漏掉。
相對地，「這檔該不該買」這種需要判斷的事，一律只做成推播給人看，不自動下任何單。

監控規則（全部可在 CHIP_RULES 調參）：
  R1 投信連買  —— 投信連續買超 N 日且累計張數達標
  R2 法人共識  —— 外資與投信同步買超，且兩者累計都達標
  R3 外資連買  —— 外資連續買超 N 日且累計達標
  R4 籌碼沉澱  —— 融資餘額減少 + 股價上漲（散戶退場、股價仍撐住）
  R5 融資警訊  —— 融資餘額大增 + 股價下跌（散戶追高套牢，列為迴避名單）

去重：同一檔同一規則，只有在「連買天數增加」或「隔了 COOLDOWN_DAYS 以上」才會再推，
      避免連買 5 天被同一檔洗版 5 次。狀態存 .cache/chip_monitor_state.json。

用法：
    python -m tools.chip_monitor          # 跑一次，有觸發就推 Telegram
    python -m tools.chip_monitor --dry    # 只印不推（測試用）
"""
import os
import sys
import json
import argparse
from datetime import datetime, date

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "daily_report"))
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

import stock_screener as ss

CACHE = ss.CACHE_DIR
STATE_F = os.path.join(CACHE, "chip_monitor_state.json")
MARGIN_HIST_F = os.path.join(CACHE, "margin_history.json")

CHIP_RULES = {
    "R1_投信連買": {"consec": 3, "cum_lots": 1000},
    "R2_法人共識": {"foreign_cum": 2000, "trust_cum": 500},
    "R3_外資連買": {"consec": 4, "cum_lots": 5000},
    "R4_籌碼沉澱": {"margin_drop_pct": 3.0, "price_up_pct": 1.0},
    "R5_融資警訊": {"margin_rise_pct": 8.0, "price_down_pct": -2.0},
}
MIN_PRICE = 10
MIN_VOL_LOTS = 500
COOLDOWN_DAYS = 5

RULE_ORDER = ["R1_投信連買", "R2_法人共識", "R3_外資連買", "R4_籌碼沉澱", "R5_融資警訊"]
RULE_ICON = {"R1_投信連買": "🏦", "R2_法人共識": "🤝", "R3_外資連買": "🌏",
             "R4_籌碼沉澱": "🧲", "R5_融資警訊": "⚠️"}


def _load(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1)


def snapshot_margin(margin):
    """每天存一份融資餘額，隔日才有得比。
    第一天跑必然沒有前日資料 —— 誠實跳過 R4/R5，不用 0 硬算出假訊號。"""
    hist = _load(MARGIN_HIST_F, {})
    today = date.today().isoformat()
    hist[today] = {c: v.get("margin_balance", 0) for c, v in margin.items()}
    for d in sorted(hist)[:-10]:
        hist.pop(d, None)
    _save(MARGIN_HIST_F, hist)
    prev_days = [d for d in sorted(hist) if d < today]
    return hist[prev_days[-1]] if prev_days else None


def scan():
    print("抓當日行情…", flush=True)
    quotes = {s["code"]: s for s in ss.prescreen_all(
        min_price=MIN_PRICE, min_volume=MIN_VOL_LOTS, top_n=100000)}
    print(f"  符合價量門檻 {len(quotes)} 檔", flush=True)

    print("抓近 5 日三大法人…", flush=True)
    inst = ss.fetch_institutional_history(days=5)
    print(f"  {len(inst)} 檔有法人資料", flush=True)

    print("抓融資融券…", flush=True)
    margin = ss.fetch_margin_data()
    print(f"  {len(margin)} 檔有融資資料", flush=True)

    prev_margin = snapshot_margin(margin) if margin else None
    if prev_margin is None:
        print("  ⚠ 無前一日融資快照，本次跳過 R4/R5（明天起就有得比）", flush=True)

    hits = []
    for code, q in quotes.items():
        nm = q["name"]
        chg = q["change_pct"]
        i = inst.get(code) or {}
        fc = i.get("foreign_consec", 0)
        tc = i.get("trust_consec", 0)
        # ⚠ 證交所 T86 回傳的是「股數」不是「張數」（1 張 = 1000 股）。
        # 既有 daily_report/analyst.py 的 _lots() 就是在做這件事，這裡比照辦理，
        # 否則門檻會整整差 1000 倍，幾乎每檔都會觸發。
        fcum = i.get("foreign_cum", 0) / 1000.0
        tcum = i.get("trust_cum", 0) / 1000.0

        r = CHIP_RULES["R1_投信連買"]
        if tc >= r["consec"] and tcum >= r["cum_lots"]:
            hits.append((code, nm, "R1_投信連買", tc,
                         f"投信連買 {tc} 日、5日累計 +{tcum:,.0f} 張｜今日 {chg:+.2f}%"))

        r = CHIP_RULES["R2_法人共識"]
        if fcum >= r["foreign_cum"] and tcum >= r["trust_cum"] and fc >= 1 and tc >= 1:
            hits.append((code, nm, "R2_法人共識", min(fc, tc),
                         f"外資 +{fcum:,.0f} 張／投信 +{tcum:,.0f} 張（同步買超）｜今日 {chg:+.2f}%"))

        r = CHIP_RULES["R3_外資連買"]
        if fc >= r["consec"] and fcum >= r["cum_lots"]:
            hits.append((code, nm, "R3_外資連買", fc,
                         f"外資連買 {fc} 日、5日累計 +{fcum:,.0f} 張｜今日 {chg:+.2f}%"))

        if prev_margin:
            cur = (margin.get(code) or {}).get("margin_balance", 0)
            prev = prev_margin.get(code, 0)
            if prev > 0 and cur > 0:
                d_pct = (cur - prev) / prev * 100
                r = CHIP_RULES["R4_籌碼沉澱"]
                if d_pct <= -r["margin_drop_pct"] and chg >= r["price_up_pct"]:
                    hits.append((code, nm, "R4_籌碼沉澱", 1,
                                 f"融資 {d_pct:+.1f}%（散戶退場）但股價 {chg:+.2f}%"))
                r = CHIP_RULES["R5_融資警訊"]
                if d_pct >= r["margin_rise_pct"] and chg <= r["price_down_pct"]:
                    hits.append((code, nm, "R5_融資警訊", 1,
                                 f"融資 {d_pct:+.1f}%（散戶加碼）但股價 {chg:+.2f}% — 迴避"))
    return hits


def dedupe(hits, dry=False):
    """只推新的或變強的訊號。
    dry=True 時不寫狀態檔 —— 否則測試跑一次就會把今天的訊號全標記成已推播，
    正式跑起來變成「今日無新訊號」，靜默漏掉整天的訊號。"""
    state = _load(STATE_F, {})
    today = date.today().isoformat()
    fresh = []
    newstate = dict(state)
    for code, nm, rule, strength, msg in hits:
        k = f"{code}|{rule}"
        old = state.get(k)
        if old:
            gap = (date.fromisoformat(today) - date.fromisoformat(old["date"])).days
            if strength <= old["strength"] and gap < COOLDOWN_DAYS:
                newstate[k] = {"date": today, "strength": strength}
                continue
        fresh.append((code, nm, rule, strength, msg))
        newstate[k] = {"date": today, "strength": strength}
    if not dry:
        _save(STATE_F, newstate)
    return fresh


def format_msg(fresh):
    if not fresh:
        return None
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    buckets = {}
    for code, nm, rule, _s, msg in fresh:
        buckets.setdefault(rule, []).append(f"　{code} {nm}\n　　{msg}")
    lines = [f"📡 籌碼監控 {stamp}",
             "（以下是證交所買賣超／融資餘額的事實，不是買賣建議）", ""]
    for rule in RULE_ORDER:
        if rule in buckets:
            items = buckets[rule]
            lines.append(f"{RULE_ICON[rule]} {rule.split('_', 1)[1]}（{len(items)} 檔）")
            lines += items[:12]
            if len(items) > 12:
                lines.append(f"　…另 {len(items) - 12} 檔")
            lines.append("")
    lines.append("資料源：證交所 T86 三大法人／MI_MARGN 融資融券")
    return "\n".join(lines)


def push(msg):
    import telegram_push as tg
    cfg = _load(os.path.join(_ROOT, "daily_report", "report_config.json"), {})
    if not tg.configured(cfg):
        print("\n⚠ 未設定 TELEGRAM_BOT_TOKEN／chat_id，僅印出未推播")
        return False
    # telegram_push.send_all(messages, cfg) —— 參數是 (訊息list, config)
    tg.send_all([msg], cfg)
    print("\n✓ 已推播 Telegram")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true", help="只印不推播")
    a = ap.parse_args()

    hits = scan()
    print(f"\n原始觸發 {len(hits)} 筆", flush=True)
    fresh = dedupe(hits, dry=a.dry)
    print(f"去重後 {len(fresh)} 筆", flush=True)

    msg = format_msg(fresh)
    if not msg:
        print("今日無新籌碼訊號")
        return
    print("\n" + msg)
    if a.dry:
        print("\n[--dry] 未推播")
        return
    push(msg)


if __name__ == "__main__":
    main()
