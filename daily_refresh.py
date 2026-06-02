"""
飆股篩選器 - 每日資料更新腳本

每天台股盤後（14:30 之後）執行一次，抓取以下 4 個資料源並寫入 .cache/：
- BWIBBU_ALL → basic_indicators.json   (本益比/殖利率/股價淨值比)
- t187ap03_L/O → company_capital.json (發行股數 → 算換手率)
- T86 近 5 日 → inst_hist_5.json      (連續法人買超)
- MI_MARGN → margin.json              (融資融券)

抓完後由 daily_refresh.bat 自動 commit + push 到 GitHub，
Render 線上版會偵測新 commit 自動重新部署，讀取這些已抓好的 cache。

直接執行：python daily_refresh.py
"""
import os
import sys
import json
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stock_screener as ss

CACHE_DIR = ss.CACHE_DIR

REFRESH_TARGETS = [
    ("basic_indicators.json",   "本益比/殖利率/股價淨值比",  ss.fetch_basic_indicators),
    ("company_capital.json",    "發行股數（換手率用）",       ss.fetch_company_capital),
    ("inst_hist_5.json",        "近 5 日法人買賣超",          lambda: ss.fetch_institutional_history(5)),
    ("margin.json",             "融資融券",                   ss.fetch_margin_data),
]


def force_refresh(name, func):
    """強制重抓：先把舊 cache 改名備份，再呼叫 fetch（會走完整 API 流程）。"""
    path = os.path.join(CACHE_DIR, name)
    backup = path + ".bak"
    # 把 fail flag 清掉，確保函式不會被 fail-fast 攔截
    for key in ("basic", "capital", "inst_hist", "margin"):
        ss._fetch_failed_today.pop(key, None)
    if os.path.exists(path):
        try:
            os.replace(path, backup)
        except Exception:
            pass
    t0 = time.time()
    try:
        data = func()
    except Exception as e:
        data = None
        print(f"  [ERR] {e}")
    elapsed = time.time() - t0
    ok = bool(data) and os.path.exists(path)
    count = len(data) if isinstance(data, dict) else 0
    if not ok and os.path.exists(backup):
        # 失敗：還原舊檔
        try:
            os.replace(backup, path)
            print(f"  [WARN] 抓取失敗，已還原舊 cache")
        except Exception:
            pass
    else:
        # 成功：刪舊備份
        if os.path.exists(backup):
            try: os.remove(backup)
            except Exception: pass
    return ok, count, elapsed


def main():
    print("=" * 60)
    print(f"  飆股篩選器 - 每日資料更新  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print(f"  cache dir: {CACHE_DIR}")
    print()

    summary = []
    for fname, desc, func in REFRESH_TARGETS:
        print(f"▶ {desc}  →  {fname}")
        ok, count, elapsed = force_refresh(fname, func)
        flag = "✓ OK " if ok else "✗ FAIL"
        print(f"  {flag}  {count:>5} 筆 / {elapsed:.1f} 秒")
        summary.append((fname, ok, count, elapsed))
        print()

    print("=" * 60)
    print("  總結")
    print("=" * 60)
    total_ok = sum(1 for _, ok, _, _ in summary if ok)
    for fname, ok, count, elapsed in summary:
        flag = "OK " if ok else "FAIL"
        print(f"  [{flag}]  {fname:<28}  {count:>5} 筆")
    print()
    print(f"  {total_ok} / {len(summary)} 成功")
    print()

    if total_ok == 0:
        print("⚠ 全部失敗，可能網路問題或 TWSE 維護中。建議稍後再試。")
        sys.exit(1)

    return 0


if __name__ == "__main__":
    sys.exit(main())
