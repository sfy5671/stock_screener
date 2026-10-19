# -*- coding: utf-8 -*-
"""
證據層 —— 把 tools/backtest.py 的回測結果餵回每日戰報。
================================================================================
為什麼要這層：戰報如果只說「KD 黃金交叉、偏多」，聽起來很篤定，
但「KD 黃金交叉」歷史勝率到底是 58% 還是 47%，讀的人無從判斷該信幾分。
這支模組把回測數字掛在訊號旁邊，讓戰報講的每句推論都帶著它的命中率。

超額(edge) 是關鍵欄位：策略均報酬 − 不挑日子的均報酬。
  edge > 0 才代表「這個訊號真的有選股能力」；edge <= 0 就是看起來準、其實沒用。
"""
import os
import csv
import glob

_DIR = os.path.dirname(os.path.abspath(__file__))
_RESULTS_DIR = os.path.join(os.path.dirname(_DIR), "backtest_results")

# 台股來回交易成本：手續費 0.1425%×2（多數券商有折扣）+ 證交稅 0.3% ≈ 0.45~0.6%。
# 超額報酬低於這個數字，賺的錢全被成本吃掉，等於沒效。
COST_PCT = 0.6

_cache = None


def load_latest(horizon=10):
    """讀最新一份回測 CSV → {策略名: {勝率, 均報酬, 超額, 訊號次數, t值}}。
    沒有回測結果就回空 dict —— 戰報照常出，只是不附統計，不會爆掉。"""
    global _cache
    if _cache is not None:
        return _cache
    _cache = {}
    try:
        files = sorted(glob.glob(os.path.join(_RESULTS_DIR, "backtest_*.csv")))
        if not files:
            return _cache
        with open(files[-1], encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                name = row.get("策略")
                if not name:
                    continue
                try:
                    _cache[name] = {
                        "勝率": float(row.get(f"{horizon}日勝率%", "nan")),
                        "均報酬": float(row.get(f"{horizon}日均報酬%", "nan")),
                        "超額": float(row.get(f"{horizon}日超額%", "nan")),
                        "次數": int(float(row.get("訊號次數", 0))),
                        "t值": float(row.get(f"{horizon}日t值", "nan") or "nan"),
                    }
                except (ValueError, TypeError):
                    continue
    except Exception:
        pass
    return _cache


def summarize(strategy_results, horizon=10, top=5):
    """strategy_results = {策略名: bool}（calc_score_and_details 的第 3 個回傳值）。
    回傳一段要塞進 prompt 的文字，依超額由高到低排序。"""
    stats = load_latest(horizon)
    if not stats:
        return "（尚未跑過回測，本次無歷史統計可附；執行 python -m tools.backtest 後即有）"

    hit = [(n, stats[n]) for n, passed in (strategy_results or {}).items()
           if passed and n in stats]
    if not hit:
        return "（今日觸發的訊號都沒有回測數據，多半是籌碼類策略——那類請看籌碼監控）"

    hit.sort(key=lambda x: (x[1]["超額"] if x[1]["超額"] == x[1]["超額"] else -99), reverse=True)
    lines = []
    for n, s in hit[:top]:
        # 超額必須大於來回交易成本才算真的有效，否則賺的都被手續費+證交稅吃掉。
        if s["超額"] > COST_PCT and s.get("t值", 0) >= 2:
            verdict = "有效"
        elif s["超額"] > COST_PCT:
            verdict = "正向但不顯著"
        elif s["超額"] > 0:
            verdict = "扣掉交易成本後等於零"
        else:
            verdict = "無選股能力"
        lines.append(f"　・{n}：{horizon}日勝率 {s['勝率']:.0f}%、超額 {s['超額']:+.2f}%"
                     f"（樣本 {s['次數']:,} 次，判定：{verdict}）")
    return "\n".join(lines)


def weakest_warning(strategy_results, horizon=10):
    """若今日觸發的訊號裡有回測證明無效的，挑出來提醒，避免戰報拿爛訊號當理由。"""
    stats = load_latest(horizon)
    bad = [n for n, passed in (strategy_results or {}).items()
           if passed and n in stats and stats[n]["超額"] <= COST_PCT]
    if not bad:
        return ""
    return "⚠ 以下訊號回測顯示超額報酬不足以覆蓋交易成本，不應拿來當作看多理由：" + "、".join(bad[:4])
