# -*- coding: utf-8 -*-
"""
飆股 Top-N 選股 —— 用 stock_screener 既有的 prescreen + momentum 策略，
從當日全市場挑出最強的「準備爆發」候選，餵給 analyst 判讀。
"""
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
import stock_screener as ss


def _get_df(code):
    """上市(.TW)抓不到就試上櫃(.TWO)。"""
    df = ss.get_stock_data(f"{code}.TW")
    if df is None:
        df = ss.get_stock_data(f"{code}.TWO")
    return df


def _score_one(item):
    code = item["code"]
    name = item.get("name", code)
    df = _get_df(code)
    if df is None:
        return None
    try:
        score, total, _sr, d = ss.calc_score_and_details(df, mode="momentum", code=code)
    except Exception:
        return None
    return {
        "code": code, "name": name,
        "score": score, "total": total,
        "price": d.get("price"),
        "change_pct": item.get("change_pct", 0),
    }


def pick_top_flyers(n=5, pool=40, min_price=20, min_volume=3000, min_score=3, workers=8):
    """
    回傳當日最強的 n 檔飆股候選（依 momentum 符合項數排序）。
    pool: 先用活躍度粗篩幾檔進深度分析
    min_score: momentum 至少符合幾項才入選（過濾濫竽）
    """
    candidates = ss.prescreen_all(min_price=min_price, min_volume=min_volume, top_n=pool)
    if not candidates:
        return []

    scored = []
    with ThreadPoolExecutor(max_workers=workers) as pool_exec:
        futures = {pool_exec.submit(_score_one, c): c for c in candidates}
        for fut in as_completed(futures):
            r = fut.result()
            if r and r["score"] >= min_score:
                scored.append(r)

    # 依 momentum 符合項數排序，同分者當日漲幅大者優先
    scored.sort(key=lambda x: (x["score"], x["change_pct"]), reverse=True)
    return scored[:n]


if __name__ == "__main__":
    print("撈當日飆股候選中（會即時抓全市場，稍等）...")
    picks = pick_top_flyers()
    if not picks:
        print("今日無符合條件的飆股候選")
    for i, p in enumerate(picks, 1):
        print(f"{i}. {p['name']}（{p['code']}）  momentum {p['score']}/{p['total']}"
              f"  收 {p['price']}  漲跌 {p['change_pct']}%")
