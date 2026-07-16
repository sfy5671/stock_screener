# -*- coding: utf-8 -*-
"""
每日股市戰報 —— 主流程
  1. 判讀自選股（全判）
  2. 選當日飆股 Top-N 並判讀
  3. 組成戰報 → 存檔 + Telegram 推播

用法:
  python daily_report/run_daily_report.py            # 完整跑 + 推播
  python daily_report/run_daily_report.py --no-push  # 只產戰報存檔，不推播（測試用）
"""
import os
import sys
import json
from datetime import datetime

try:  # Windows 終端/排程 log 用 cp950，印 emoji 會炸，強制 UTF-8
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_DIR)
for p in (_DIR, _ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)

import analyst
import picker
import telegram_push as tg

CONFIG_PATH = os.path.join(_DIR, "report_config.json")
WATCHLIST_PATH = os.path.join(_DIR, "watchlist.json")
REPORTS_DIR = os.path.join(_DIR, "reports")
WEEKDAYS = ["一", "二", "三", "四", "五", "六", "日"]


def load_config():
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


def load_watchlist(cfg):
    """自選股與 App 頁共用 watchlist.json；讀不到才回退 config 裡的 watchlist。"""
    try:
        with open(WATCHLIST_PATH, encoding="utf-8") as f:
            wl = json.load(f)
        if isinstance(wl, list) and wl:
            return wl
    except Exception:
        pass
    return cfg.get("watchlist", [])


def build_report(cfg):
    """回傳 (messages: list[str], full_text: str)。"""
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    header = f"🗓 每日股市戰報 {date_str}（週{WEEKDAYS[now.weekday()]}）"
    messages = [header]

    # --- 自選股：全判 ---
    watchlist = load_watchlist(cfg)
    if watchlist:
        messages.append("━━━━ 📌 我的自選股 ━━━━")
        for item in watchlist:
            print(f"  判讀自選股 {item['code']} {item.get('name','')}...")
            card = analyst.analyze(item["code"], item.get("name"))
            messages.append(card["text"])

    # --- 飆股雷達 ---
    fcfg = cfg.get("flyers", {})
    top_n = int(fcfg.get("top_n", 0))
    if top_n > 0:
        print("  撈當日飆股候選（即時抓全市場）...")
        picks = picker.pick_top_flyers(
            n=top_n, pool=int(fcfg.get("pool", 40)),
            min_price=fcfg.get("min_price", 20),
            min_volume=fcfg.get("min_volume", 3000),
            min_score=fcfg.get("min_score", 3),
        )
        messages.append(f"━━━━ 🚀 今日飆股雷達 Top{len(picks)} ━━━━")
        if not picks:
            messages.append("今日無符合條件的飆股候選（市場偏弱或量能不足）")
        for p in picks:
            print(f"  判讀飆股 {p['code']} {p['name']}（momentum {p['score']}/{p['total']}）...")
            card = analyst.analyze(p["code"], p["name"])
            tag = f"〔momentum {p['score']}/{p['total']}｜當日 {p['change_pct']}%〕\n"
            messages.append(tag + card["text"])

    messages.append("——\n本戰報由 AI 自動彙整技術面＋籌碼面數據，僅供參考，非投資建議。")
    full_text = "\n\n".join(messages)
    return messages, full_text


def save_report(full_text):
    os.makedirs(REPORTS_DIR, exist_ok=True)
    path = os.path.join(REPORTS_DIR, datetime.now().strftime("%Y-%m-%d") + ".txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(full_text)
    return path


def main():
    no_push = "--no-push" in sys.argv
    cfg = load_config()

    print("=" * 50)
    print(f"  每日股市戰報  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 50)

    messages, full_text = build_report(cfg)
    path = save_report(full_text)
    print(f"\n✓ 戰報已存檔：{path}")

    if no_push:
        print("（--no-push：略過 Telegram 推播）")
    elif tg.configured(cfg):
        try:
            n = tg.send_all(messages, cfg)
            print(f"✓ 已推播 Telegram（{n} 則）")
        except Exception as e:
            print(f"✗ Telegram 推播失敗：{e}")
    else:
        print("⚠ 尚未設定 Telegram（缺 TELEGRAM_BOT_TOKEN 或 chat_id），僅存檔未推播")

    return 0


if __name__ == "__main__":
    sys.exit(main())
