# -*- coding: utf-8 -*-
"""
一鍵設定 Telegram 推播（給宋先生自己跑一次即可）。

前置：
  1. 在手機/電腦 Telegram 搜尋 @BotFather → 傳 /newbot → 依指示取名 →
     它會給你一組 token（形如 123456:ABC-DEF...）
  2. 在 Telegram 搜尋你剛建的 bot → 點 START → 隨便傳一句「hi」給它

然後執行（把 token 換成你的）：
  py -3.11 daily_report/setup_telegram.py 123456:ABC-DEF你的token

腳本會：抓出你的 chat_id 寫進 report_config.json → 用 setx 把 token
存進 Windows 使用者環境變數 TELEGRAM_BOT_TOKEN → 發一則測試訊息。
"""
import os
import sys
import json
import subprocess
import requests

try:  # Windows 終端 cp950 印 emoji 會炸，強制 UTF-8 輸出
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "report_config.json")


def main():
    if len(sys.argv) < 2:
        print("用法：py -3.11 daily_report/setup_telegram.py <你的BOT_TOKEN>")
        return 1
    token = sys.argv[1].strip()

    # 1. 抓 chat_id
    print("→ 讀取你剛剛傳給 bot 的訊息，找出 chat_id...")
    r = requests.get(f"https://api.telegram.org/bot{token}/getUpdates", timeout=30)
    data = r.json()
    if not data.get("ok"):
        print(f"✗ token 可能不對，Telegram 回應：{data}")
        return 1
    updates = data.get("result", [])
    if not updates:
        print("✗ 找不到訊息。請先在 Telegram 對你的 bot 點 START 並傳一句話，再跑一次。")
        return 1
    chat_id = str(updates[-1]["message"]["chat"]["id"])
    print(f"✓ 找到 chat_id：{chat_id}")

    # 2. 寫進 config
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = json.load(f)
    cfg.setdefault("telegram", {})["chat_id"] = chat_id
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    print(f"✓ 已寫入 {CONFIG_PATH}")

    # 3. setx token 進 Windows 使用者環境變數
    subprocess.run(["setx", "TELEGRAM_BOT_TOKEN", token], check=False,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("✓ 已將 TELEGRAM_BOT_TOKEN 存進 Windows 使用者環境變數")

    # 4. 發測試訊息
    resp = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                         json={"chat_id": chat_id,
                               "text": "✅ 飆股戰報 Telegram 串接成功！以後每天盤後會在這裡收到戰報。"},
                         timeout=30)
    if resp.json().get("ok"):
        print("✓ 已發送測試訊息，去 Telegram 看看有沒有收到！")
        print("\n全部完成。之後每天盤後排程會自動推播戰報。")
        return 0
    print(f"✗ 測試訊息發送失敗：{resp.json()}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
