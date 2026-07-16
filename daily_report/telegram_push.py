# -*- coding: utf-8 -*-
"""
Telegram 推播 —— 把每日戰報各段訊息推到宋先生手機。
BOT_TOKEN 走環境變數 TELEGRAM_BOT_TOKEN；chat_id 走 env TELEGRAM_CHAT_ID 或 config。
"""
import os
import time
import requests

TG_LIMIT = 4000  # Telegram 單則上限 4096，留餘裕


def token():
    return os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()


def chat_id(cfg):
    return (os.environ.get("TELEGRAM_CHAT_ID", "").strip()
            or cfg.get("telegram", {}).get("chat_id", "").strip())


def configured(cfg):
    return bool(token() and chat_id(cfg))


def _split(text):
    """超過上限的長訊息，按字元切段（盡量在換行處切）。"""
    if len(text) <= TG_LIMIT:
        return [text]
    parts, buf = [], ""
    for line in text.split("\n"):
        if len(buf) + len(line) + 1 > TG_LIMIT:
            if buf:
                parts.append(buf)
            buf = line
        else:
            buf = f"{buf}\n{line}" if buf else line
    if buf:
        parts.append(buf)
    return parts


def send_one(text, cid, tok):
    url = f"https://api.telegram.org/bot{tok}/sendMessage"
    r = requests.post(url, json={
        "chat_id": cid, "text": text,
        "disable_web_page_preview": True,
    }, timeout=30)
    r.raise_for_status()
    return r.json()


def send_all(messages, cfg):
    """把多則訊息依序推出。回傳成功則數。"""
    tok, cid = token(), chat_id(cfg)
    if not (tok and cid):
        raise RuntimeError("Telegram 未設定（缺 TELEGRAM_BOT_TOKEN 或 chat_id）")
    sent = 0
    for msg in messages:
        for part in _split(msg):
            send_one(part, cid, tok)
            sent += 1
            time.sleep(0.4)  # 避免觸發 Telegram 限流
    return sent


if __name__ == "__main__":
    # 測試連線：python daily_report/telegram_push.py
    import json
    cfg_path = os.path.join(os.path.dirname(__file__), "report_config.json")
    with open(cfg_path, encoding="utf-8") as f:
        cfg = json.load(f)
    if not configured(cfg):
        print("尚未設定 Telegram（需 env TELEGRAM_BOT_TOKEN + config chat_id）")
    else:
        n = send_all(["✅ 飆股戰報 Telegram 測試訊息，收到代表串接成功！"], cfg)
        print(f"已送出 {n} 則")
