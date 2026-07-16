# -*- coding: utf-8 -*-
"""
AI 判讀引擎 —— 把 stock_screener 算好的技術+籌碼資料包，
交給 Claude 產出「大師級」白話判讀，供每日戰報使用。

用法:
    from daily_report import analyst
    card = analyst.analyze("2308", "台達電")
    print(card["text"])
"""
import os
import sys
import json
import requests

# 讓子目錄能 import 父目錄的 stock_screener
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
import stock_screener as ss

# Claude API：只讀環境變數，不 hard-code（見記憶 anthropic-api-key）
API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
API_URL = "https://api.anthropic.com/v1/messages"
MODEL = os.environ.get("REPORT_MODEL", "claude-sonnet-4-6")

SYSTEM_PROMPT = (
    "你是頂尖台股技術分析大師，專長技術面與籌碼面判讀。"
    "你會拿到某一檔台股「今日盤後」的量化數據，請據此產出精簡、白話、"
    "能讓程式初學者看懂的每日戰報。嚴格要求：\n"
    "1. 只根據提供的數據判讀，不得杜撰新聞或財報。\n"
    "2. 數據不足時直說「資料不足」，不要硬掰。\n"
    "3. 全程繁體中文（台灣用語）。\n"
    "4. 結尾務必附一行風險提醒：本判讀為技術面資訊整理，非投資建議。\n"
    "5. 控制長度：整份 250~400 字，適合手機閱讀。"
)

# 要求 Claude 用固定小標，方便 Telegram 呈現
USER_TMPL = """請判讀以下個股，用這個格式（每項一行、精簡）：

📊 {name}（{code}）
定調：（一句話：多方/空方/整理，附信心強弱）
均線：（5/20/60日多空排列，現價位置）
量能：（放量/量縮，代表什麼）
籌碼：（外資/投信近日動向）
關卡：支撐 xxx／壓力 xxx
操作：（觀察方向，含關鍵觀察價；非投資建議）

=== 今日量化數據 ===
現價 {price}｜漲跌 {change_pct}%｜週 {week_change}%｜月 {month_change}%
MA5 {ma5}｜MA20 {ma20}｜MA60 {ma60}｜乖離20日 {bias_20}%
RSI {rsi}｜KD K{k}/D{d}｜MACD 柱 {osc}
量能比(今/20日均) {vol_ratio}｜5日量能趨勢 {vol_5d_ratio}
外資今日買賣超(張) {foreign_net_lots}｜外資連續買超天數 {foreign_consec}｜外資5日累計(張) {foreign_cum_5d_lots}
投信連續買超天數 {trust_consec}｜融資餘額 {margin_balance}｜融資增減 {margin_change}
本益比 {pe}｜殖利率 {yield_}%｜產業 {industry}
（momentum 策略符合 {score}/{total} 項）
"""


def _lots(shares):
    """股數轉張數（1 張 = 1000 股），None 安全處理。"""
    try:
        return round(float(shares) / 1000)
    except (TypeError, ValueError):
        return "N/A"


def _fmt(v, nd=1):
    try:
        return round(float(v), nd)
    except (TypeError, ValueError):
        return "N/A"


def build_context(code, name, df):
    """算出 details 並組成餵給 Claude 的 prompt。回傳 (prompt, details) 或 (None, None)。"""
    try:
        score, total, _sr, d = ss.calc_score_and_details(df, mode="momentum", code=code)
    except Exception as e:
        print(f"  [analyst] {code} 計算失敗: {e}")
        return None, None

    prompt = USER_TMPL.format(
        name=name, code=code,
        price=_fmt(d.get("price")), change_pct=_fmt(d.get("change_pct"), 2),
        week_change=_fmt(d.get("week_change"), 2), month_change=_fmt(d.get("month_change"), 2),
        ma5=_fmt(d.get("ma5")), ma20=_fmt(d.get("ma20")), ma60=_fmt(d.get("ma60")),
        bias_20=_fmt(d.get("bias_20"), 2),
        rsi=_fmt(d.get("rsi")), k=_fmt(d.get("k")), d=_fmt(d.get("d")),
        osc=_fmt(d.get("osc"), 2),
        vol_ratio=_fmt(d.get("vol_ratio"), 2), vol_5d_ratio=_fmt(d.get("vol_5d_ratio"), 2),
        foreign_net_lots=_lots(d.get("foreign_net")), foreign_consec=d.get("foreign_consec", 0),
        foreign_cum_5d_lots=_lots(d.get("foreign_cum_5d")),
        trust_consec=d.get("trust_consec", 0),
        margin_balance=_fmt(d.get("margin_balance"), 0), margin_change=_fmt(d.get("margin_change"), 0),
        pe=_fmt(d.get("pe"), 2), yield_=_fmt(d.get("yield"), 2), industry=d.get("industry", "N/A"),
        score=score, total=total,
    )
    return prompt, d


def call_claude(prompt, max_tokens=700):
    if not API_KEY:
        raise RuntimeError("環境變數 ANTHROPIC_API_KEY 未設定")
    resp = requests.post(
        API_URL,
        headers={
            "x-api-key": API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": MODEL,
            "max_tokens": max_tokens,
            "system": SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=90,
    )
    resp.raise_for_status()
    data = resp.json()
    return "".join(block.get("text", "") for block in data.get("content", []))


def analyze(code, name=None):
    """對單一個股產出判讀卡片。回傳 dict：{code, name, text, ok}。"""
    name = name or code
    ticker = f"{code}.TW"
    df = ss.get_stock_data(ticker)
    if df is None:
        df = ss.get_stock_data(f"{code}.TWO")
    if df is None:
        return {"code": code, "name": name, "ok": False,
                "text": f"⚠️ {name}（{code}）取不到日K資料，略過"}

    prompt, _d = build_context(code, name, df)
    if prompt is None:
        return {"code": code, "name": name, "ok": False,
                "text": f"⚠️ {name}（{code}）技術指標計算失敗，略過"}

    try:
        text = call_claude(prompt).strip()
    except Exception as e:
        return {"code": code, "name": name, "ok": False,
                "text": f"⚠️ {name}（{code}）AI 判讀失敗：{e}"}

    return {"code": code, "name": name, "ok": True, "text": text}


if __name__ == "__main__":
    # 測試：python daily_report/analyst.py 2308 台達電
    _code = sys.argv[1] if len(sys.argv) > 1 else "2308"
    _name = sys.argv[2] if len(sys.argv) > 2 else None
    print(f"model = {MODEL}")
    r = analyze(_code, _name)
    print("=" * 50)
    print(r["text"])
    print("=" * 50)
    print("ok =", r["ok"])
