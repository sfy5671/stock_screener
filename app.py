"""
飆股篩選器 v3.0 - Flask Web 應用 (PWA)
支援: 全上市/上櫃股票篩選, K線圖, 日/周/月線切換, 進階篩選條件
"""

from flask import Flask, render_template, jsonify, request
import stock_screener as ss
import numpy as np
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)


def sf(val):
    """safe float - 將 numpy/pandas 數值轉為 JSON 可序列化的 float"""
    if val is None:
        return None
    try:
        f = float(val)
        if np.isnan(f) or np.isinf(f):
            return None
        return round(f, 2)
    except (TypeError, ValueError):
        return None


# ============================================================================
#  頁面
# ============================================================================

@app.route("/")
def index():
    return render_template("index.html")


# ============================================================================
#  API: 股票清單
# ============================================================================

@app.route("/api/all-stocks")
def get_all_stocks():
    """回傳全部上市+上櫃股票清單"""
    stocks = ss.get_all_stocks()
    result = [{"code": c, "name": n} for c, n in sorted(stocks.items())]
    return jsonify({"stocks": result, "count": len(result)})


@app.route("/api/prescreen", methods=["POST"])
def prescreen():
    """
    第一階段：快速預篩全市場 (約2~3秒)。
    從證交所/櫃買中心批量抓當日行情，不需逐一查詢。
    """
    data = request.get_json() or {}
    min_price = float(data.get("min_price", 10))
    min_volume = int(data.get("min_volume", 500))
    top_n = int(data.get("top_n", 200))

    candidates = ss.prescreen_all(min_price=min_price, min_volume=min_volume, top_n=top_n)

    return jsonify({
        "candidates": candidates,
        "count": len(candidates),
    })


@app.route("/api/search-stock")
def search_stock():
    """搜尋股票（代號或名稱模糊搜尋）"""
    q = request.args.get("q", "").strip()
    if not q or len(q) < 1:
        return jsonify([])
    stocks = ss.get_all_stocks()
    results = []
    for code, name in stocks.items():
        if q in code or q in name:
            results.append({"code": code, "name": name})
        if len(results) >= 30:
            break
    return jsonify(results)


# ============================================================================
#  API: 進階策略定義
# ============================================================================

@app.route("/api/strategies")
def get_strategies():
    """回傳可用的進階篩選策略及其參數"""
    base = [{"key": k, "label": k} for k in ss.BASE_STRATEGIES]
    advanced = []
    for k in ss.ADVANCED_STRATEGIES:
        params = ss.ADVANCED_PARAMS.get(k, {})
        advanced.append({"key": k, "label": k, "params": params})
    return jsonify({"base": base, "advanced": advanced})


# ============================================================================
#  API: 篩選
# ============================================================================

@app.route("/api/screen", methods=["POST"])
def screen_stocks():
    """篩選股票 API（多線程加速版）"""
    data = request.get_json()
    stock_list = data.get("stocks", [])
    top_n = int(data.get("top_n", 0))
    mode = data.get("mode", "momentum")
    extra_strategies = data.get("extra_strategies", [])
    extra_params = data.get("extra_params", {})

    if not stock_list:
        return jsonify({"error": "請至少加入一檔股票"}), 400

    # --- 單檔分析函式（給線程池用） ---
    def analyze_one(item):
        code = str(item.get("code", "")).strip()
        name = item.get("name", code)
        if not code:
            return None, None

        ticker = f"{code}.TW"
        df = ss.get_stock_data(ticker)
        if df is None:
            ticker = f"{code}.TWO"
            df = ss.get_stock_data(ticker)
        if df is None:
            return None, f"{code} {name}"

        try:
            score, total, strat_results, details = ss.calc_score_and_details(
                df, mode=mode, code=code,
                extra_strategies=extra_strategies, extra_params=extra_params
            )
        except Exception:
            return None, f"{code} {name}"

        matched = [s for s, v in strat_results.items() if v]

        if extra_strategies:
            if not any(strat_results.get(s, False) for s in extra_strategies):
                return None, None

        return {
            "code": code, "name": name,
            "price": sf(details["price"]),
            "change_pct": sf(details["change_pct"]),
            "week_change": sf(details["week_change"]),
            "month_change": sf(details["month_change"]),
            "rsi": sf(details["rsi"]),
            "k": sf(details["k"]), "d": sf(details["d"]),
            "dif": sf(details["dif"]),
            "macd_signal": sf(details["macd_signal"]),
            "osc": sf(details["osc"]),
            "vol_ratio": sf(details["vol_ratio"]),
            "vol_change_pct": sf(details["vol_change_pct"]),
            "vol_5d_ratio": sf(details["vol_5d_ratio"]),
            "vol_trend_5d": sf(details["vol_trend_5d"]),
            "volume": sf(details["volume"]),
            "ma5": sf(details["ma5"]), "ma10": sf(details["ma10"]),
            "ma20": sf(details["ma20"]), "ma60": sf(details["ma60"]),
            "score": score, "total": total,
            "momentum_score": details.get("momentum_score", 0),
            "momentum_total": details.get("momentum_total", 0),
            "highwin_score": details.get("highwin_score", 0),
            "highwin_total": details.get("highwin_total", 0),
            "daytrade_score": details.get("daytrade_score", 0),
            "daytrade_total": details.get("daytrade_total", 0),
            "swing_score": details.get("swing_score", 0),
            "swing_total": details.get("swing_total", 0),
            "foreign_net": sf(details.get("foreign_net", 0)),
            "trust_net": sf(details.get("trust_net", 0)),
            "matched": matched,
            "strategies": {s: bool(v) for s, v in strat_results.items()},
        }, None

    # --- 多線程並行分析 ---
    results = []
    errors = []
    workers = min(8, len(stock_list))  # 最多 8 線程

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(analyze_one, item): item for item in stock_list}
        for future in as_completed(futures):
            result, error = future.result()
            if result:
                results.append(result)
            if error:
                errors.append(error)

    results.sort(key=lambda x: x["score"], reverse=True)
    if top_n > 0:
        results = results[:top_n]

    return jsonify({
        "results": results,
        "errors": errors,
        "count": len(results),
    })


# ============================================================================
#  API: 即時報價 + 市場狀態
# ============================================================================

@app.route("/api/realtime", methods=["POST"])
def realtime_quotes():
    """取得多檔股票即時報價（盤中用）"""
    data = request.get_json() or {}
    codes = data.get("codes", [])
    if not codes:
        return jsonify({})
    quotes = ss.get_realtime_quotes(codes)
    return jsonify({"quotes": quotes, "market_open": ss.is_market_open()})


@app.route("/api/market-status")
def market_status():
    """取得市場開盤狀態 + 大盤/台指期指數"""
    idx = ss.get_market_index()
    return jsonify({
        "market_open": ss.is_market_open(),
        "index": idx,
    })


# ============================================================================
#  API: K線圖資料
# ============================================================================

@app.route("/api/chart/<code>")
def get_chart(code):
    """取得個股 K 線圖資料 (支援 1日/1週/1月/1年)"""
    key = request.args.get("period", "1mo")
    # period = yfinance 抓取範圍, interval = K棒週期
    period_map = {
        "1m":  {"period": "1d",  "interval": "1m"},    # 1分K (當日)
        "5m":  {"period": "1d",  "interval": "5m"},    # 5分K (當日)
        "15m": {"period": "5d",  "interval": "15m"},   # 15分K (5日)
        "30m": {"period": "10d", "interval": "30m"},   # 30分K (10日)
        "1d":  {"period": "3mo", "interval": "1d"},    # 日K (3個月)
        "1wk": {"period": "2y",  "interval": "1wk"},   # 週K (2年)
    }
    cfg = period_map.get(key, period_map["1d"])

    # 大盤指數用 ^TWII
    if code in ("t00", "TWII", "twii"):
        ticker = "^TWII"
        data = ss.get_chart_data(ticker, period=cfg["period"], interval=cfg["interval"])
    else:
        ticker = f"{code}.TW"
        data = ss.get_chart_data(ticker, period=cfg["period"], interval=cfg["interval"])
        if data is None:
            ticker = f"{code}.TWO"
            data = ss.get_chart_data(ticker, period=cfg["period"], interval=cfg["interval"])

    if data is None:
        return jsonify({"error": "無法取得資料"}), 404

    # 計算均線（用於疊加在圖上）
    closes = [d["close"] for d in data]
    ma_data = {}
    for ma_period in [5, 10, 20, 60]:
        ma_vals = []
        for i in range(len(closes)):
            if i < ma_period - 1:
                ma_vals.append(None)
            else:
                avg = sum(closes[i - ma_period + 1: i + 1]) / ma_period
                ma_vals.append(round(avg, 2))
        ma_data[f"ma{ma_period}"] = [
            {"time": data[i]["time"], "value": v}
            for i, v in enumerate(ma_vals) if v is not None
        ]

    # 計算買賣訊號（所有週期都計算）
    signals = []
    try:
        import yfinance as yf_sig
        sig_ticker = f"{code}.TW"
        sig_stock = yf_sig.Ticker(sig_ticker)
        # 訊號一律用日K計算（指標最準確）
        sig_df = sig_stock.history(period="1y", interval="1d")
        if sig_df is None or sig_df.empty:
            sig_stock = yf_sig.Ticker(f"{code}.TWO")
            sig_df = sig_stock.history(period="1y", interval="1d")
        if sig_df is not None and len(sig_df) >= 60:
            raw_signals = ss.calc_chart_signals(sig_df)
            min_time = data[0]["time"] if data else 0
            max_time = data[-1]["time"] if data else float('inf')

            if cfg["interval"] in ("5m", "30m"):
                # 分鐘K：將日K訊號映射到當天的第一根分鐘K
                from datetime import datetime as dt, timezone, timedelta
                tw = timezone(timedelta(hours=8))
                day_times = {}  # date_str -> first candle time
                for c in data:
                    ds = dt.fromtimestamp(c["time"], tz=tw).strftime("%Y-%m-%d")
                    if ds not in day_times:
                        day_times[ds] = c["time"]
                for s in raw_signals:
                    ds = dt.fromtimestamp(s["time"], tz=tw).strftime("%Y-%m-%d")
                    if ds in day_times:
                        mapped = dict(s)
                        mapped["time"] = day_times[ds]
                        signals.append(mapped)
            else:
                signals = [s for s in raw_signals if min_time <= s["time"] <= max_time]
    except Exception:
        pass

    return jsonify({
        "candles": data,
        "ma": ma_data,
        "signals": signals,
        "code": code,
    })


# ============================================================================
#  啟動
# ============================================================================

import os

if __name__ == "__main__":
    # 預先載入股票清單
    print("\n" + "=" * 50)
    print("  飆股篩選器 v3.0 啟動中...")
    stocks = ss.get_all_stocks()
    print(f"  已載入 {len(stocks)} 檔上市/上櫃股票")
    port = int(os.environ.get("PORT", 5000))
    print(f"  電腦: http://localhost:{port}")
    print(f"  手機: http://你的區網IP:{port}")
    print("=" * 50 + "\n")
    app.run(host="0.0.0.0", port=port, debug=True)
