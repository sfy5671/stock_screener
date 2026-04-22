"""
================================================================================
  飆股篩選器 v3.0 - 台股技術分析選股工具 (全上市股票版)
================================================================================
"""

import sys
import io
import os
import json
import requests
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
import time

# 修復 Windows 終端機 UTF-8 輸出（僅在直接執行時）
if sys.stdout and hasattr(sys.stdout, 'buffer'):
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    except Exception:
        pass

warnings.filterwarnings('ignore')

# ============================================================================
#  快取目錄
# ============================================================================
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache")
os.makedirs(CACHE_DIR, exist_ok=True)


# ============================================================================
#  批量預篩：從證交所一次抓全部當日行情��快速篩出候選股
# ============================================================================

def fetch_twse_daily_all():
    """從證交所 OpenAPI 抓全部上市股票的當日行情 (一次請��拿全部)"""
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    try:
        url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        r = requests.get(url, timeout=20, verify=False)
        data = r.json()
        result = []
        for item in data:
            code = str(item.get("Code", "")).strip()
            name = str(item.get("Name", "")).strip()
            if not code or not name or len(code) != 4 or not code.isdigit():
                continue
            try:
                close = float(item.get("ClosingPrice", 0))
                opening = float(item.get("OpeningPrice", 0))
                high = float(item.get("HighestPrice", 0))
                low = float(item.get("LowestPrice", 0))
                change = float(item.get("Change", 0))
                volume = int(item.get("TradeVolume", 0))
                value = int(item.get("TradeValue", 0))
                txn = int(item.get("Transaction", 0))
            except (ValueError, TypeError):
                continue
            if close <= 0 or volume <= 0:
                continue
            prev_close = close - change
            change_pct = (change / prev_close * 100) if prev_close > 0 else 0
            result.append({
                "code": code, "name": name,
                "open": opening, "high": high, "low": low,
                "close": close, "change": change, "change_pct": round(change_pct, 2),
                "volume": volume, "value": value, "txn": txn,
            })
        return result
    except Exception:
        return []


def fetch_tpex_daily_all():
    """從櫃買中心抓全部上櫃股票的當日行情"""
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    try:
        url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
        r = requests.get(url, timeout=20, verify=False)
        data = r.json()
        result = []
        for item in data:
            code = str(item.get("SecuritiesCompanyCode", "")).strip()
            name = str(item.get("CompanyName", "")).strip()
            if not code or not name or len(code) != 4 or not code.isdigit():
                continue
            try:
                close = float(item.get("Close", 0))
                opening = float(item.get("Open", 0))
                high = float(item.get("High", 0))
                low = float(item.get("Low", 0))
                change = float(item.get("Change", 0))
                volume = int(item.get("TradingShares", 0))
            except (ValueError, TypeError):
                continue
            if close <= 0 or volume <= 0:
                continue
            prev_close = close - change
            change_pct = (change / prev_close * 100) if prev_close > 0 else 0
            result.append({
                "code": code, "name": name,
                "open": opening, "high": high, "low": low,
                "close": close, "change": change, "change_pct": round(change_pct, 2),
                "volume": volume, "value": 0, "txn": 0,
            })
        return result
    except Exception:
        return []


def prescreen_all(min_price=10, min_volume=500, top_n=200):
    """
    快速預篩全市場股票（約2~3秒完成）。
    回傳依「活躍度」排序的候選股清單。
    min_price: 最低股價（過濾雞蛋水餃股）
    min_volume: 最低成交量（張），過濾冷門股
    top_n: 取前 N 檔進入深度分析
    """
    cache_file = os.path.join(CACHE_DIR, "daily_all.json")
    today_str = datetime.now().strftime("%Y-%m-%d")

    # 快取（每日同一天只抓一次）
    all_data = None
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cached = json.load(f)
            if cached.get("date") == today_str:
                all_data = cached.get("data", [])
        except Exception:
            pass

    if all_data is None:
        twse = fetch_twse_daily_all()
        tpex = fetch_tpex_daily_all()
        all_data = twse + tpex
        if all_data:
            try:
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump({"date": today_str, "data": all_data}, f, ensure_ascii=False)
            except Exception:
                pass

    # 過濾條件
    filtered = []
    for s in all_data:
        vol_lots = s["volume"] / 1000  # 張
        if s["close"] < min_price:
            continue
        if vol_lots < min_volume:
            continue
        # 計算活躍度分數 = |漲跌幅| + 量能加權
        activity = abs(s["change_pct"]) * 2 + min(vol_lots / 1000, 10)
        s["activity"] = round(activity, 2)
        filtered.append(s)

    # 依活躍度排序，取前 N 檔
    filtered.sort(key=lambda x: x["activity"], reverse=True)
    return filtered[:top_n]


# ============================================================================
#  取得全部上市/上櫃股票清單
# ============================================================================

def fetch_twse_stocks():
    """從證交所 OpenAPI 取得全部上市股票清單"""
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # 方法1: OpenAPI JSON
    try:
        url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        r = requests.get(url, timeout=15, verify=False)
        data = r.json()
        result = {}
        for item in data:
            code = str(item.get("Code", "")).strip()
            name = str(item.get("Name", "")).strip()
            if code and name and len(code) == 4 and code.isdigit():
                result[code] = name
        if len(result) > 100:
            return result
    except Exception:
        pass

    # 方法2: CSV open_data
    try:
        url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data"
        df = pd.read_csv(url, encoding='big5')
        result = {}
        for _, row in df.iterrows():
            code = str(row.iloc[1]).strip() if len(df.columns) > 2 else str(row.iloc[0]).strip()
            name = str(row.iloc[2]).strip() if len(df.columns) > 2 else str(row.iloc[1]).strip()
            if code and name and len(code) == 4 and code.isdigit():
                result[code] = name
        return result
    except Exception:
        return {}


def fetch_tpex_stocks():
    """從櫃買中心取得全部上櫃股票清單"""
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # 方法1: OpenAPI
    try:
        url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
        r = requests.get(url, timeout=15, verify=False)
        data = r.json()
        result = {}
        for item in data:
            code = str(item.get("SecuritiesCompanyCode", "")).strip()
            name = str(item.get("CompanyName", "")).strip()
            if not code:
                code = str(item.get("Code", "")).strip()
            if not name:
                name = str(item.get("Name", "")).strip()
            if code and name and len(code) == 4 and code.isdigit():
                result[code] = name
        if len(result) > 50:
            return result
    except Exception:
        pass

    # 方法2: 傳統 API
    try:
        today = datetime.now()
        tw_year = today.year - 1911
        date_str = f"{tw_year}/{today.month:02d}/{today.day:02d}"
        url = f"https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&d={date_str}&se=EW&_=1"
        r = requests.get(url, timeout=10, verify=False)
        data = r.json()
        result = {}
        for row in data.get("aaData", []):
            code = str(row[0]).strip()
            name = str(row[1]).strip()
            if code and name and len(code) == 4 and code.isdigit():
                result[code] = name
        return result
    except Exception:
        return {}


def get_all_stocks(force_refresh=False):
    """取得全部上市+上櫃股票，帶快取(每日更新)"""
    cache_file = os.path.join(CACHE_DIR, "all_stocks.json")
    today_str = datetime.now().strftime("%Y-%m-%d")

    # 讀取快取
    if not force_refresh and os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cached = json.load(f)
            if cached.get("date") == today_str and cached.get("stocks"):
                return cached["stocks"]
        except Exception:
            pass

    # 從網路抓取
    twse = fetch_twse_stocks()
    tpex = fetch_tpex_stocks()
    all_stocks = {**twse, **tpex}

    if all_stocks:
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump({"date": today_str, "stocks": all_stocks}, f, ensure_ascii=False)
        except Exception:
            pass

    # 如果抓取失敗，用內建的備用清單
    if not all_stocks:
        all_stocks = FALLBACK_STOCKS

    return all_stocks


# 備用清單（網路失敗時使用）
FALLBACK_STOCKS = {
    "2330": "台積電", "2303": "聯電", "2454": "聯發科",
    "3711": "日月光投控", "2379": "瑞昱", "3034": "聯詠",
    "6415": "矽力-KY", "2408": "南亞科",
    "2317": "鴻海", "3231": "緯創", "2382": "廣達",
    "2356": "英業達", "3017": "奇鋐", "6669": "緯穎",
    "2345": "智邦", "3036": "文曄",
    "2308": "台達電", "2327": "國巨", "3037": "欣興",
    "2383": "台光電", "6533": "晶心科",
    "2881": "富邦金", "2882": "國泰金", "2884": "玉山金",
    "2886": "兆豐金", "2891": "中信金",
    "2002": "中鋼", "1301": "台塑", "2912": "統一超",
    "2207": "和泰車", "9910": "豐泰",
    "6446": "藥華藥",
    "0050": "元大台灣50", "0056": "元大高股息",
}


# ============================================================================
#  技術指標計算
# ============================================================================

def calc_ma(close, period):
    return close.rolling(window=period).mean()

def calc_ema(close, period):
    return close.ewm(span=period, adjust=False).mean()

def calc_rsi(close, period=14):
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calc_kd(high, low, close, k_period=9, d_period=3):
    lowest_low = low.rolling(window=k_period).min()
    highest_high = high.rolling(window=k_period).max()
    rsv = (close - lowest_low) / (highest_high - lowest_low) * 100
    k = pd.Series(index=close.index, dtype=float)
    d = pd.Series(index=close.index, dtype=float)
    k.iloc[k_period - 1] = 50
    d.iloc[k_period - 1] = 50
    for i in range(k_period, len(close)):
        k.iloc[i] = 2 / 3 * k.iloc[i - 1] + 1 / 3 * rsv.iloc[i]
        d.iloc[i] = 2 / 3 * d.iloc[i - 1] + 1 / 3 * k.iloc[i]
    return k, d

def calc_macd(close, fast=12, slow=26, signal=9):
    ema_fast = calc_ema(close, fast)
    ema_slow = calc_ema(close, slow)
    dif = ema_fast - ema_slow
    macd_signal = calc_ema(dif, signal)
    histogram = (dif - macd_signal) * 2
    return dif, macd_signal, histogram

def calc_bollinger(close, period=20, std_dev=2):
    ma = close.rolling(window=period).mean()
    std = close.rolling(window=period).std()
    upper = ma + std_dev * std
    lower = ma - std_dev * std
    return upper, ma, lower

def calc_williams_r(high, low, close, period=14):
    """計算 Williams %R"""
    highest_high = high.rolling(window=period).max()
    lowest_low = low.rolling(window=period).min()
    wr = (highest_high - close) / (highest_high - lowest_low) * -100
    return wr


# ============================================================================
#  法人籌碼資料
# ============================================================================

def fetch_institutional_data():
    """從證交所取得三大法人每日買賣超"""
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    cache_file = os.path.join(CACHE_DIR, "institutional.json")
    today_str = datetime.now().strftime("%Y-%m-%d")

    # 快取
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cached = json.load(f)
            if cached.get("date") == today_str and cached.get("data"):
                return cached["data"]
        except Exception:
            pass

    result = {}
    # 上市：三大法人買賣超日報
    try:
        today_tw = datetime.now().strftime("%Y%m%d")
        url = f"https://www.twse.com.tw/fund/T86?response=json&date={today_tw}&selectType=ALL"
        r = requests.get(url, timeout=15, verify=False)
        data = r.json()
        for row in data.get("data", []):
            code = str(row[0]).strip()
            if len(code) != 4 or not code.isdigit():
                continue
            try:
                foreign = int(str(row[4]).replace(",", ""))   # 外資買賣超股數
                trust = int(str(row[10]).replace(",", ""))     # 投信買賣超股數
                dealer = int(str(row[11]).replace(",", ""))    # 自營商買賣超股數
            except (ValueError, IndexError):
                continue
            result[code] = {
                "foreign": foreign,
                "trust": trust,
                "dealer": dealer,
                "total": foreign + trust + dealer,
            }
    except Exception:
        pass

    if result:
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump({"date": today_str, "data": result}, f, ensure_ascii=False)
        except Exception:
            pass

    return result


# ============================================================================
#  篩選策略（基本7大 - 飆股模式）
# ============================================================================

def strategy_ma_bullish(df, **kw):
    """均線多頭排列 - MA5>MA10>MA20>MA60"""
    c = df['Close']
    ma5 = calc_ma(c, 5).iloc[-1]
    ma10 = calc_ma(c, 10).iloc[-1]
    ma20 = calc_ma(c, 20).iloc[-1]
    ma60 = calc_ma(c, 60).iloc[-1]
    return ma5 > ma10 > ma20 > ma60

def strategy_above_all_ma(df, **kw):
    """站上所有均線 - 收盤>MA5/10/20/60"""
    c = df['Close']
    price = c.iloc[-1]
    ma5 = calc_ma(c, 5).iloc[-1]
    ma10 = calc_ma(c, 10).iloc[-1]
    ma20 = calc_ma(c, 20).iloc[-1]
    ma60 = calc_ma(c, 60).iloc[-1]
    return price > ma5 and price > ma10 and price > ma20 and price > ma60

def strategy_kd_golden_cross(df, **kw):
    """KD黃金交叉 - 近5日K上穿D"""
    k, d = calc_kd(df['High'], df['Low'], df['Close'])
    for i in range(-5, 0):
        try:
            if (k.iloc[i - 1] < d.iloc[i - 1]) and (k.iloc[i] > d.iloc[i]):
                if k.iloc[i] < 80:
                    return True
        except (IndexError, KeyError):
            continue
    return False

def strategy_macd_bullish(df, **kw):
    """MACD翻多 - DIF上穿信號線"""
    dif, signal, _ = calc_macd(df['Close'])
    for i in range(-5, 0):
        try:
            if (dif.iloc[i - 1] < signal.iloc[i - 1]) and (dif.iloc[i] > signal.iloc[i]):
                return True
        except (IndexError, KeyError):
            continue
    return False

def strategy_rsi_strong(df, **kw):
    """RSI強勢區間 - 50~80"""
    rsi = calc_rsi(df['Close'])
    rsi_val = rsi.iloc[-1]
    return 50 <= rsi_val <= 80

def strategy_volume_breakout(df, **kw):
    """爆量突破 - 量>20日均量2倍"""
    vol = df['Volume']
    avg_vol_20 = vol.rolling(window=20).mean().iloc[-1]
    today_vol = vol.iloc[-1]
    return today_vol > avg_vol_20 * 2

def strategy_bollinger_breakout(df, **kw):
    """布林上軌突破"""
    upper, _, _ = calc_bollinger(df['Close'])
    return df['Close'].iloc[-1] > upper.iloc[-1]


# ============================================================================
#  進階篩選條件
# ============================================================================

def strategy_limit_up(df, days=5, **kw):
    """近N日內有漲停板 (漲幅>=9.5%)"""
    n = int(kw.get("days", days))
    close = df['Close']
    for i in range(-n, 0):
        try:
            prev = close.iloc[i - 1]
            curr = close.iloc[i]
            pct = (curr - prev) / prev * 100
            if pct >= 9.5:
                return True
        except (IndexError, KeyError):
            continue
    return False

def strategy_limit_down(df, days=5, **kw):
    """近N日內有跌停板 (跌幅<=-9.5%)"""
    n = int(kw.get("days", days))
    close = df['Close']
    for i in range(-n, 0):
        try:
            prev = close.iloc[i - 1]
            curr = close.iloc[i]
            pct = (curr - prev) / prev * 100
            if pct <= -9.5:
                return True
        except (IndexError, KeyError):
            continue
    return False

def strategy_consecutive_up(df, days=3, **kw):
    """連續N日上漲"""
    n = int(kw.get("days", days))
    close = df['Close']
    if len(close) < n + 1:
        return False
    for i in range(-n, 0):
        if close.iloc[i] <= close.iloc[i - 1]:
            return False
    return True

def strategy_consecutive_down(df, days=3, **kw):
    """連續N日下跌"""
    n = int(kw.get("days", days))
    close = df['Close']
    if len(close) < n + 1:
        return False
    for i in range(-n, 0):
        if close.iloc[i] >= close.iloc[i - 1]:
            return False
    return True

def strategy_price_new_high(df, days=60, **kw):
    """創N日新高"""
    n = int(kw.get("days", days))
    close = df['Close']
    if len(close) < n:
        return False
    current = close.iloc[-1]
    past_high = close.iloc[-n:-1].max()
    return current >= past_high

def strategy_volume_shrink(df, **kw):
    """量縮整理 - 今日量<20日均量50%"""
    vol = df['Volume']
    avg_vol_20 = vol.rolling(window=20).mean().iloc[-1]
    today_vol = vol.iloc[-1]
    return today_vol < avg_vol_20 * 0.5

def strategy_gap_up(df, **kw):
    """今日跳空上漲 - 今日最低>昨日最高"""
    if len(df) < 2:
        return False
    return df['Low'].iloc[-1] > df['High'].iloc[-2]

def strategy_ma_golden_cross(df, **kw):
    """均線黃金交叉 - 近5日5MA上穿20MA"""
    c = df['Close']
    ma5 = calc_ma(c, 5)
    ma20 = calc_ma(c, 20)
    for i in range(-5, 0):
        try:
            if (ma5.iloc[i - 1] < ma20.iloc[i - 1]) and (ma5.iloc[i] > ma20.iloc[i]):
                return True
        except (IndexError, KeyError):
            continue
    return False

def strategy_weekly_up_pct(df, min_pct=10, **kw):
    """近一週漲幅超過N%"""
    pct = float(kw.get("min_pct", min_pct))
    close = df['Close']
    if len(close) < 5:
        return False
    week_change = (close.iloc[-1] - close.iloc[-5]) / close.iloc[-5] * 100
    return week_change >= pct

def strategy_monthly_up_pct(df, min_pct=20, **kw):
    """近一月漲幅超過N%"""
    pct = float(kw.get("min_pct", min_pct))
    close = df['Close']
    if len(close) < 20:
        return False
    month_change = (close.iloc[-1] - close.iloc[-20]) / close.iloc[-20] * 100
    return month_change >= pct

def strategy_vol_price_up(df, **kw):
    """價漲量增 - 今日收紅且量>昨日量"""
    if len(df) < 2:
        return False
    return (df['Close'].iloc[-1] > df['Close'].iloc[-2] and
            df['Volume'].iloc[-1] > df['Volume'].iloc[-2])

def strategy_vol_price_diverge(df, **kw):
    """量價背離 - 價創5日新高但量萎縮（潛在反轉警示）"""
    close = df['Close']
    vol = df['Volume']
    if len(close) < 6:
        return False
    price_high = close.iloc[-1] >= close.iloc[-5:].max()
    vol_down = vol.iloc[-1] < vol.iloc[-5:].mean() * 0.7
    return price_high and vol_down

def strategy_vol_continuous_up(df, days=3, **kw):
    """連續量增 - 連續N日成交量逐日增加"""
    n = int(kw.get("days", days))
    vol = df['Volume']
    if len(vol) < n + 1:
        return False
    for i in range(-n, 0):
        if vol.iloc[i] <= vol.iloc[i - 1]:
            return False
    return True


# ============================================================================
#  高勝率策略
# ============================================================================

def strategy_rsi2_oversold(df, **kw):
    """RSI(2)超賣反彈 (回測勝率~91%) - RSI(2)<10 且股價在60MA之上"""
    close = df['Close']
    rsi2 = calc_rsi(close, period=2)
    ma60 = calc_ma(close, 60)
    return rsi2.iloc[-1] < 10 and close.iloc[-1] > ma60.iloc[-1]

def strategy_rsi2_overbought(df, **kw):
    """RSI(2)超買訊號 - RSI(2)>90，短線獲利了結"""
    close = df['Close']
    rsi2 = calc_rsi(close, period=2)
    return rsi2.iloc[-1] > 90

def strategy_williams_oversold(df, **kw):
    """Williams %R超賣反彈 (回測勝率~81%) - W%R<-80 且股價在60MA之上"""
    wr = calc_williams_r(df['High'], df['Low'], df['Close'], period=14)
    ma60 = calc_ma(df['Close'], 60)
    return wr.iloc[-1] < -80 and df['Close'].iloc[-1] > ma60.iloc[-1]

def strategy_bollinger_lower_bounce(df, **kw):
    """布林下軌反彈 - 股價觸及或跌破下軌後反彈（均值回歸）"""
    upper, mid, lower = calc_bollinger(df['Close'])
    close = df['Close']
    if len(close) < 3:
        return False
    # 前2日曾觸碰下軌，且今日反彈收在下軌之上
    touched = close.iloc[-3] <= lower.iloc[-3] or close.iloc[-2] <= lower.iloc[-2]
    bounced = close.iloc[-1] > lower.iloc[-1]
    return touched and bounced

def strategy_multi_indicator_resonance(df, **kw):
    """多指標共振 (高勝率) - RSI+MACD+KD+布林 至少3個同時翻多"""
    signals = 0
    close = df['Close']

    # RSI 由超賣區回升 (近3日曾<30，現在>30)
    rsi = calc_rsi(close)
    if any(rsi.iloc[i] < 30 for i in range(-3, -1)) and rsi.iloc[-1] > 30:
        signals += 1

    # MACD 柱狀體翻正
    _, _, osc = calc_macd(close)
    if osc.iloc[-1] > 0 and osc.iloc[-2] <= 0:
        signals += 1

    # KD 黃金交叉
    k, d = calc_kd(df['High'], df['Low'], close)
    if k.iloc[-1] > d.iloc[-1] and k.iloc[-2] <= d.iloc[-2]:
        signals += 1

    # 股價回到布林中軌之上
    _, mid, lower = calc_bollinger(close)
    if close.iloc[-1] > mid.iloc[-1] and close.iloc[-2] <= mid.iloc[-2]:
        signals += 1

    return signals >= 3

def strategy_foreign_net_buy(df, code=None, **kw):
    """外資買超 - 今日外資淨買超"""
    if not code:
        return False
    inst = fetch_institutional_data()
    info = inst.get(code)
    if not info:
        return False
    return info["foreign"] > 0

def strategy_trust_net_buy(df, code=None, **kw):
    """投信買超 - 今日投信淨買超"""
    if not code:
        return False
    inst = fetch_institutional_data()
    info = inst.get(code)
    if not info:
        return False
    return info["trust"] > 0

def strategy_institutional_consensus(df, code=None, **kw):
    """法人同步買超 - 外資+投信今日都買超（法人共識看多）"""
    if not code:
        return False
    inst = fetch_institutional_data()
    info = inst.get(code)
    if not info:
        return False
    return info["foreign"] > 0 and info["trust"] > 0


# ============================================================================
#  當沖策略
# ============================================================================

def strategy_daytrade_volume(df, **kw):
    """當沖量能門檻 - 日成交量>3000張"""
    vol = df['Volume'].iloc[-1]
    return vol > 3000 * 1000  # 3000張

def strategy_daytrade_volatility(df, **kw):
    """盤中振幅夠大 - 當日振幅>3%"""
    high = df['High'].iloc[-1]
    low = df['Low'].iloc[-1]
    if low <= 0:
        return False
    amplitude = (high - low) / low * 100
    return amplitude > 3

def strategy_daytrade_opening_breakout(df, **kw):
    """開盤突破 - 今日開盤>昨日最高"""
    if len(df) < 2:
        return False
    return df['Open'].iloc[-1] > df['High'].iloc[-2]

def strategy_daytrade_momentum(df, **kw):
    """短線動能 - 5分鐘級漲幅>1.5%且量增"""
    close = df['Close']
    vol = df['Volume']
    if len(close) < 2:
        return False
    chg = (close.iloc[-1] - close.iloc[-2]) / close.iloc[-2] * 100
    vol_up = vol.iloc[-1] > vol.iloc[-2] if len(vol) > 1 else False
    return chg > 1.5 and vol_up

def strategy_daytrade_above_vwap(df, **kw):
    """站上均價線 - 收盤價在VWAP之上(多方控盤)"""
    close = df['Close']
    vol = df['Volume']
    # 簡易VWAP = 累計(成交金額) / 累計(成交量)
    typical = (df['High'] + df['Low'] + close) / 3
    cum_tv = (typical * vol).cumsum()
    cum_vol = vol.cumsum()
    vwap = cum_tv / cum_vol
    return close.iloc[-1] > vwap.iloc[-1]

def strategy_daytrade_tick_spread(df, **kw):
    """價格活躍 - 最近K棒有明顯價格跳動(非冷門股)"""
    if len(df) < 5:
        return False
    recent = df.tail(5)
    spreads = recent['High'] - recent['Low']
    avg_spread_pct = (spreads / recent['Low']).mean() * 100
    return avg_spread_pct > 1.0

def strategy_daytrade_new_high(df, **kw):
    """盤中創高 - 今日最高價>前5日最高"""
    if len(df) < 6:
        return False
    today_high = df['High'].iloc[-1]
    past_high = df['High'].iloc[-6:-1].max()
    return today_high > past_high


# ============================================================================
#  隔日沖策略
# ============================================================================

def strategy_swing_close_near_high(df, **kw):
    """收盤接近最高 - (收盤-最低)/(最高-最低)>80%"""
    c = df['Close'].iloc[-1]
    h = df['High'].iloc[-1]
    l = df['Low'].iloc[-1]
    if h == l:
        return False
    return (c - l) / (h - l) > 0.8

def strategy_swing_strong_close(df, **kw):
    """強勢收盤 - 收盤漲幅>3%且收在上半根K棒"""
    if len(df) < 2:
        return False
    close = df['Close']
    chg = (close.iloc[-1] - close.iloc[-2]) / close.iloc[-2] * 100
    h = df['High'].iloc[-1]
    l = df['Low'].iloc[-1]
    mid = (h + l) / 2
    return chg > 3 and close.iloc[-1] > mid

def strategy_swing_volume_surge(df, **kw):
    """尾盤爆量 - 成交量>5日均量3倍(主力進場訊號)"""
    vol = df['Volume']
    avg5 = vol.rolling(5).mean().iloc[-1]
    return vol.iloc[-1] > avg5 * 3 if avg5 > 0 else False

def strategy_swing_limit_up_open(df, **kw):
    """漲停或接近漲停 - 漲幅>=9%(隔日有續漲動能)"""
    if len(df) < 2:
        return False
    close = df['Close']
    chg = (close.iloc[-1] - close.iloc[-2]) / close.iloc[-2] * 100
    return chg >= 9.0

def strategy_swing_gap_potential(df, **kw):
    """跳空潛力 - 收盤創5日新高+量增+收紅"""
    if len(df) < 6:
        return False
    close = df['Close']
    vol = df['Volume']
    new_high = close.iloc[-1] >= close.iloc[-6:-1].max()
    vol_up = vol.iloc[-1] > vol.iloc[-2]
    up_day = close.iloc[-1] > close.iloc[-2]
    return new_high and vol_up and up_day

def strategy_swing_institutional_buy(df, code=None, **kw):
    """法人同步買 - 外資+投信同日買超(隔日容易續漲)"""
    if not code:
        return False
    inst = fetch_institutional_data()
    info = inst.get(code)
    if not info:
        return False
    return info["foreign"] > 0 and info["trust"] > 0

def strategy_swing_breakout_close(df, **kw):
    """突破收盤 - 收盤突破20日最高價(波段啟動)"""
    if len(df) < 21:
        return False
    close = df['Close']
    past_high = close.iloc[-21:-1].max()
    return close.iloc[-1] > past_high


# ============================================================================
#  策略字典（五種模式）
# ============================================================================

# === 飆股模式：追漲動能 ===
MOMENTUM_STRATEGIES = {
    "均線多頭排列": strategy_ma_bullish,
    "站上所有均線": strategy_above_all_ma,
    "KD黃金交叉":  strategy_kd_golden_cross,
    "MACD翻多":    strategy_macd_bullish,
    "RSI強勢區間":  strategy_rsi_strong,
    "爆量突破":     strategy_volume_breakout,
    "布林上軌突破":  strategy_bollinger_breakout,
}

# === 高勝率模式：均值回歸+法人共識 ===
HIGHWIN_STRATEGIES = {
    "RSI(2)超賣反彈":  strategy_rsi2_oversold,
    "W%R超賣反彈":     strategy_williams_oversold,
    "布林下軌反彈":    strategy_bollinger_lower_bounce,
    "多指標共振":      strategy_multi_indicator_resonance,
    "外資買超":        strategy_foreign_net_buy,
    "投信買超":        strategy_trust_net_buy,
    "法人同步買超":    strategy_institutional_consensus,
}

# === 當沖模式 ===
DAYTRADE_STRATEGIES = {
    "當沖量能門檻":  strategy_daytrade_volume,
    "振幅夠大":      strategy_daytrade_volatility,
    "開盤突破":      strategy_daytrade_opening_breakout,
    "短線動能":      strategy_daytrade_momentum,
    "站上均價線":    strategy_daytrade_above_vwap,
    "價格活躍":      strategy_daytrade_tick_spread,
    "盤中創高":      strategy_daytrade_new_high,
}

# === 隔日沖模式 ===
SWING_STRATEGIES = {
    "收盤接近最高":  strategy_swing_close_near_high,
    "強勢收盤":      strategy_swing_strong_close,
    "尾盤爆量":      strategy_swing_volume_surge,
    "接近漲停":      strategy_swing_limit_up_open,
    "跳空潛力":      strategy_swing_gap_potential,
    "法人同步買":    strategy_swing_institutional_buy,
    "突破收盤":      strategy_swing_breakout_close,
}

# 向下相容
BASE_STRATEGIES = MOMENTUM_STRATEGIES

# 進階篩選條件（所有模式共用）
ADVANCED_STRATEGIES = {
    "近期漲停板":    strategy_limit_up,
    "近期跌停板":    strategy_limit_down,
    "連續上漲":      strategy_consecutive_up,
    "連續下跌":      strategy_consecutive_down,
    "創新高":        strategy_price_new_high,
    "量縮整理":      strategy_volume_shrink,
    "跳空上漲":      strategy_gap_up,
    "均線黃金交叉":  strategy_ma_golden_cross,
    "週漲幅達標":    strategy_weekly_up_pct,
    "月漲幅達標":    strategy_monthly_up_pct,
    "價漲量增":      strategy_vol_price_up,
    "量價背離":      strategy_vol_price_diverge,
    "連續量增":      strategy_vol_continuous_up,
}

ALL_STRATEGIES = {**MOMENTUM_STRATEGIES, **HIGHWIN_STRATEGIES, **DAYTRADE_STRATEGIES, **SWING_STRATEGIES, **ADVANCED_STRATEGIES}

# 模式對照表
MODE_STRATEGIES = {
    "momentum": MOMENTUM_STRATEGIES,
    "highwin":  HIGHWIN_STRATEGIES,
    "daytrade": DAYTRADE_STRATEGIES,
    "swing":    SWING_STRATEGIES,
}

# 進階策略的參數定義
ADVANCED_PARAMS = {
    "近期漲停板":   {"days": {"label": "天數", "default": 5, "min": 1, "max": 30}},
    "近期跌停板":   {"days": {"label": "天數", "default": 5, "min": 1, "max": 30}},
    "連續上漲":     {"days": {"label": "連續天數", "default": 3, "min": 2, "max": 10}},
    "連續下跌":     {"days": {"label": "連續天數", "default": 3, "min": 2, "max": 10}},
    "創新高":       {"days": {"label": "天數", "default": 60, "min": 5, "max": 240}},
    "週漲幅達標":   {"min_pct": {"label": "最低漲幅%", "default": 10, "min": 1, "max": 50}},
    "月漲幅達標":   {"min_pct": {"label": "最低漲幅%", "default": 20, "min": 1, "max": 100}},
    "連續量增":     {"days": {"label": "連續天數", "default": 3, "min": 2, "max": 10}},
}


# ============================================================================
#  即時報價（盤中用，來自證交所即時 API）
# ============================================================================

def get_realtime_quotes(codes):
    """
    從證交所即時 API 取得多檔股票即時報價。
    codes: ['2330','2317',...]
    回傳: {code: {price, change, change_pct, volume, high, low, open, time, name}, ...}
    """
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if not codes:
        return {}

    # 組合查詢參數 (每次最多50檔)
    result = {}
    for batch_start in range(0, len(codes), 50):
        batch = codes[batch_start:batch_start + 50]
        ex_ch = "|".join(f"tse_{c}.tw" for c in batch)
        try:
            url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}"
            r = requests.get(url, timeout=10, verify=False)
            data = r.json()
            for item in data.get("msgArray", []):
                code = item.get("c", "")
                try:
                    price = float(item.get("z", "0").replace("-", "0"))
                    yesterday = float(item.get("y", "0"))
                    high = float(item.get("h", "0").replace("-", "0"))
                    low = float(item.get("l", "0").replace("-", "0"))
                    opening = float(item.get("o", "0").replace("-", "0"))
                    volume = int(item.get("v", "0")) * 1000  # 張 -> 股
                    name = item.get("n", "")
                    t = item.get("t", "")

                    if price <= 0:
                        price = yesterday
                    change = price - yesterday if yesterday > 0 else 0
                    change_pct = (change / yesterday * 100) if yesterday > 0 else 0

                    result[code] = {
                        "price": round(price, 2),
                        "change": round(change, 2),
                        "change_pct": round(change_pct, 2),
                        "volume": volume,
                        "high": round(high, 2),
                        "low": round(low, 2),
                        "open": round(opening, 2),
                        "yesterday": round(yesterday, 2),
                        "time": t,
                        "name": name,
                    }
                except (ValueError, TypeError):
                    continue
        except Exception:
            continue

    return result


def is_market_open():
    """判斷台股是否開盤中 (週一~五 9:00~13:30)"""
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    h, m = now.hour, now.minute
    if h == 9 and m >= 0:
        return True
    if 10 <= h <= 12:
        return True
    if h == 13 and m <= 30:
        return True
    return False


def get_market_index():
    """取得大盤加權指數 + 台指期即時資料"""
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    result = {}

    # 大盤加權指數
    try:
        url = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_t00.tw"
        r = requests.get(url, timeout=8, verify=False)
        d = r.json()
        for item in d.get("msgArray", []):
            price = float(item.get("z", "0").replace("-", "0"))
            yesterday = float(item.get("y", "0"))
            if price <= 0:
                price = yesterday
            change = price - yesterday
            change_pct = (change / yesterday * 100) if yesterday > 0 else 0
            result["taiex"] = {
                "name": "加權指數",
                "price": round(price, 2),
                "change": round(change, 2),
                "change_pct": round(change_pct, 2),
                "high": float(item.get("h", "0").replace("-", "0")),
                "low": float(item.get("l", "0").replace("-", "0")),
                "time": item.get("t", ""),
            }
    except Exception:
        pass

    # 台指期
    try:
        url = "https://mis.taifex.com.tw/futures/api/getQuoteList"
        r = requests.post(url, json={"MarketType": "0", "CID": "TXF"},
                          timeout=8, verify=False)
        d = r.json()
        rows = d.get("RtData", {}).get("QuoteList", [])
        if rows:
            # 第一筆是台指期近月
            item = rows[0]
            price = float(item.get("CLastPrice", "0"))
            diff = float(item.get("CDiff", "0"))
            yesterday = price - diff
            pct = (diff / yesterday * 100) if yesterday > 0 else 0
            result["futures"] = {
                "name": item.get("DispCName", "台指期"),
                "price": round(price, 2),
                "change": round(diff, 2),
                "change_pct": round(pct, 2),
                "volume": item.get("CTotalVolume", ""),
                "time": item.get("CTime", ""),
            }
    except Exception:
        pass

    return result


# ============================================================================
#  資料取得
# ============================================================================

def get_stock_data(ticker, period="6mo"):
    """下載股票歷史資料"""
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period=period)
        if df.empty or len(df) < 60:
            return None
        return df
    except Exception:
        return None


def get_chart_data(ticker, period="6mo", interval="1d"):
    """取得 K 線圖資料 (OHLCV)，支援不同週期"""
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period=period, interval=interval)
        if df.empty:
            return None

        records = []
        for idx, row in df.iterrows():
            ts = int(idx.timestamp())
            records.append({
                "time": ts,
                "open": round(float(row["Open"]), 2),
                "high": round(float(row["High"]), 2),
                "low": round(float(row["Low"]), 2),
                "close": round(float(row["Close"]), 2),
                "volume": int(row["Volume"]),
            })
        return records
    except Exception:
        return None


# ============================================================================
#  K線圖買賣訊號計算
# ============================================================================

def calc_chart_signals(df):
    """
    掃描歷史 K 線，回傳高可靠度進場/離場訊號。
    規則：
    1. 至少 2 個指標同時確認才觸發（避免雜訊）
    2. 買進需在上升趨勢中（股價 > MA60），賣出需在高檔區
    3. 買賣衝突時不標記（方向不明就不動）
    4. 冷卻期：同方向訊號間隔至少 3 根 K 棒
    """
    if df is None or len(df) < 60:
        return []

    close = df['Close']
    high = df['High']
    low = df['Low']
    vol = df['Volume']

    # 預計算所有指標
    rsi2 = calc_rsi(close, period=2)
    rsi14 = calc_rsi(close, period=14)
    k, d_val = calc_kd(high, low, close)
    dif, macd_sig, osc = calc_macd(close)
    upper, mid, lower = calc_bollinger(close)
    wr = calc_williams_r(high, low, close, period=14)
    ma5 = calc_ma(close, 5)
    ma20 = calc_ma(close, 20)
    ma60 = calc_ma(close, 60)
    avg_vol_20 = vol.rolling(window=20).mean()

    signals = []
    last_buy_i = -99   # 上次買進訊號的位置
    last_sell_i = -99   # 上次賣出訊號的位置
    COOLDOWN = 3        # 同方向最少間隔 K 棒數

    for i in range(60, len(df)):
        ts = int(df.index[i].timestamp())
        buy_count = 0
        sell_count = 0

        try:
            chg = (close.iloc[i] - close.iloc[i-1]) / close.iloc[i-1] * 100
            in_uptrend = close.iloc[i] > ma60.iloc[i]     # 大趨勢向上
            in_downtrend = close.iloc[i] < ma20.iloc[i]    # 短期趨勢向下

            # ===== 買進條件計數 =====

            # KD 黃金交叉（低檔區才有意義）
            if k.iloc[i-1] < d_val.iloc[i-1] and k.iloc[i] > d_val.iloc[i] and k.iloc[i] < 50:
                buy_count += 1

            # MACD 柱狀體翻正
            if osc.iloc[i-1] <= 0 and osc.iloc[i] > 0:
                buy_count += 1

            # RSI(2) 超賣反彈 + 大趨勢向上
            if rsi2.iloc[i-1] < 10 and rsi2.iloc[i] > 10 and in_uptrend:
                buy_count += 1

            # Williams %R 超賣反彈 + 大趨勢向上
            if wr.iloc[i-1] < -80 and wr.iloc[i] > -80 and in_uptrend:
                buy_count += 1

            # 布林下軌反彈
            if close.iloc[i-1] <= lower.iloc[i-1] and close.iloc[i] > lower.iloc[i] and in_uptrend:
                buy_count += 1

            # 5MA 上穿 20MA（且在60MA上方）
            if ma5.iloc[i-1] < ma20.iloc[i-1] and ma5.iloc[i] > ma20.iloc[i] and in_uptrend:
                buy_count += 1

            # 爆量長紅（漲>3% 且量>20日均量2倍）
            if avg_vol_20.iloc[i] > 0:
                if chg > 3 and vol.iloc[i] > avg_vol_20.iloc[i] * 2:
                    buy_count += 1

            # ===== 賣出條件計數 =====

            # KD 死亡交叉（高檔區才有意義）
            if k.iloc[i-1] > d_val.iloc[i-1] and k.iloc[i] < d_val.iloc[i] and k.iloc[i] > 50:
                sell_count += 1

            # MACD 柱狀體翻負
            if osc.iloc[i-1] >= 0 and osc.iloc[i] < 0:
                sell_count += 1

            # RSI(2) 超買
            if rsi2.iloc[i-1] < 90 and rsi2.iloc[i] > 90:
                sell_count += 1

            # 跌破布林上軌回落
            if close.iloc[i-1] > upper.iloc[i-1] and close.iloc[i] < upper.iloc[i]:
                sell_count += 1

            # 5MA 下穿 20MA
            if ma5.iloc[i-1] > ma20.iloc[i-1] and ma5.iloc[i] < ma20.iloc[i]:
                sell_count += 1

            # 爆量長黑（跌>3% 且量>20日均量2倍）
            if avg_vol_20.iloc[i] > 0:
                if chg < -3 and vol.iloc[i] > avg_vol_20.iloc[i] * 2:
                    sell_count += 1

        except (IndexError, KeyError):
            continue

        # === 判定規則 ===
        # 1. 至少 2 個條件同時確認
        has_buy = buy_count >= 2
        has_sell = sell_count >= 2

        # 2. 買賣衝突 → 都不標（方向不明）
        if has_buy and has_sell:
            continue

        # 3. 冷卻期檢查
        if has_buy and (i - last_buy_i) >= COOLDOWN:
            signals.append({
                "time": ts,
                "position": "belowBar",
                "color": "#ff5252",
                "shape": "arrowUp",
                "text": "",
            })
            last_buy_i = i

        if has_sell and (i - last_sell_i) >= COOLDOWN:
            signals.append({
                "time": ts,
                "position": "aboveBar",
                "color": "#00e676",
                "shape": "arrowDown",
                "text": "",
            })
            last_sell_i = i

    return signals


# ============================================================================
#  分析計算
# ============================================================================

def calc_score_and_details(df, mode="momentum", code=None,
                           extra_strategies=None, extra_params=None):
    """
    計算策略得分與技術指標數值。
    mode: momentum / highwin / daytrade / swing / combined
    """
    results = {}
    mode_scores = {}  # {mode_name: (score, total)}

    # 根據模式決定要跑哪些策略組
    if mode == "combined":
        groups = {
            "momentum": MOMENTUM_STRATEGIES,
            "highwin": HIGHWIN_STRATEGIES,
            "daytrade": DAYTRADE_STRATEGIES,
            "swing": SWING_STRATEGIES,
        }
    else:
        groups = {mode: MODE_STRATEGIES.get(mode, MOMENTUM_STRATEGIES)}

    for gname, strats in groups.items():
        sc = 0
        for name, func in strats.items():
            try:
                passed = func(df, code=code)
                results[name] = passed
                if passed:
                    sc += 1
            except Exception:
                results[name] = False
        mode_scores[gname] = (sc, len(strats))

    # 進階篩選
    extra_score = 0
    if extra_strategies:
        params = extra_params or {}
        for sname in extra_strategies:
            func = ADVANCED_STRATEGIES.get(sname)
            if func:
                try:
                    p = params.get(sname, {})
                    passed = func(df, **p)
                    results[sname] = passed
                    if passed:
                        extra_score += 1
                except Exception:
                    results[sname] = False

    # 計算指標數值
    close = df['Close']
    price = close.iloc[-1]
    prev_price = close.iloc[-2] if len(close) > 1 else price
    change_pct = (price - prev_price) / prev_price * 100

    rsi = calc_rsi(close).iloc[-1]
    k, d = calc_kd(df['High'], df['Low'], df['Close'])
    dif, signal, osc = calc_macd(close)

    vol = df['Volume']
    today_vol = vol.iloc[-1]
    prev_vol = vol.iloc[-2] if len(vol) > 1 else today_vol
    avg_vol_5 = vol.rolling(window=5).mean().iloc[-1]
    avg_vol_20 = vol.rolling(window=20).mean().iloc[-1]
    vol_ratio = today_vol / avg_vol_20 if avg_vol_20 > 0 else 0
    vol_change_pct = (today_vol - prev_vol) / prev_vol * 100 if prev_vol > 0 else 0
    vol_5d_ratio = today_vol / avg_vol_5 if avg_vol_5 > 0 else 0

    # 5日量能趨勢 (正=量增, 負=量縮)
    vol_trend_5d = 0
    if len(vol) >= 6:
        vol_trend_5d = (avg_vol_5 - vol.iloc[-6:-1].mean()) / vol.iloc[-6:-1].mean() * 100 if vol.iloc[-6:-1].mean() > 0 else 0

    ma5 = calc_ma(close, 5).iloc[-1]
    ma10 = calc_ma(close, 10).iloc[-1]
    ma20 = calc_ma(close, 20).iloc[-1]
    ma60 = calc_ma(close, 60).iloc[-1]

    # 週漲跌幅
    week_change = 0
    if len(close) >= 5:
        week_change = (close.iloc[-1] - close.iloc[-5]) / close.iloc[-5] * 100

    # 月漲跌幅
    month_change = 0
    if len(close) >= 20:
        month_change = (close.iloc[-1] - close.iloc[-20]) / close.iloc[-20] * 100

    details = {
        "price": price,
        "change_pct": change_pct,
        "week_change": week_change,
        "month_change": month_change,
        "rsi": rsi,
        "k": k.iloc[-1],
        "d": d.iloc[-1],
        "dif": dif.iloc[-1],
        "macd_signal": signal.iloc[-1],
        "osc": osc.iloc[-1],
        "vol_ratio": vol_ratio,           # 今量/20日均量
        "vol_change_pct": vol_change_pct,  # 今量 vs 昨量 %
        "vol_5d_ratio": vol_5d_ratio,      # 今量/5日均量
        "vol_trend_5d": vol_trend_5d,      # 5日量能趨勢 %
        "volume": today_vol,
        "ma5": ma5,
        "ma10": ma10,
        "ma20": ma20,
        "ma60": ma60,
    }

    # 計算總分
    base_score = sum(s for s, t in mode_scores.values())
    base_total = sum(t for s, t in mode_scores.values())
    total = base_total + len(extra_strategies or [])
    score = base_score + extra_score

    # 各模式子分數
    for mname in ("momentum", "highwin", "daytrade", "swing"):
        s, t = mode_scores.get(mname, (0, 0))
        details[f"{mname}_score"] = s
        details[f"{mname}_total"] = t

    # 法人資料
    inst = fetch_institutional_data()
    inst_info = inst.get(code, {})
    details["foreign_net"] = inst_info.get("foreign", 0)
    details["trust_net"] = inst_info.get("trust", 0)
    details["dealer_net"] = inst_info.get("dealer", 0)

    return score, total, results, details
