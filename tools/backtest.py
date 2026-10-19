"""
飆股篩選器 — 策略回測器
================================================================================
目的：用歷史資料驗證 stock_screener.py 裡每一支策略「到底有沒有效」。

設計重點（為什麼可以相信這份結果）：
1. **直接呼叫原始策略函式**，不重寫向量化版本 —— 避免「回測邏輯 ≠ 線上邏輯」。
2. **無未來資訊**：訊號在第 t 日收盤產生，第 t+1 日「開盤」才進場，第 t+N 日收盤出場。
3. **一定跟 baseline 比**：baseline = 同一批股票、同一段期間、隨便挑一天進場的平均報酬。
   策略贏不過 baseline，就等於沒有選股能力 —— 這是最容易被略過、也最會打臉的一欄。

用法：
    python -m tools.backtest                 # 預設 3 年、40 檔
    python -m tools.backtest --years 5 --horizon 10
"""
import os, sys, json, argparse, warnings, pickle
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
import yfinance as yf

import stock_screener as ss

CACHE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.cache')
os.makedirs(CACHE, exist_ok=True)

# 回測宇宙：權值 + 中型 + 常見投機標的，混合避免只測大型股而高估穩定度
UNIVERSE = [
    '2330','2317','2454','2308','2382','2412','2881','2882','2891','2886',
    '1301','1303','2002','1216','2207','2603','2609','2615','3711','3034',
    '3008','2379','2357','4938','6505','1101','2105','9910','2409','2408',
    '3037','3231','2376','6415','8046','3443','5269','6669','1590','2327',
]

WINDOW = 160          # 餵給策略的回看視窗（最長指標 MA60，160 綽綽有餘）
WARMUP = 170


def load_prices(years, force=False):
    cache_f = os.path.join(CACHE, f'backtest_prices_{years}y.pkl')
    if os.path.exists(cache_f) and not force:
        with open(cache_f, 'rb') as f:
            return pickle.load(f)
    data = {}
    for i, code in enumerate(UNIVERSE, 1):
        for suffix in ('.TW', '.TWO'):
            try:
                df = yf.Ticker(code + suffix).history(period=f'{years}y', auto_adjust=True)
                if len(df) > WARMUP + 30:
                    df = df[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                    data[code] = df
                    break
            except Exception:
                continue
        print(f'  [{i}/{len(UNIVERSE)}] {code}: {len(data.get(code, []))} 根K', flush=True)
    with open(cache_f, 'wb') as f:
        pickle.dump(data, f)
    return data


# 這幾支靠「今日」法人/籌碼即時資料，yfinance 沒有對應歷史 → 無法用本回測器驗證。
# 不是略過不管，而是改由 tools/chip_monitor.py 走證交所歷史資料另外處理。
NOT_BACKTESTABLE = {
    '連續法人買超3日': '需歷史三大法人買賣超，yfinance 無此資料',
    '外資買超':       '需歷史三大法人買賣超，yfinance 無此資料',
    '投信買超':       '需歷史三大法人買賣超，yfinance 無此資料',
    '法人同步買超':   '需歷史三大法人買賣超，yfinance 無此資料',
    '法人同步買':     '需歷史三大法人買賣超，yfinance 無此資料',
}


def probe_strategies(sample_df):
    """逐支策略試跑一次，剔除會報錯或需要即時外部資料（法人/融資）的策略。
    這些策略不是不好，是 yfinance 沒有對應的歷史資料可回測 —— 誠實標示而非假裝測過。"""
    ok, skipped = {}, {}
    win = sample_df.iloc[-WINDOW:]
    for name, fn in ss.ALL_STRATEGIES.items():
        if name in NOT_BACKTESTABLE:
            skipped[name] = NOT_BACKTESTABLE[name]
            continue
        try:
            r = fn(win)
            if isinstance(r, (bool, np.bool_)):
                ok[name] = fn
            else:
                skipped[name] = f'回傳非 bool ({type(r).__name__})'
        except Exception as e:
            skipped[name] = f'{type(e).__name__}: {str(e)[:60]}'
    return ok, skipped


def run(years=3, horizons=(5, 10, 20), force=False):
    print(f'== 載入歷史資料（{years} 年，{len(UNIVERSE)} 檔）==', flush=True)
    data = load_prices(years, force)
    print(f'   實際取得 {len(data)} 檔\n', flush=True)
    if not data:
        print('沒有資料，中止'); return

    sample = next(iter(data.values()))
    strategies, skipped = probe_strategies(sample)
    print(f'== 可回測策略 {len(strategies)} 支；略過 {len(skipped)} 支 ==')
    for n, why in skipped.items():
        print(f'   - {n}: {why}')
    print(flush=True)

    maxh = max(horizons)
    recs = []        # 策略訊號的報酬
    baseline = []    # 所有交易日的報酬（不挑日子）

    for si, (code, df) in enumerate(data.items(), 1):
        o = df['Open'].to_numpy(); c = df['Close'].to_numpy()
        n = len(df)
        for t in range(WARMUP, n - maxh - 1):
            entry = o[t + 1]
            if not np.isfinite(entry) or entry <= 0:
                continue
            rets = {h: (c[t + h] / entry - 1.0) for h in horizons}
            baseline.append(rets)
            win = df.iloc[t - WINDOW + 1: t + 1]
            for name, fn in strategies.items():
                try:
                    if fn(win):
                        recs.append({'strategy': name, 'code': code,
                                     'date': df.index[t].date().isoformat(), **rets})
                except Exception:
                    continue
        print(f'  [{si}/{len(data)}] {code} 完成，累計訊號 {len(recs)}', flush=True)

    if not recs:
        print('無任何訊號'); return

    sig = pd.DataFrame(recs)
    base = pd.DataFrame(baseline)

    rows = []
    for name in strategies:
        s = sig[sig.strategy == name]
        if len(s) == 0:
            continue
        row = {'策略': name, '訊號次數': len(s),
               '訊號密度%': round(100 * len(s) / len(base), 2)}
        for h in horizons:
            r = s[h]; b = base[h]
            row[f'{h}日勝率%'] = round(100 * (r > 0).mean(), 1)
            row[f'{h}日均報酬%'] = round(100 * r.mean(), 2)
            row[f'{h}日中位數%'] = round(100 * r.median(), 2)
            # 超額 = 策略均報酬 − 不挑日子的均報酬。這欄 ≤0 就是沒有選股能力。
            row[f'{h}日超額%'] = round(100 * (r.mean() - b.mean()), 2)
            # t 檢定：訊號報酬是否顯著不同於 baseline
            se = r.std(ddof=1) / np.sqrt(len(r)) if len(r) > 1 else np.nan
            row[f'{h}日t值'] = round((r.mean() - b.mean()) / se, 2) if se and se > 0 else np.nan
        rows.append(row)

    out = pd.DataFrame(rows).sort_values(f'{horizons[1]}日超額%', ascending=False)

    stamp = datetime.now().strftime('%Y%m%d_%H%M')
    outdir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'backtest_results')
    os.makedirs(outdir, exist_ok=True)
    csv_p = os.path.join(outdir, f'backtest_{stamp}.csv')
    out.to_csv(csv_p, index=False, encoding='utf-8-sig')

    meta = {'跑測時間': stamp, '期間年數': years, '股票檔數': len(data),
            '總交易日樣本': len(base), '可回測策略': len(strategies),
            '略過策略': skipped,
            'baseline均報酬%': {f'{h}日': round(100 * base[h].mean(), 3) for h in horizons},
            'baseline勝率%': {f'{h}日': round(100 * (base[h] > 0).mean(), 1) for h in horizons}}
    with open(os.path.join(outdir, f'backtest_{stamp}_meta.json'), 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print('\n' + '=' * 90)
    print('baseline（隨便挑一天進場）：', meta['baseline均報酬%'], meta['baseline勝率%'])
    print('=' * 90)
    cols = ['策略', '訊號次數', '訊號密度%'] + [f'{h}日{k}' for h in horizons for k in ('勝率%', '均報酬%', '超額%', 't值')]
    print(out[[c for c in cols if c in out.columns]].to_string(index=False))
    print(f'\n已輸出：{csv_p}')
    return out, meta


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--years', type=int, default=3)
    ap.add_argument('--horizon', type=int, nargs='*', default=[5, 10, 20])
    ap.add_argument('--force', action='store_true')
    a = ap.parse_args()
    run(a.years, tuple(a.horizon), a.force)
