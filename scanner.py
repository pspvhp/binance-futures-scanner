import sys
import os
import json
import csv
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler

sys.stdout.reconfigure(encoding='utf-8')

# Global cache
CACHE_DATA = None
LAST_SCAN_TIME = None
IS_SCANNING = False

def get_exchange_symbols():
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read().decode('utf-8'))
        return [
            s['symbol'] for s in data['symbols']
            if s['quoteAsset'] == 'USDT' and s['contractType'] == 'PERPETUAL' and s['status'] == 'TRADING'
        ]

def get_ticker_24hr():
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read().decode('utf-8'))
        return {
            item['symbol']: {
                'lastPrice': float(item['lastPrice']),
                'quoteVolume': float(item.get('quoteVolume', 0))
            } for item in data
        }

def get_funding_rates():
    """Fetch live Binance funding rates in 1 fast call"""
    try:
        url = "https://fapi.binance.com/fapi/v1/premiumIndex"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            return {
                item['symbol']: float(item.get('lastFundingRate', 0)) * 100
                for item in data
            }
    except Exception:
        return {}

def get_market_caps():
    mc_map = {}
    
    # 1. Primary: Binance Composite Market Cap API
    try:
        url = 'https://www.binance.com/bapi/composite/v1/public/marketing/symbol/list'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        data = json.loads(urllib.request.urlopen(req, timeout=6).read()).get('data', [])
        for item in data:
            base = item.get('baseAsset', '').upper()
            sym = item.get('symbol', '').upper()
            mc = item.get('marketCap')
            if base and mc: mc_map[base] = float(mc)
            if sym and mc: mc_map[sym] = float(mc)
    except Exception:
        pass

    # 2. Fallback: CoinMarketCap Public Listing (covers top 1,500 coins missing on Binance)
    try:
        url = 'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing?start=1&limit=1500&sortBy=market_cap&sortType=desc'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        d = json.loads(urllib.request.urlopen(req, timeout=7).read())
        for c in d.get('data', {}).get('cryptoCurrencyList', []):
            sym = c.get('symbol', '').upper()
            quotes = c.get('quotes', [])
            mc = quotes[0].get('marketCap') if quotes else None
            if sym and mc and sym not in mc_map:
                mc_map[sym] = float(mc)
    except Exception:
        pass

    # 3. Fallback: CoinGecko Top Coins (covers additional niche tokens)
    try:
        url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=1'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        cg = json.loads(urllib.request.urlopen(req, timeout=5).read())
        for item in cg:
            sym = item.get('symbol', '').upper()
            mc = item.get('market_cap')
            if sym and mc and sym not in mc_map:
                mc_map[sym] = float(mc)
    except Exception:
        pass

    return mc_map

def fetch_symbol_international(symbol):
    encoded_sym = urllib.parse.quote(symbol)
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={encoded_sym}&interval=1d&limit=185"
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            if data and len(data) > 0:
                cur_price = float(data[-1][4])
                now_dt = datetime.fromtimestamp(data[-1][0] / 1000.0, tz=timezone.utc)

                # 1D: Today's 00:00 UTC candle open
                open_1d = float(data[-1][1])

                # 1W & 2W: Mondays (weekday == 0)
                mondays = [x for x in data if datetime.fromtimestamp(x[0] / 1000.0, tz=timezone.utc).weekday() == 0]
                open_1w = float(mondays[-1][1]) if mondays else float(data[0][1])
                open_2w = float(mondays[-2][1]) if len(mondays) >= 2 else float(data[0][1])

                # Helper for 1st of month (UTC 00:00)
                def get_month_open(months_back):
                    y = now_dt.year
                    m = now_dt.month - months_back
                    while m <= 0:
                        m += 12
                        y -= 1
                    month_candles = [
                        x for x in data
                        if datetime.fromtimestamp(x[0] / 1000.0, tz=timezone.utc).year == y and
                           datetime.fromtimestamp(x[0] / 1000.0, tz=timezone.utc).month == m
                    ]
                    if month_candles:
                        return float(month_candles[0][1])
                    return float(data[0][1])

                open_1m = get_month_open(0) # 1M (1st day of current month)
                open_3m = get_month_open(2) # 3M (1st day of 3 months ago)
                open_6m = get_month_open(5) # 6M (1st day of 6 months ago)

                def calc_pct(open_p):
                    return ((cur_price - open_p) / open_p) * 100 if open_p > 0 else 0

                pct_1d = calc_pct(open_1d)
                pct_1w = calc_pct(open_1w)
                pct_2w = calc_pct(open_2w)
                pct_1m = calc_pct(open_1m)
                pct_3m = calc_pct(open_3m)
                pct_6m = calc_pct(open_6m)

                return symbol, cur_price, {
                    '1d': {'open': open_1d, 'pct': pct_1d, 'is_above': cur_price > open_1d},
                    '1w': {'open': open_1w, 'pct': pct_1w, 'is_above': cur_price > open_1w},
                    '2w': {'open': open_2w, 'pct': pct_2w, 'is_above': cur_price > open_2w},
                    '1m': {'open': open_1m, 'pct': pct_1m, 'is_above': cur_price > open_1m},
                    '3m': {'open': open_3m, 'pct': pct_3m, 'is_above': cur_price > open_3m},
                    '6m': {'open': open_6m, 'pct': pct_6m, 'is_above': cur_price > open_6m},
                    'candles_count': len(data)
                }, None
            return symbol, 0, {}, "No kline data"
    except Exception as e:
        return symbol, 0, {}, str(e)

def run_scan():
    global IS_SCANNING, CACHE_DATA, LAST_SCAN_TIME
    IS_SCANNING = True

    symbols = get_exchange_symbols()
    tickers = get_ticker_24hr()
    mc_map = get_market_caps()
    fr_map = get_funding_rates()

    results = []
    failed = []

    with ThreadPoolExecutor(max_workers=25) as executor:
        futures = {executor.submit(fetch_symbol_international, sym): sym for sym in symbols}
        for fut in as_completed(futures):
            sym, cur_price, tf_data, err = fut.result()
            if err or not tf_data:
                failed.append({'symbol': sym, 'error': str(err)})
            else:
                ticker_info = tickers.get(sym, {'lastPrice': cur_price, 'quoteVolume': 0})
                latest_price = ticker_info['lastPrice'] or cur_price
                volume_24h = ticker_info['quoteVolume']

                base = sym.replace('USDT', '')
                clean_base = base
                for prefix in ['1000000', '100000', '10000', '1000', '100']:
                    if clean_base.startswith(prefix) and len(clean_base) > len(prefix):
                        clean_base = clean_base[len(prefix):]
                        break

                ALIASES = {'LUNA2': 'LUNA', 'DODOX': 'DODO'}
                alt_base = ALIASES.get(base, ALIASES.get(clean_base, ''))
                market_cap = mc_map.get(base, mc_map.get(clean_base, mc_map.get(alt_base, mc_map.get(sym, 0))))
                funding_rate = fr_map.get(sym, 0.0)

                green_count = sum(1 for tf in ['1d', '1w', '2w', '1m', '3m', '6m'] if tf_data[tf]['pct'] > 0)

                results.append({
                    'symbol': sym,
                    'base': base,
                    'current_price': latest_price,
                    'volume_24h': volume_24h,
                    'market_cap': market_cap,
                    'funding_rate': funding_rate,
                    'pct_1d': tf_data['1d']['pct'],
                    'open_1d': tf_data['1d']['open'],
                    'above_1d': tf_data['1d']['is_above'],
                    'pct_1w': tf_data['1w']['pct'],
                    'open_1w': tf_data['1w']['open'],
                    'above_1w': tf_data['1w']['is_above'],
                    'pct_2w': tf_data['2w']['pct'],
                    'open_2w': tf_data['2w']['open'],
                    'above_2w': tf_data['2w']['is_above'],
                    'pct_1m': tf_data['1m']['pct'],
                    'open_1m': tf_data['1m']['open'],
                    'above_1m': tf_data['1m']['is_above'],
                    'pct_3m': tf_data['3m']['pct'],
                    'open_3m': tf_data['3m']['open'],
                    'above_3m': tf_data['3m']['is_above'],
                    'pct_6m': tf_data['6m']['pct'],
                    'open_6m': tf_data['6m']['open'],
                    'above_6m': tf_data['6m']['is_above'],
                    'green_count': green_count,
                    'candles_count': tf_data['candles_count']
                })

    results.sort(key=lambda x: x['pct_1w'], reverse=True)

    scan_payload = {
        'scan_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'standard': 'International UTC 00:00 (Binance / TradingView)',
        'total_scanned': len(results),
        'failed_count': len(failed),
        'tokens': results
    }

    CACHE_DATA = scan_payload
    LAST_SCAN_TIME = scan_payload['scan_time']
    IS_SCANNING = False
    return scan_payload

def export_to_csv(data, filter_tf='1w', filter_cond='all', min_vol=0, dist_filter='all', min_mc=0, max_mc=0, fr_filter='all'):
    filename = f"binance_utc_{filter_tf}_{filter_cond}.csv"
    tokens = data['tokens']

    # Filter by condition
    if filter_cond == 'above':
        tokens = [t for t in tokens if t[f'above_{filter_tf}']]
    elif filter_cond == 'below':
        tokens = [t for t in tokens if not t[f'above_{filter_tf}']]

    # Filter by distance
    if dist_filter != 'all':
        def match_dist(t):
            val = abs(t[f'pct_{filter_tf}'])
            if dist_filter == 'near_3': return val <= 3
            if dist_filter == 'near_7': return 3 < val <= 7
            if dist_filter == 'mid_15': return 7 < val <= 15
            if dist_filter == 'far_15': return val > 15
            return True
        tokens = [t for t in tokens if match_dist(t)]

    # Filter by Market Cap
    if min_mc > 0:
        tokens = [t for t in tokens if t['market_cap'] >= min_mc]
    if max_mc > 0:
        tokens = [t for t in tokens if 0 < t['market_cap'] <= max_mc]

    # Filter by Funding Rate
    if fr_filter == 'neg':
        tokens = [t for t in tokens if t['funding_rate'] < 0]
    elif fr_filter == 'neg_deep':
        tokens = [t for t in tokens if t['funding_rate'] < -0.05]
    elif fr_filter == 'pos':
        tokens = [t for t in tokens if t['funding_rate'] > 0]
    elif fr_filter == 'pos_high':
        tokens = [t for t in tokens if t['funding_rate'] > 0.03]

    if min_vol > 0:
        tokens = [t for t in tokens if t['volume_24h'] >= min_vol]

    tf_labels = {
        '1d': 'Hôm Nay (1D UTC)',
        '1w': 'Nến Tuần (1W UTC)',
        '2w': 'Nến 2 Tuần (2W UTC)',
        '1m': 'Nến Tháng (1M UTC)',
        '3m': 'Nến Quý (3M UTC)',
        '6m': 'Nến 6 Tháng (6M UTC)'
    }
    cur_label = tf_labels.get(filter_tf, filter_tf)

    with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow([
            "STT", "Symbol", "Token", "Giá Hiện Tại (USDT)", "Funding Rate (%)", "Market Cap (USDT)",
            f"Giá Open UTC ({cur_label})", f"Cách Open UTC (%)",
            "% 1D (Hôm nay)", "% 1W (Tuần này)", "% 2W (2 Tuần)", "% 1M (Tháng này)", "% 3M (Quý)", "% 6M (Nửa năm)",
            "Mở Cửa 1D", "Mở Cửa 1W", "Mở Cửa 2W", "Mở Cửa 1M", "Mở Cửa 3M", "Mở Cửa 6M",
            "Volume 24h (USDT)", "Số Khung Tăng (0-6)"
        ])
        for idx, t in enumerate(tokens, 1):
            open_val = t[f'open_{filter_tf}']
            diff_pct = t[f'pct_{filter_tf}']
            mc_str = f"{t['market_cap']:,.0f}" if t['market_cap'] > 0 else "N/A"
            writer.writerow([
                idx, t['symbol'], t['base'], t['current_price'], f"{t['funding_rate']:+.4f}%", mc_str,
                open_val, f"{diff_pct:+.2f}%",
                f"{t['pct_1d']:.2f}%", f"{t['pct_1w']:.2f}%", f"{t['pct_2w']:.2f}%",
                f"{t['pct_1m']:.2f}%", f"{t['pct_3m']:.2f}%", f"{t['pct_6m']:.2f}%",
                t['open_1d'], t['open_1w'], t['open_2w'], t['open_1m'], t['open_3m'], t['open_6m'],
                f"{t['volume_24h']:,.0f}", f"{t['green_count']}/6"
            ])
    return os.path.abspath(filename)

# ----------------- MODERN MULTI-TIMEFRAME WEB DASHBOARD -----------------
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Binance Futures Scanner | CryptoRank Screener Style</title>
    <style>
        :root {
            --bg: #0b0e14;
            --surface: #121721;
            --surface-hover: #19202e;
            --border: #222b3d;
            --border-active: #3b82f6;
            --text: #eaecef;
            --text-dim: #848e9c;
            --accent: #2563eb;
            --accent-light: rgba(37, 99, 235, 0.15);
            --green: #0ecb81;
            --green-bg: rgba(14, 203, 129, 0.12);
            --red: #f6465d;
            --red-bg: rgba(246, 70, 93, 0.12);
            --yellow: #f0b90b;
            --yellow-bg: rgba(240, 185, 11, 0.15);
            --purple: #b388ff;
            --purple-bg: rgba(179, 136, 255, 0.12);
            --orange: #ff9800;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', Roboto, sans-serif; -webkit-tap-highlight-color: transparent; }
        body { background: var(--bg); color: var(--text); padding: 14px 14px 90px 14px; min-height: 100vh; font-size: 13px; line-height: 1.4; }
        
        /* HEADER */
        .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; flex-wrap: wrap; gap: 8px; border-bottom: 1px solid var(--border); padding-bottom: 10px; }
        .title-group h1 { font-size: 18px; font-weight: 700; color: #fff; display: flex; align-items: center; gap: 6px; }
        .live-tag { display: inline-flex; align-items: center; gap: 4px; background: rgba(14, 203, 129, 0.15); color: var(--green); padding: 2px 7px; border-radius: 20px; font-size: 11px; font-weight: 700; border: 1px solid rgba(14, 203, 129, 0.3); }
        .live-dot { width: 6px; height: 6px; background: var(--green); border-radius: 50%; box-shadow: 0 0 6px var(--green); }
        .utc-badge { background: #2563eb25; color: #60a5fa; border: 1px solid #2563eb55; padding: 2px 6px; border-radius: 4px; font-size: 10.5px; font-weight: 700; }
        .header-meta { font-size: 11.5px; color: var(--text-dim); }

        /* CRYPTORANK MARKET STATS STRIP */
        .stats-grid { display: flex; overflow-x: auto; gap: 8px; margin-bottom: 12px; padding-bottom: 4px; scroll-snap-type: x mandatory; -webkit-overflow-scrolling: touch; }
        .stat-card { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 8px 12px; min-width: 130px; flex: 1 0 auto; scroll-snap-align: start; }
        .stat-label { color: var(--text-dim); font-size: 11px; margin-bottom: 2px; }
        .stat-value { font-size: 16px; font-weight: 700; }
        .stat-value.green { color: var(--green); }
        .stat-value.red { color: var(--red); }
        .stat-value.blue { color: #60a5fa; }
        .stat-value.orange { color: var(--orange); }
        .stat-sub { font-size: 10px; color: var(--text-dim); margin-top: 1px; }

        /* ================= CRYPTORANK FILTER DECK ================= */
        .cr-filter-deck {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: 10px 12px;
            margin-bottom: 12px;
            box-shadow: 0 3px 12px rgba(0,0,0,0.25);
        }
        .cr-chips-scroll {
            display: flex;
            align-items: center;
            gap: 8px;
            overflow-x: auto;
            padding-bottom: 4px;
            scrollbar-width: none;
            -webkit-overflow-scrolling: touch;
        }
        .cr-chips-scroll::-webkit-scrollbar { display: none; }

        /* DROPDOWN WRAPPER & CHIPS */
        .cr-dropdown-wrap { position: relative; display: inline-block; }
        .cr-chip {
            background: #18202d;
            border: 1px solid var(--border);
            color: var(--text);
            padding: 7px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 600;
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            gap: 6px;
            white-space: nowrap;
            transition: all 0.15s;
            user-select: none;
            min-height: 34px;
        }
        .cr-chip:hover { background: #222c3d; border-color: #3b4b66; color: #fff; }
        .cr-chip.active-chip { background: var(--accent-light); border-color: var(--border-active); color: #93c5fd; font-weight: 700; }
        .cr-arrow { font-size: 10px; color: var(--text-dim); transition: transform 0.2s; }
        .cr-chip.open .cr-arrow { transform: rotate(180deg); }

        /* CRYPTORANK POPOVER CARD */
        .cr-popover {
            position: absolute;
            top: calc(100% + 6px);
            left: 0;
            background: #141a24;
            border: 1px solid #2d384e;
            border-radius: 10px;
            padding: 12px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.6);
            z-index: 1000;
            display: none;
            min-width: 220px;
            animation: fadeIn 0.15s ease-out;
        }
        .cr-popover.show { display: block; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(-4px); } to { opacity: 1; transform: translateY(0); } }

        .cr-popover-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; border-bottom: 1px solid rgba(255,255,255,0.06); padding-bottom: 6px; }
        .cr-popover-title { font-size: 11.5px; font-weight: 700; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.5px; }
        
        .cr-popover-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 6px; margin-top: 6px; }
        .cr-preset-btn { background: #19202e; border: 1px solid var(--border); color: var(--text); padding: 7px 10px; border-radius: 6px; font-size: 12px; font-weight: 600; cursor: pointer; text-align: center; }
        .cr-preset-btn:hover { background: #232c40; color: #fff; }
        .cr-preset-btn.active { background: var(--accent); border-color: var(--accent); color: #fff; font-weight: 700; }

        .cr-popover-list { display: flex; flex-direction: column; gap: 4px; margin-top: 6px; }
        .cr-list-btn { background: transparent; border: none; color: var(--text); padding: 7px 10px; border-radius: 6px; font-size: 12px; font-weight: 500; cursor: pointer; text-align: left; display: flex; align-items: center; justify-content: space-between; }
        .cr-list-btn:hover { background: #1d2535; color: #fff; }
        .cr-list-btn.active { background: var(--accent-light); color: #93c5fd; font-weight: 700; }

        .cr-inputs-row { display: flex; gap: 6px; align-items: center; margin-top: 6px; margin-bottom: 8px; }
        .cr-input { flex: 1; padding: 7px 9px; border-radius: 6px; border: 1px solid var(--border); background: #18202d; color: #fff; font-size: 13px; outline: none; }
        .cr-input:focus { border-color: var(--border-active); }

        .cr-presets-row { display: flex; gap: 5px; flex-wrap: wrap; }
        .cr-preset-tag { padding: 3px 8px; border-radius: 4px; border: 1px solid var(--border); background: #18202d; color: var(--text-dim); font-size: 11px; cursor: pointer; }
        .cr-preset-tag:hover { color: #fff; border-color: #3e4c6b; }

        /* RANGES GRID */
        .cr-ranges-grid { display: flex; flex-direction: column; gap: 6px; margin-top: 6px; }
        .cr-range-row { display: flex; align-items: center; gap: 6px; justify-content: space-between; }
        .cr-range-label { font-size: 11.5px; font-weight: 600; color: #fff; min-width: 72px; }
        .cr-input-sm { width: 70px; padding: 5px 6px; font-size: 12px; text-align: center; border-radius: 6px; border: 1px solid var(--border); background: #18202d; color: #fff; outline: none; }
        .cr-input-sm:focus { border-color: var(--border-active); }

        .cr-link-btn { background: transparent; border: none; color: var(--yellow); font-size: 11px; cursor: pointer; font-weight: 600; }

        .cr-reset-all { background: transparent; border: 1px dashed var(--border); color: var(--text-dim); padding: 7px 12px; border-radius: 20px; font-size: 11.5px; font-weight: 600; cursor: pointer; white-space: nowrap; margin-left: auto; }
        .cr-reset-all:hover { color: var(--yellow); border-color: var(--yellow); }

        /* ACTIVE FILTER TAGS ROW (CRYPTORANK SIGNATURE) */
        .cr-active-tags-row { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; margin-top: 8px; padding-top: 8px; border-top: 1px solid rgba(255,255,255,0.06); min-height: 28px; }
        .cr-tag { background: #1e2638; border: 1px solid #32405d; color: #cbd5e1; border-radius: 14px; padding: 2px 8px; font-size: 11px; font-weight: 600; display: inline-flex; align-items: center; gap: 6px; }
        .cr-tag-remove { cursor: pointer; color: var(--text-dim); font-size: 12px; line-height: 1; }
        .cr-tag-remove:hover { color: #f87171; }

        /* CONTROLS & ACTIONS */
        .controls { display: flex; gap: 8px; margin-bottom: 10px; flex-wrap: wrap; align-items: center; justify-content: space-between; }
        .controls-left { display: flex; gap: 8px; align-items: center; flex: 1; min-width: 200px; }
        .search-box { flex: 1; padding: 8px 11px; border-radius: 6px; border: 1px solid var(--border); background: var(--surface); color: #fff; font-size: 13.5px; outline: none; min-height: 36px; }
        .search-box:focus { border-color: var(--border-active); }

        .view-switch { display: inline-flex; background: var(--surface); border: 1px solid var(--border); border-radius: 6px; padding: 2px; }
        .view-btn { padding: 5px 10px; border-radius: 4px; border: none; background: transparent; color: var(--text-dim); font-size: 11.5px; font-weight: 600; cursor: pointer; display: flex; align-items: center; gap: 4px; }
        .view-btn.active { background: #232c40; color: #fff; }

        .btn-export { background: #18202d; border: 1px solid var(--border); color: var(--text); padding: 7px 12px; border-radius: 6px; font-size: 12px; font-weight: 600; cursor: pointer; }
        .btn-export:hover { background: #232c40; color: #fff; }

        /* BADGES */
        .pill-tag { display: inline-flex; align-items: center; padding: 2px 6px; border-radius: 4px; font-size: 10.5px; font-weight: 700; white-space: nowrap; }
        .pill-tag.green { background: var(--green-bg); color: var(--green); border: 1px solid rgba(14, 203, 129, 0.3); }
        .pill-tag.red { background: var(--red-bg); color: var(--red); border: 1px solid rgba(246, 70, 93, 0.3); }
        .pill-tag.yellow { background: var(--yellow-bg); color: var(--yellow); border: 1px solid rgba(240, 185, 11, 0.3); }

        .diff-badge { display: inline-flex; align-items: center; gap: 3px; padding: 3px 7px; border-radius: 5px; font-weight: 700; font-size: 12px; }
        .diff-badge.green { background: rgba(14, 203, 129, 0.18); color: var(--green); border: 1px solid rgba(14, 203, 129, 0.4); }
        .diff-badge.red { background: rgba(246, 70, 93, 0.18); color: var(--red); border: 1px solid rgba(246, 70, 93, 0.4); }
        
        .text-green { color: var(--green); font-weight: 700; }
        .text-red { color: var(--red); font-weight: 700; }
        .text-purple { color: var(--purple); font-weight: 600; }
        .text-orange { color: var(--orange); font-weight: 700; }
        .symbol-cell { font-weight: 700; color: #fff; font-size: 13.5px; }
        .symbol-sub { font-size: 10px; color: var(--text-dim); margin-left: 2px; }

        /* CARDS VIEW */
        .card-container { display: grid; grid-template-columns: repeat(auto-fill, minmax(310px, 1fr)); gap: 9px; }
        .crypto-card { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 11px 13px; transition: all 0.15s; }
        .crypto-card:hover { border-color: #3b4b66; background: var(--surface-hover); }
        
        .card-header-row { display: flex; justify-content: space-between; align-items: center; margin-bottom: 7px; }
        .card-sym-title { font-size: 15px; font-weight: 700; color: #fff; display: flex; align-items: center; gap: 5px; }
        .card-price-display { font-size: 15px; font-weight: 700; color: #fff; }

        .card-focus-bar {
            background: rgba(37, 99, 235, 0.08);
            border: 1px solid rgba(37, 99, 235, 0.22);
            border-radius: 7px;
            padding: 7px 9px;
            margin-bottom: 7px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .focus-title { font-size: 11px; color: var(--text-dim); }
        .focus-open { font-size: 11.5px; font-weight: 600; color: #fff; }

        .card-metrics-row {
            display: flex;
            justify-content: space-between;
            font-size: 11px;
            color: var(--text-dim);
            margin-bottom: 8px;
            padding-bottom: 7px;
            border-bottom: 1px dashed rgba(255,255,255,0.08);
        }

        .tf-grid { display: grid; grid-template-columns: repeat(6, 1fr); gap: 3px; }
        .tf-tile { background: #18202d; border-radius: 5px; padding: 4px 2px; text-align: center; border: 1px solid var(--border); }
        .tf-tile-name { font-size: 9px; color: var(--text-dim); font-weight: 700; margin-bottom: 1px; }
        .tf-tile-val { font-size: 11px; font-weight: 700; white-space: nowrap; }
        .tf-tile.highlight { border-color: var(--border-active); box-shadow: 0 0 5px rgba(59, 130, 246, 0.4); }

        /* TABLE VIEW */
        .table-container { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; overflow-x: auto; -webkit-overflow-scrolling: touch; }
        table { width: 100%; border-collapse: collapse; text-align: left; font-size: 12.5px; }
        th { background: #161c27; padding: 9px 11px; color: var(--text-dim); font-weight: 600; cursor: pointer; user-select: none; border-bottom: 1px solid var(--border); white-space: nowrap; }
        th:hover { color: #fff; }
        th.active-sort { color: var(--yellow); }
        th.col-focus { background: #1a2232; color: #fff; }
        td { padding: 9px 11px; border-bottom: 1px solid var(--border); white-space: nowrap; }
        td.col-focus { background: rgba(37, 99, 235, 0.05); }
        tr:hover td { background: var(--surface-hover); }

        /* STICKY COLUMNS */
        th.col-sticky-1, td.col-sticky-1 { position: sticky; left: 0; z-index: 3; background: #121721; width: 30px; text-align: center; }
        th.col-sticky-2, td.col-sticky-2 { position: sticky; left: 30px; z-index: 3; background: #121721; box-shadow: 2px 0 5px rgba(0,0,0,0.3); }
        th.col-sticky-1, th.col-sticky-2 { background: #161c27; z-index: 4; }

        /* FAB */
        .fab-rescan {
            position: fixed;
            bottom: 18px;
            right: 18px;
            background: var(--yellow);
            color: #000;
            font-weight: 700;
            padding: 10px 16px;
            border-radius: 25px;
            box-shadow: 0 3px 14px rgba(0,0,0,0.5);
            border: none;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 6px;
            z-index: 999;
            font-size: 12px;
            transition: all 0.2s;
        }
        .fab-rescan:active { transform: scale(0.95); }

        /* LOADING */
        #loading { display: none; text-align: center; padding: 36px 14px; }
        .spinner { border: 3px solid rgba(240, 185, 11, 0.15); border-top: 3px solid var(--yellow); border-radius: 50%; width: 32px; height: 32px; animation: spin 0.8s linear infinite; margin: 0 auto 10px; }
        @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
        .loading-text { font-size: 14px; font-weight: 600; color: var(--yellow); }

        @media (max-width: 768px) {
            body { padding: 10px 8px 80px 8px; }
            .header h1 { font-size: 16px; }
            .search-box, .cr-input { font-size: 16px !important; }
            .tf-grid { grid-template-columns: repeat(3, 1fr); gap: 4px; }
            .cr-popover { position: fixed; top: auto; bottom: 0; left: 0; right: 0; width: 100%; border-radius: 16px 16px 0 0; border: 1px solid #334155; padding: 16px; z-index: 10000; max-height: 80vh; overflow-y: auto; }
        }
    </style>
</head>
<body>
    <!-- HEADER -->
    <div class="header">
        <div class="title-group">
            <h1>Binance Futures Scanner <span class="utc-badge">UTC 00:00</span> <span class="live-tag"><span class="live-dot"></span> LIVE</span></h1>
        </div>
        <div class="header-meta" id="lastScanTime">Syncing...</div>
    </div>

    <!-- STATS BAR -->
    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-label">Total Pairs</div>
            <div class="stat-value" id="statTotal">-</div>
            <div class="stat-sub">USDT-M Perpetual</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">> Open (Bull)</div>
            <div class="stat-value green" id="statAbove">-</div>
            <div class="stat-sub" id="statAbovePct">-</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">< Open (Bear)</div>
            <div class="stat-value red" id="statBelow">-</div>
            <div class="stat-sub" id="statBelowPct">-</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Avg Dev. (Focus)</div>
            <div class="stat-value blue" id="statAvgDist">-</div>
            <div class="stat-sub" id="statAvgDistSub">% from Open</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Neg. FR 🔥</div>
            <div class="stat-value orange" id="statNegFr">-</div>
            <div class="stat-sub" id="statNegFrSub">Short Squeeze Watch</div>
        </div>
    </div>

    <!-- ================= CRYPTORANK STYLE FILTER DECK ================= -->
    <div class="cr-filter-deck">
        <div class="cr-chips-scroll">
            <!-- 1. Timeframe Chip -->
            <div class="cr-dropdown-wrap">
                <button class="cr-chip active-chip" id="btnChipTf" onclick="togglePopover('pop_tf')">
                    <span>⏱️ Timeframe: <b id="chipTfText">1W</b></span>
                    <span class="cr-arrow">▾</span>
                </button>
                <div class="cr-popover" id="pop_tf">
                    <div class="cr-popover-title">Focus Timeframe (UTC 00:00)</div>
                    <div class="cr-popover-grid">
                        <button class="cr-preset-btn" data-tf="1d" onclick="selectTf('1d')">1D (Daily)</button>
                        <button class="cr-preset-btn active" data-tf="1w" onclick="selectTf('1w')">1W (Weekly)</button>
                        <button class="cr-preset-btn" data-tf="2w" onclick="selectTf('2w')">2W (14 Days)</button>
                        <button class="cr-preset-btn" data-tf="1m" onclick="selectTf('1m')">1M (Monthly)</button>
                        <button class="cr-preset-btn" data-tf="3m" onclick="selectTf('3m')">3M (Quarter)</button>
                        <button class="cr-preset-btn" data-tf="6m" onclick="selectTf('6m')">6M (Semi-annual)</button>
                    </div>
                </div>
            </div>

            <!-- 2. Position Condition Chip -->
            <div class="cr-dropdown-wrap">
                <button class="cr-chip active-chip" id="btnChipCond" onclick="togglePopover('pop_cond')">
                    <span id="chipCondText">🟢 > Open</span>
                    <span class="cr-arrow">▾</span>
                </button>
                <div class="cr-popover" id="pop_cond">
                    <div class="cr-popover-title">Position vs Open Price</div>
                    <div class="cr-popover-list">
                        <button class="cr-list-btn active" data-cond="above" onclick="selectCond('above')">🟢 Above Open (Bull)</button>
                        <button class="cr-list-btn" data-cond="below" onclick="selectCond('below')">🔴 Below Open (Bear)</button>
                        <button class="cr-list-btn" data-cond="all" onclick="selectCond('all')">⚪ All Positions</button>
                    </div>
                </div>
            </div>

            <!-- 3. Market Cap Chip -->
            <div class="cr-dropdown-wrap">
                <button class="cr-chip" id="btnChipMc" onclick="togglePopover('pop_mc')">
                    <span id="chipMcText">💎 Market Cap</span>
                    <span class="cr-arrow">▾</span>
                </button>
                <div class="cr-popover" id="pop_mc" style="min-width: 270px;">
                    <div class="cr-popover-title">Market Cap Range (USD)</div>
                    <div class="cr-inputs-row">
                        <input type="text" id="minMcInput" class="cr-input" placeholder="Min (50M)" oninput="onMcChange()">
                        <span style="color:var(--text-dim);">-</span>
                        <input type="text" id="maxMcInput" class="cr-input" placeholder="Max (1B)" oninput="onMcChange()">
                    </div>
                    <div class="cr-presets-row">
                        <button class="cr-preset-tag" onclick="setMcPreset(0, 0)">All</button>
                        <button class="cr-preset-tag" onclick="setMcPreset(0, 50e6)">&lt;$50M</button>
                        <button class="cr-preset-tag" onclick="setMcPreset(50e6, 300e6)">$50M-$300M</button>
                        <button class="cr-preset-tag" onclick="setMcPreset(300e6, 1e9)">$300M-$1B</button>
                        <button class="cr-preset-tag" onclick="setMcPreset(1e9, 0)">&gt;$1B</button>
                    </div>
                </div>
            </div>

            <!-- 4. Funding Rate Chip -->
            <div class="cr-dropdown-wrap">
                <button class="cr-chip" id="btnChipFr" onclick="togglePopover('pop_fr')">
                    <span id="chipFrText">⚡ Funding Rate</span>
                    <span class="cr-arrow">▾</span>
                </button>
                <div class="cr-popover" id="pop_fr">
                    <div class="cr-popover-title">Binance Funding Rate</div>
                    <div class="cr-popover-list">
                        <button class="cr-list-btn active" data-fr="all" onclick="selectFr('all')">All Funding Rates</button>
                        <button class="cr-list-btn" data-fr="neg" onclick="selectFr('neg')">🔥 Negative FR (&lt;0%)</button>
                        <button class="cr-list-btn" data-fr="neg_deep" onclick="selectFr('neg_deep')">⚠️ Deep Neg (&lt;-0.05%) Squeeze</button>
                        <button class="cr-list-btn" data-fr="pos" onclick="selectFr('pos')">📈 Positive FR (&gt;0%)</button>
                        <button class="cr-list-btn" data-fr="pos_high" onclick="selectFr('pos_high')">⚡ High Positive (&gt;0.03%)</button>
                    </div>
                </div>
            </div>

            <!-- 5. 24h Volume Chip -->
            <div class="cr-dropdown-wrap">
                <button class="cr-chip" id="btnChipVol" onclick="togglePopover('pop_vol')">
                    <span id="chipVolText">📊 24h Volume</span>
                    <span class="cr-arrow">▾</span>
                </button>
                <div class="cr-popover" id="pop_vol">
                    <div class="cr-popover-title">24h Trading Volume</div>
                    <div class="cr-popover-list">
                        <button class="cr-list-btn active" data-vol="0" onclick="selectVol(0)">All Volumes</button>
                        <button class="cr-list-btn" data-vol="1000000" onclick="selectVol(1000000)">&gt; $1M Volume</button>
                        <button class="cr-list-btn" data-vol="5000000" onclick="selectVol(5000000)">&gt; $5M Volume</button>
                        <button class="cr-list-btn" data-vol="20000000" onclick="selectVol(20000000)">&gt; $20M Volume</button>
                    </div>
                </div>
            </div>

            <!-- 6. Custom % Ranges Chip -->
            <div class="cr-dropdown-wrap">
                <button class="cr-chip" id="btnChipRanges" onclick="togglePopover('pop_ranges')">
                    <span id="chipRangesText">📈 % Performance</span>
                    <span class="cr-arrow">▾</span>
                </button>
                <div class="cr-popover" id="pop_ranges" style="min-width: 300px;">
                    <div class="cr-popover-header">
                        <div class="cr-popover-title">Performance % Filters</div>
                        <button class="cr-link-btn" onclick="resetRangeFilters()">Reset</button>
                    </div>
                    <div class="cr-ranges-grid">
                        <div class="cr-range-row">
                            <span class="cr-range-label" id="crDevLabel">Dev. %</span>
                            <input type="number" step="any" id="min_dev_input" class="cr-input-sm" placeholder="Min" oninput="onRangeChange()">
                            <span>-</span>
                            <input type="number" step="any" id="max_dev_input" class="cr-input-sm" placeholder="Max" oninput="onRangeChange()">
                        </div>
                        <div class="cr-range-row">
                            <span class="cr-range-label">1D (24h)</span>
                            <input type="number" step="any" id="min_1d_input" class="cr-input-sm" placeholder="Min" oninput="onRangeChange()">
                            <span>-</span>
                            <input type="number" step="any" id="max_1d_input" class="cr-input-sm" placeholder="Max" oninput="onRangeChange()">
                        </div>
                        <div class="cr-range-row">
                            <span class="cr-range-label">1W (7d)</span>
                            <input type="number" step="any" id="min_1w_input" class="cr-input-sm" placeholder="Min" oninput="onRangeChange()">
                            <span>-</span>
                            <input type="number" step="any" id="max_1w_input" class="cr-input-sm" placeholder="Max" oninput="onRangeChange()">
                        </div>
                        <div class="cr-range-row">
                            <span class="cr-range-label">2W (14d)</span>
                            <input type="number" step="any" id="min_2w_input" class="cr-input-sm" placeholder="Min" oninput="onRangeChange()">
                            <span>-</span>
                            <input type="number" step="any" id="max_2w_input" class="cr-input-sm" placeholder="Max" oninput="onRangeChange()">
                        </div>
                        <div class="cr-range-row">
                            <span class="cr-range-label">1M (30d)</span>
                            <input type="number" step="any" id="min_1m_input" class="cr-input-sm" placeholder="Min" oninput="onRangeChange()">
                            <span>-</span>
                            <input type="number" step="any" id="max_1m_input" class="cr-input-sm" placeholder="Max" oninput="onRangeChange()">
                        </div>
                        <div class="cr-range-row">
                            <span class="cr-range-label">3M</span>
                            <input type="number" step="any" id="min_3m_input" class="cr-input-sm" placeholder="Min" oninput="onRangeChange()">
                            <span>-</span>
                            <input type="number" step="any" id="max_3m_input" class="cr-input-sm" placeholder="Max" oninput="onRangeChange()">
                        </div>
                        <div class="cr-range-row">
                            <span class="cr-range-label">6M</span>
                            <input type="number" step="any" id="min_6m_input" class="cr-input-sm" placeholder="Min" oninput="onRangeChange()">
                            <span>-</span>
                            <input type="number" step="any" id="max_6m_input" class="cr-input-sm" placeholder="Max" oninput="onRangeChange()">
                        </div>
                    </div>
                </div>
            </div>

            <!-- Reset All -->
            <button class="cr-reset-all" onclick="resetAllFilters()">↺ Reset All</button>
        </div>

        <!-- ACTIVE FILTER TAGS ROW -->
        <div class="cr-active-tags-row" id="crActiveTags"></div>
    </div>

    <!-- CONTROLS -->
    <div class="controls">
        <div class="controls-left">
            <input type="text" id="searchInput" class="search-box" placeholder="🔍 Search token (BTC, SOL, SUI, PEPE...)" oninput="renderView()">
        </div>
        <div style="display:flex; gap:8px; align-items:center;">
            <button class="btn-export" onclick="exportCSV()">📥 CSV</button>
            <div style="font-size:12.5px; font-weight:600; color:var(--text-dim);">
                <b style="color:#fff;" id="resultCount">0</b> pairs
            </div>
            <!-- View Switcher -->
            <div class="view-switch">
                <button class="view-btn" id="btnViewCards" onclick="switchView('cards')">🎴 Cards</button>
                <button class="view-btn" id="btnViewTable" onclick="switchView('table')">📋 Table</button>
            </div>
        </div>
    </div>

    <!-- LOADING -->
    <div id="loading">
        <div class="spinner"></div>
        <div class="loading-text">⚡ Scanning Binance Futures & Funding Rates...</div>
    </div>

    <!-- VIEW CONTAINERS -->
    <div class="card-container" id="cardContainer"></div>

    <div class="table-container" id="tableContainer" style="display:none;">
        <table>
            <thead>
                <tr>
                    <th class="col-sticky-1" onclick="changeSort('index')">#</th>
                    <th class="col-sticky-2" onclick="changeSort('symbol')">Symbol</th>
                    <th onclick="changeSort('current_price')">Price</th>
                    <th onclick="changeSort('funding_rate')">⚡ FR ⬍</th>
                    <th onclick="changeSort('market_cap')">💎 MCap ⬍</th>
                    <th class="col-focus" id="th_open_active">Open</th>
                    <th class="col-focus" id="th_diff_pct" onclick="changeSort('diff_pct')">🔥 % Dev. ⬍</th>
                    <th id="th_pct_1d" onclick="changeSort('pct_1d')">1D % ⬍</th>
                    <th id="th_pct_1w" onclick="changeSort('pct_1w')">1W % ⬍</th>
                    <th id="th_pct_2w" onclick="changeSort('pct_2w')">2W % ⬍</th>
                    <th id="th_pct_1m" onclick="changeSort('pct_1m')">1M % ⬍</th>
                    <th id="th_pct_3m" onclick="changeSort('pct_3m')">3M % ⬍</th>
                    <th id="th_pct_6m" onclick="changeSort('pct_6m')">6M % ⬍</th>
                    <th onclick="changeSort('volume_24h')">24h Vol</th>
                    <th onclick="changeSort('green_count')">Trend ⬍</th>
                </tr>
            </thead>
            <tbody id="tableBody"></tbody>
        </table>
    </div>

    <!-- FAB RESCAN -->
    <button class="fab-rescan" onclick="fetchScan(true)">
        🔄 <span>Rescan</span>
    </button>

    <script>
        // SMART AUTO-DETECTION: Mobile vs Desktop
        const isMobile = /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent) || (window.innerWidth <= 768);
        
        let currentFilterTf = '1w';
        let currentFilterCond = 'above';
        let currentFilterFr = 'all';
        let currentFilterVol = 0;
        let allTokens = [];
        let currentSort = { col: 'diff_pct', dir: 'desc' };
        let currentView = isMobile ? 'cards' : 'table';

        // Close popovers on click outside
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.cr-dropdown-wrap')) {
                closeAllPopovers();
            }
        });

        function closeAllPopovers() {
            document.querySelectorAll('.cr-popover').forEach(p => p.classList.remove('show'));
            document.querySelectorAll('.cr-chip').forEach(c => c.classList.remove('open'));
        }

        function togglePopover(id) {
            const el = document.getElementById(id);
            const isOpen = el.classList.contains('show');
            closeAllPopovers();
            if (!isOpen) {
                el.classList.add('show');
                el.parentElement.querySelector('.cr-chip').classList.add('open');
            }
        }

        function switchView(mode) {
            currentView = mode;
            document.getElementById('btnViewCards').classList.toggle('active', mode === 'cards');
            document.getElementById('btnViewTable').classList.toggle('active', mode === 'table');
            document.getElementById('cardContainer').style.display = mode === 'cards' ? 'grid' : 'none';
            document.getElementById('tableContainer').style.display = mode === 'table' ? 'block' : 'none';
            renderView();
        }

        function parseAmount(valStr) {
            if (!valStr) return null;
            valStr = valStr.toString().trim().toUpperCase().replace(/[$,\s]/g, '');
            if (!valStr) return null;
            let multiplier = 1;
            if (valStr.endsWith('T')) { multiplier = 1e12; valStr = valStr.slice(0, -1); }
            else if (valStr.endsWith('B')) { multiplier = 1e9; valStr = valStr.slice(0, -1); }
            else if (valStr.endsWith('M')) { multiplier = 1e6; valStr = valStr.slice(0, -1); }
            else if (valStr.endsWith('K')) { multiplier = 1e3; valStr = valStr.slice(0, -1); }
            const num = parseFloat(valStr);
            return isNaN(num) ? null : num * multiplier;
        }

        function formatNumber(num) {
            if (num === null || num === undefined) return '-';
            if (num >= 1000) return num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            if (num >= 1) return num.toLocaleString('en-US', { minimumFractionDigits: 4, maximumFractionDigits: 4 });
            return num.toLocaleString('en-US', { minimumFractionDigits: 4, maximumFractionDigits: 6 });
        }

        function formatVolume(num) {
            if (!num) return '$0';
            if (num >= 1e12) return '$' + (num / 1e12).toFixed(2) + 'T';
            if (num >= 1e9) return '$' + (num / 1e9).toFixed(2) + 'B';
            if (num >= 1e6) return '$' + (num / 1e6).toFixed(1) + 'M';
            if (num >= 1e3) return '$' + (num / 1e3).toFixed(0) + 'K';
            return '$' + num.toFixed(0);
        }

        function formatMarketCap(mc) {
            if (!mc || mc <= 0) return '<span style="color:var(--text-dim);">N/A</span>';
            return `<span class="text-purple">${formatVolume(mc)}</span>`;
        }

        function getFundingBadge(fr) {
            if (fr === null || fr === undefined) return '<span style="color:var(--text-dim);">-</span>';
            const sign = fr > 0 ? '+' : '';
            if (fr < -0.05) return `<span class="pill-tag red">🔥 ${fr.toFixed(4)}%</span>`;
            if (fr < 0) return `<span class="text-orange" style="font-weight:700;">${fr.toFixed(4)}%</span>`;
            if (fr > 0.03) return `<span class="pill-tag green">⚡ +${fr.toFixed(4)}%</span>`;
            return `<span style="color:var(--text-dim);">${sign}${fr.toFixed(4)}%</span>`;
        }

        /* FILTER SELECTORS */
        function selectTf(tf) {
            currentFilterTf = tf;
            document.querySelectorAll('#pop_tf .cr-preset-btn').forEach(b => {
                b.classList.toggle('active', b.getAttribute('data-tf') === tf);
            });
            document.getElementById('chipTfText').innerText = tf.toUpperCase();
            closeAllPopovers();
            updateStats();
            renderView();
        }

        function selectCond(cond) {
            currentFilterCond = cond;
            document.querySelectorAll('#pop_cond .cr-list-btn').forEach(b => {
                b.classList.toggle('active', b.getAttribute('data-cond') === cond);
            });
            const condLabels = { 'above': '🟢 > Open', 'below': '🔴 < Open', 'all': '⚪ All' };
            document.getElementById('chipCondText').innerText = condLabels[cond];
            closeAllPopovers();
            renderView();
        }

        function selectFr(fr) {
            currentFilterFr = fr;
            document.querySelectorAll('#pop_fr .cr-list-btn').forEach(b => {
                b.classList.toggle('active', b.getAttribute('data-fr') === fr);
            });
            const frBtn = document.getElementById('btnChipFr');
            const frText = document.getElementById('chipFrText');
            if (fr === 'all') {
                frBtn.classList.remove('active-chip');
                frText.innerText = '⚡ Funding Rate';
            } else {
                frBtn.classList.add('active-chip');
                const labels = { 'neg': '🔥 Neg FR', 'neg_deep': '⚠️ Deep Neg', 'pos': '📈 Pos FR', 'pos_high': '⚡ High Pos' };
                frText.innerText = `⚡ ${labels[fr]}`;
            }
            closeAllPopovers();
            renderView();
        }

        function selectVol(vol) {
            currentFilterVol = vol;
            document.querySelectorAll('#pop_vol .cr-list-btn').forEach(b => {
                b.classList.toggle('active', parseFloat(b.getAttribute('data-vol')) === vol);
            });
            const volBtn = document.getElementById('btnChipVol');
            const volText = document.getElementById('chipVolText');
            if (vol === 0) {
                volBtn.classList.remove('active-chip');
                volText.innerText = '📊 24h Volume';
            } else {
                volBtn.classList.add('active-chip');
                volText.innerText = `📊 > ${formatVolume(vol)}`;
            }
            closeAllPopovers();
            renderView();
        }

        function setMcPreset(minVal, maxVal) {
            document.getElementById('minMcInput').value = minVal > 0 ? (minVal >= 1e9 ? (minVal/1e9)+'B' : (minVal/1e6)+'M') : '';
            document.getElementById('maxMcInput').value = maxVal > 0 ? (maxVal >= 1e9 ? (maxVal/1e9)+'B' : (maxVal/1e6)+'M') : '';
            onMcChange();
            closeAllPopovers();
        }

        function onMcChange() {
            const minMc = parseAmount(document.getElementById('minMcInput').value);
            const maxMc = parseAmount(document.getElementById('maxMcInput').value);
            const chipBtn = document.getElementById('btnChipMc');
            const chipText = document.getElementById('chipMcText');

            if (!minMc && !maxMc) {
                chipBtn.classList.remove('active-chip');
                chipText.innerText = '💎 Market Cap';
            } else {
                chipBtn.classList.add('active-chip');
                if (minMc && maxMc) chipText.innerText = `💎 MC: ${formatVolume(minMc)}-${formatVolume(maxMc)}`;
                else if (minMc) chipText.innerText = `💎 MC: ≥${formatVolume(minMc)}`;
                else chipText.innerText = `💎 MC: ≤${formatVolume(maxMc)}`;
            }
            renderView();
        }

        function onRangeChange() {
            let activeCount = 0;
            ['dev', '1d', '1w', '2w', '1m', '3m', '6m'].forEach(k => {
                const minEl = document.getElementById(`min_${k}_input`);
                const maxEl = document.getElementById(`max_${k}_input`);
                if ((minEl && minEl.value !== '') || (maxEl && maxEl.value !== '')) activeCount++;
            });

            const chipBtn = document.getElementById('btnChipRanges');
            const chipText = document.getElementById('chipRangesText');
            if (activeCount > 0) {
                chipBtn.classList.add('active-chip');
                chipText.innerText = `📈 % Ranges (${activeCount})`;
            } else {
                chipBtn.classList.remove('active-chip');
                chipText.innerText = '📈 % Performance';
            }
            renderView();
        }

        function resetRangeFilters() {
            ['dev', '1d', '1w', '2w', '1m', '3m', '6m'].forEach(k => {
                const minEl = document.getElementById(`min_${k}_input`);
                const maxEl = document.getElementById(`max_${k}_input`);
                if (minEl) minEl.value = '';
                if (maxEl) maxEl.value = '';
            });
            onRangeChange();
            closeAllPopovers();
        }

        function resetAllFilters() {
            selectTf('1w');
            selectCond('above');
            selectFr('all');
            selectVol(0);
            setMcPreset(0, 0);
            resetRangeFilters();
            document.getElementById('searchInput').value = '';
            renderView();
        }

        /* UPDATE ACTIVE FILTER TAGS (CRYPTORANK STYLE CHIPS BAR) */
        function updateActiveTags() {
            const container = document.getElementById('crActiveTags');
            container.innerHTML = '';
            const tags = [];

            // Timeframe tag
            tags.push({ label: `Timeframe: ${currentFilterTf.toUpperCase()}`, onRemove: () => selectTf('1w') });

            // Condition tag
            if (currentFilterCond !== 'all') {
                const condLabel = currentFilterCond === 'above' ? 'Position: > Open' : 'Position: < Open';
                tags.push({ label: condLabel, onRemove: () => selectCond('all') });
            }

            // Market Cap tag
            const minMc = parseAmount(document.getElementById('minMcInput').value);
            const maxMc = parseAmount(document.getElementById('maxMcInput').value);
            if (minMc || maxMc) {
                let lbl = 'MCap';
                if (minMc && maxMc) lbl = `MCap: ${formatVolume(minMc)} - ${formatVolume(maxMc)}`;
                else if (minMc) lbl = `MCap ≥ ${formatVolume(minMc)}`;
                else lbl = `MCap ≤ ${formatVolume(maxMc)}`;
                tags.push({ label: lbl, onRemove: () => setMcPreset(0, 0) });
            }

            // Funding Rate tag
            if (currentFilterFr !== 'all') {
                tags.push({ label: `FR: ${currentFilterFr}`, onRemove: () => selectFr('all') });
            }

            // Volume tag
            if (currentFilterVol > 0) {
                tags.push({ label: `Vol > ${formatVolume(currentFilterVol)}`, onRemove: () => selectVol(0) });
            }

            // Range tags
            ['dev', '1d', '1w', '2w', '1m', '3m', '6m'].forEach(k => {
                const minEl = document.getElementById(`min_${k}_input`);
                const maxEl = document.getElementById(`max_${k}_input`);
                const hasMin = minEl && minEl.value !== '';
                const hasMax = maxEl && maxEl.value !== '';
                if (hasMin || hasMax) {
                    let name = k.toUpperCase();
                    if (k === 'dev') name = `Dev.${currentFilterTf.toUpperCase()}`;
                    let str = `${name}%: `;
                    if (hasMin && hasMax) str += `${minEl.value} to ${maxEl.value}`;
                    else if (hasMin) str += `≥ ${minEl.value}`;
                    else str += `≤ ${maxEl.value}`;
                    tags.push({
                        label: str,
                        onRemove: () => {
                            if (minEl) minEl.value = '';
                            if (maxEl) maxEl.value = '';
                            onRangeChange();
                        }
                    });
                }
            });

            tags.forEach(t => {
                const el = document.createElement('span');
                el.className = 'cr-tag';
                el.innerHTML = `<span>${t.label}</span><span class="cr-tag-remove">✕</span>`;
                el.querySelector('.cr-tag-remove').onclick = t.onRemove;
                container.appendChild(el);
            });
        }

        function updateStats() {
            if (!allTokens || allTokens.length === 0) return;
            const aboveKey = `above_${currentFilterTf}`;
            const pctKey = `pct_${currentFilterTf}`;
            const total = allTokens.length;
            const aboveCount = allTokens.filter(t => t[aboveKey]).length;
            const belowCount = total - aboveCount;
            const negFrCount = allTokens.filter(t => t.funding_rate < 0).length;

            document.getElementById('statTotal').innerText = total;
            document.getElementById('statAbove').innerText = aboveCount;
            document.getElementById('statBelow').innerText = belowCount;
            document.getElementById('statAbovePct').innerText = `${((aboveCount/total)*100).toFixed(1)}% market`;
            document.getElementById('statBelowPct').innerText = `${((belowCount/total)*100).toFixed(1)}% market`;
            document.getElementById('statNegFr').innerText = negFrCount;
            document.getElementById('statNegFrSub').innerText = `${((negFrCount/total)*100).toFixed(1)}% neg pairs`;

            let targetGroup = allTokens;
            if (currentFilterCond === 'above') targetGroup = allTokens.filter(t => t[aboveKey]);
            else if (currentFilterCond === 'below') targetGroup = allTokens.filter(t => !t[aboveKey]);

            if (targetGroup.length > 0) {
                const avg = targetGroup.reduce((sum, t) => sum + t[pctKey], 0) / targetGroup.length;
                const sign = avg >= 0 ? '+' : '';
                document.getElementById('statAvgDist').innerText = `${sign}${avg.toFixed(2)}%`;
                document.getElementById('statAvgDistSub').innerText = `Avg of ${targetGroup.length} (${currentFilterTf.toUpperCase()})`;
            } else {
                document.getElementById('statAvgDist').innerText = '0.00%';
            }

            document.getElementById('th_open_active').innerText = `Open (${currentFilterTf.toUpperCase()})`;
            document.getElementById('th_diff_pct').innerText = `🔥 % Dev. (${currentFilterTf.toUpperCase()}) ⬍`;
            const devLabel = document.getElementById('crDevLabel');
            if (devLabel) devLabel.innerText = `Dev. (${currentFilterTf.toUpperCase()}) %`;
        }

        async function fetchScan(force = false) {
            document.getElementById('loading').style.display = 'block';
            document.getElementById('cardContainer').style.opacity = '0.2';
            document.getElementById('tableContainer').style.opacity = '0.2';
            try {
                const url = force ? '/api/scan?force=1' : '/api/scan';
                const resp = await fetch(url);
                const data = await resp.json();

                allTokens = data.tokens;
                document.getElementById('lastScanTime').innerText = `${data.scan_time} UTC`;
                updateStats();
                switchView(currentView);
            } catch (err) {
                alert('Scan error: ' + err);
            } finally {
                document.getElementById('loading').style.display = 'none';
                document.getElementById('cardContainer').style.opacity = '1';
                document.getElementById('tableContainer').style.opacity = '1';
            }
        }

        function changeSort(col) {
            if (currentSort.col === col) {
                currentSort.dir = currentSort.dir === 'asc' ? 'desc' : 'asc';
            } else {
                currentSort.col = col;
                currentSort.dir = (col === 'diff_pct' || col === 'market_cap' || col.startsWith('pct_') || col === 'volume_24h' || col === 'green_count') ? 'desc' : (col === 'funding_rate' ? 'asc' : 'asc');
            }
            renderView();
        }

        function getFilteredTokens() {
            const search = document.getElementById('searchInput').value.toUpperCase().trim();
            const minMc = parseAmount(document.getElementById('minMcInput').value);
            const maxMc = parseAmount(document.getElementById('maxMcInput').value);

            const aboveKey = `above_${currentFilterTf}`;
            const pctKey = `pct_${currentFilterTf}`;

            // Range % filters
            const minDev = parseFloat(document.getElementById('min_dev_input').value);
            const maxDev = parseFloat(document.getElementById('max_dev_input').value);

            const tfRangeFilters = {};
            ['1d', '1w', '2w', '1m', '3m', '6m'].forEach(tf => {
                const minEl = document.getElementById(`min_${tf}_input`);
                const maxEl = document.getElementById(`max_${tf}_input`);
                tfRangeFilters[tf] = {
                    min: minEl && minEl.value !== '' ? parseFloat(minEl.value) : NaN,
                    max: maxEl && maxEl.value !== '' ? parseFloat(maxEl.value) : NaN
                };
            });

            let filtered = allTokens.filter(t => {
                if (currentFilterCond === 'above' && !t[aboveKey]) return false;
                if (currentFilterCond === 'below' && t[aboveKey]) return false;
                if (currentFilterVol > 0 && t.volume_24h < currentFilterVol) return false;
                if (search && !t.symbol.toUpperCase().includes(search) && !t.base.toUpperCase().includes(search)) return false;

                // MC filter
                if (minMc !== null && minMc > 0) {
                    if (!t.market_cap || t.market_cap < minMc) return false;
                }
                if (maxMc !== null && maxMc > 0) {
                    if (!t.market_cap || t.market_cap > maxMc) return false;
                }

                // FR filter
                if (currentFilterFr === 'neg' && t.funding_rate >= 0) return false;
                if (currentFilterFr === 'neg_deep' && t.funding_rate >= -0.05) return false;
                if (currentFilterFr === 'pos' && t.funding_rate <= 0) return false;
                if (currentFilterFr === 'pos_high' && t.funding_rate <= 0.03) return false;

                // Dev % filter
                const curDev = t[pctKey];
                if (!isNaN(minDev) && curDev < minDev) return false;
                if (!isNaN(maxDev) && curDev > maxDev) return false;

                // Individual timeframe % filters (1D, 1W, 2W, 1M, 3M, 6M)
                for (const tf of ['1d', '1w', '2w', '1m', '3m', '6m']) {
                    const range = tfRangeFilters[tf];
                    const val = t[`pct_${tf}`];
                    if (!isNaN(range.min) && val < range.min) return false;
                    if (!isNaN(range.max) && val > range.max) return false;
                }

                return true;
            });

            filtered.sort((a, b) => {
                let vA, vB;
                if (currentSort.col === 'diff_pct') {
                    vA = a[pctKey];
                    vB = b[pctKey];
                } else {
                    vA = a[currentSort.col];
                    vB = b[currentSort.col];
                }

                if (typeof vA === 'string') {
                    return currentSort.dir === 'asc' ? vA.localeCompare(vB) : vB.localeCompare(vA);
                }
                return currentSort.dir === 'asc' ? (vA - vB) : (vB - vA);
            });

            return filtered;
        }

        function renderView() {
            const filtered = getFilteredTokens();
            document.getElementById('resultCount').innerText = filtered.length;
            updateActiveTags();

            if (currentView === 'cards') {
                renderCards(filtered);
            } else {
                renderTable(filtered);
            }
        }

        function renderCards(filtered) {
            const container = document.getElementById('cardContainer');
            container.innerHTML = '';
            const pctKey = `pct_${currentFilterTf}`;
            const openKey = `open_${currentFilterTf}`;

            filtered.forEach((t, index) => {
                const diffPct = t[pctKey];
                const openVal = t[openKey];
                const isGreen = diffPct >= 0;
                const diffCls = isGreen ? 'green' : 'red';
                const diffSymbol = isGreen ? '▲ +' : '▼ ';
                
                let strengthBadge = '';
                if (t.green_count >= 5) strengthBadge = `<span class="pill-tag green">🟢 Bull (${t.green_count}/6)</span>`;
                else if (t.green_count >= 3) strengthBadge = `<span class="pill-tag yellow">🟡 Neutral (${t.green_count}/6)</span>`;
                else strengthBadge = `<span class="pill-tag red">🔴 Bear (${t.green_count}/6)</span>`;

                const makeTile = (label, pct, tfKey) => {
                    const isHigh = currentFilterTf === tfKey;
                    const tileCls = pct >= 0 ? 'text-green' : 'text-red';
                    const sign = pct >= 0 ? '+' : '';
                    return `
                        <div class="tf-tile ${isHigh ? 'highlight' : ''}">
                            <div class="tf-tile-name">${label}</div>
                            <div class="tf-tile-val ${tileCls}">${sign}${pct.toFixed(1)}%</div>
                        </div>
                    `;
                };

                const card = document.createElement('div');
                card.className = 'crypto-card';
                card.innerHTML = `
                    <div class="card-header-row">
                        <div class="card-sym-title">
                            <span style="color:var(--text-dim); font-size:11px;">#${index + 1}</span>
                            <span>${t.base}</span>
                            <span class="symbol-sub">USDT</span>
                            ${strengthBadge}
                        </div>
                        <div class="card-price-display">$${formatNumber(t.current_price)}</div>
                    </div>

                    <div class="card-focus-bar">
                        <div>
                            <div class="focus-title">${currentFilterTf.toUpperCase()} Open Dev. (UTC)</div>
                            <div class="focus-open">Open: $${formatNumber(openVal)}</div>
                        </div>
                        <div class="diff-badge ${diffCls}" style="font-size:13px;">
                            ${diffSymbol}${Math.abs(diffPct).toFixed(2)}%
                        </div>
                    </div>

                    <div class="card-metrics-row">
                        <div>⚡ FR: ${getFundingBadge(t.funding_rate)}</div>
                        <div>💎 MC: ${formatMarketCap(t.market_cap)}</div>
                        <div>📊 Vol: <b>${formatVolume(t.volume_24h)}</b></div>
                    </div>

                    <div class="tf-grid">
                        ${makeTile('1D', t.pct_1d, '1d')}
                        ${makeTile('1W', t.pct_1w, '1w')}
                        ${makeTile('2W', t.pct_2w, '2w')}
                        ${makeTile('1M', t.pct_1m, '1m')}
                        ${makeTile('3M', t.pct_3m, '3m')}
                        ${makeTile('6M', t.pct_6m, '6m')}
                    </div>
                `;
                container.appendChild(card);
            });
        }

        function renderTable(filtered) {
            document.querySelectorAll('th').forEach(th => th.classList.remove('active-sort'));
            const activeTh = document.getElementById(`th_${currentSort.col}`);
            if (activeTh) activeTh.classList.add('active-sort');

            const tbody = document.getElementById('tableBody');
            tbody.innerHTML = '';
            const pctKey = `pct_${currentFilterTf}`;
            const openKey = `open_${currentFilterTf}`;

            filtered.forEach((t, index) => {
                const tr = document.createElement('tr');
                const diffPct = t[pctKey];
                const openVal = t[openKey];
                const isGreen = diffPct >= 0;
                const diffCls = isGreen ? 'green' : 'red';
                const diffSymbol = isGreen ? '▲ +' : '▼ ';
                
                let strengthTag = '';
                if (t.green_count >= 5) strengthTag = `<span class="pill-tag green">Bull (${t.green_count}/6)</span>`;
                else if (t.green_count >= 3) strengthTag = `<span class="pill-tag yellow">Neutral (${t.green_count}/6)</span>`;
                else strengthTag = `<span class="pill-tag red">Bear (${t.green_count}/6)</span>`;

                tr.innerHTML = `
                    <td class="col-sticky-1" style="color:var(--text-dim);">${index + 1}</td>
                    <td class="col-sticky-2">
                        <span class="symbol-cell">${t.base}</span>
                        <span class="symbol-sub">USDT</span>
                    </td>
                    <td style="font-weight:600;">$${formatNumber(t.current_price)}</td>
                    <td>${getFundingBadge(t.funding_rate)}</td>
                    <td>${formatMarketCap(t.market_cap)}</td>
                    <td class="col-focus" style="color:var(--text-dim);">$${formatNumber(openVal)}</td>
                    <td class="col-focus">
                        <span class="diff-badge ${diffCls}">
                            ${diffSymbol}${Math.abs(diffPct).toFixed(2)}%
                        </span>
                    </td>
                    <td class="${t.pct_1d >= 0 ? 'text-green' : 'text-red'}">${t.pct_1d >= 0 ? '+' : ''}${t.pct_1d.toFixed(2)}%</td>
                    <td class="${t.pct_1w >= 0 ? 'text-green' : 'text-red'}">${t.pct_1w >= 0 ? '+' : ''}${t.pct_1w.toFixed(2)}%</td>
                    <td class="${t.pct_2w >= 0 ? 'text-green' : 'text-red'}">${t.pct_2w >= 0 ? '+' : ''}${t.pct_2w.toFixed(2)}%</td>
                    <td class="${t.pct_1m >= 0 ? 'text-green' : 'text-red'}">${t.pct_1m >= 0 ? '+' : ''}${t.pct_1m.toFixed(2)}%</td>
                    <td class="${t.pct_3m >= 0 ? 'text-green' : 'text-red'}">${t.pct_3m >= 0 ? '+' : ''}${t.pct_3m.toFixed(2)}%</td>
                    <td class="${t.pct_6m >= 0 ? 'text-green' : 'text-red'}">${t.pct_6m >= 0 ? '+' : ''}${t.pct_6m.toFixed(2)}%</td>
                    <td>${formatVolume(t.volume_24h)}</td>
                    <td>${strengthTag}</td>
                `;
                tbody.appendChild(tr);
            });
        }

        function exportCSV() {
            const minMc = parseAmount(document.getElementById('minMcInput').value) || 0;
            const maxMc = parseAmount(document.getElementById('maxMcInput').value) || 0;
            window.location.href = `/api/export-csv?timeframe=${currentFilterTf}&condition=${currentFilterCond}&min_vol=${currentFilterVol}&fr=${currentFilterFr}&min_mc=${minMc}&max_mc=${maxMc}`;
        }

        // Auto initial fetch
        fetchScan();
    </script>
</body>
</html>
"""

class ScannerHttpHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        global CACHE_DATA
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == '/' or parsed.path == '/index.html':
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(HTML_TEMPLATE.encode('utf-8'))
        elif parsed.path == '/api/scan':
            query = urllib.parse.parse_qs(parsed.query)
            force = 'force' in query
            if CACHE_DATA is None or force:
                data = run_scan()
            else:
                data = CACHE_DATA
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))
        elif parsed.path == '/api/export-csv':
            query = urllib.parse.parse_qs(parsed.query)
            tf = query.get('timeframe', ['1w'])[0]
            cond = query.get('condition', ['all'])[0]
            dist = query.get('dist', ['all'])[0]
            fr = query.get('fr', ['all'])[0]
            min_vol = float(query.get('min_vol', [0])[0])
            min_mc = float(query.get('min_mc', [0])[0])
            max_mc = float(query.get('max_mc', [0])[0])

            if CACHE_DATA is None:
                CACHE_DATA = run_scan()

            csv_path = export_to_csv(CACHE_DATA, filter_tf=tf, filter_cond=cond, min_vol=min_vol, dist_filter=dist, min_mc=min_mc, max_mc=max_mc, fr_filter=fr)
            with open(csv_path, 'rb') as f:
                content = f.read()
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv; charset=utf-8')
            self.send_header('Content-Disposition', f'attachment; filename="binance_utc_{tf}_{cond}.csv"')
            self.end_headers()
            self.wfile.write(content)
        else:
            self.send_response(404)
            self.end_headers()

def start_web_server():
    # Support cloud environment variables HOST & PORT for online hosting
    host = os.environ.get('HOST', '0.0.0.0')
    env_port = os.environ.get('PORT')

    if env_port:
        port = int(env_port)
        server = HTTPServer((host, port), ScannerHttpHandler)
        active_port = port
    else:
        server = None
        default_port = 5000
        active_port = default_port
        for p in range(default_port, default_port + 20):
            try:
                server = HTTPServer((host, p), ScannerHttpHandler)
                active_port = p
                break
            except OSError:
                continue

    if not server:
        print(f"\n[LỖI] Không thể mở cổng mạng.")
        return

    url = f"http://127.0.0.1:{active_port}"
    print(f"\n================================================================")
    print(f" BINANCE FUTURES SCANNER: MULTI-TIMEFRAME & FUNDING RATE & MC  ")
    print(f"================================================================")
    print(f"\n🚀 Máy chủ đang chạy tại: {url} (Host: {host}:{active_port})")
    print(f"✔ Đã bật tính năng Funding Rate Binance thời gian thực!")
    print(f"(Nhấn Ctrl+C trong cửa sổ này nếu muốn dừng máy chủ).\n")

    if not os.environ.get('PORT'):
        try:
            webbrowser.open(url)
        except Exception:
            pass

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nĐã dừng máy chủ.")
        server.server_close()

if __name__ == '__main__':
    start_web_server()
