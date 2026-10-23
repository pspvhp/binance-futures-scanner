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
    try:
        url = 'https://www.binance.com/bapi/composite/v1/public/marketing/symbol/list'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        data = json.loads(urllib.request.urlopen(req, timeout=5).read()).get('data', [])
        for item in data:
            base = item.get('baseAsset', '').upper()
            sym = item.get('symbol', '').upper()
            mc = item.get('marketCap')
            if base and mc: mc_map[base] = float(mc)
            if sym and mc: mc_map[sym] = float(mc)
    except Exception:
        pass

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
                for prefix in ['1000000', '1000', '100']:
                    if clean_base.startswith(prefix) and len(clean_base) > len(prefix):
                        clean_base = clean_base[len(prefix):]
                        break

                market_cap = mc_map.get(sym, mc_map.get(base, mc_map.get(clean_base, 0)))
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
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Binance Futures Multi-Timeframe Scanner & Funding Rate</title>
    <style>
        :root {
            --bg: #0b0e14;
            --card-bg: #151a24;
            --card-hover: #1c2230;
            --border: #222b3d;
            --text: #eaecef;
            --text-dim: #848e9c;
            --green: #0ecb81;
            --green-bg: rgba(14, 203, 129, 0.12);
            --red: #f6465d;
            --red-bg: rgba(246, 70, 93, 0.12);
            --yellow: #f0b90b;
            --yellow-bg: rgba(240, 185, 11, 0.15);
            --purple: #b388ff;
            --purple-bg: rgba(179, 136, 255, 0.12);
            --orange: #ff9800;
            --orange-bg: rgba(255, 152, 0, 0.15);
        }
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
        body { background: var(--bg); color: var(--text); padding: 22px; min-height: 100vh; }
        
        .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; flex-wrap: wrap; gap: 16px; border-bottom: 1px solid var(--border); padding-bottom: 16px; }
        .title-group h1 { font-size: 22px; font-weight: 700; color: #fff; display: flex; align-items: center; gap: 10px; }
        .title-group p { font-size: 13px; color: var(--text-dim); margin-top: 4px; }
        .live-tag { display: inline-flex; align-items: center; gap: 6px; background: rgba(14, 203, 129, 0.15); color: var(--green); padding: 4px 10px; border-radius: 20px; font-size: 12px; font-weight: 600; border: 1px solid rgba(14, 203, 129, 0.3); }
        .live-dot { width: 8px; height: 8px; background: var(--green); border-radius: 50%; box-shadow: 0 0 8px var(--green); }
        .utc-badge { background: #2962ff25; color: #64b5f6; border: 1px solid #2962ff55; padding: 3px 8px; border-radius: 4px; font-size: 11px; font-weight: 700; }

        /* Configuration Panel */
        .config-card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 12px; padding: 18px; margin-bottom: 20px; box-shadow: 0 4px 20px rgba(0,0,0,0.25); }
        .config-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 18px; }
        .config-section-title { font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; color: var(--text-dim); margin-bottom: 8px; display: flex; align-items: center; justify-content: space-between; }
        
        .pill-group { display: flex; gap: 8px; flex-wrap: wrap; }
        .pill-btn { padding: 7px 13px; border-radius: 8px; border: 1px solid var(--border); background: #19202e; color: var(--text); cursor: pointer; font-size: 12.5px; font-weight: 600; transition: all 0.2s; user-select: none; }
        .pill-btn:hover { background: #232c40; color: #fff; border-color: #3e4c6b; }
        .pill-btn.active { background: var(--yellow); color: #000; border-color: var(--yellow); font-weight: 700; }

        .cond-btn.active.cond-above { background: var(--green); color: #000; border-color: var(--green); }
        .cond-btn.active.cond-below { background: var(--red); color: #fff; border-color: var(--red); }
        .cond-btn.active.cond-all { background: var(--yellow); color: #000; border-color: var(--yellow); }

        .filter-select { padding: 8px 12px; border-radius: 8px; border: 1px solid var(--border); background: #19202e; color: #fff; font-size: 13px; outline: none; width: 100%; cursor: pointer; }
        .filter-select:focus { border-color: var(--yellow); }

        .mc-inputs-row { display: flex; gap: 8px; align-items: center; margin-bottom: 8px; }
        .input-box { flex: 1; padding: 8px 12px; border-radius: 8px; border: 1px solid var(--border); background: #19202e; color: #fff; font-size: 13px; outline: none; }
        .input-box:focus { border-color: var(--yellow); }

        .action-row { display: flex; gap: 12px; align-items: center; flex-wrap: wrap; margin-top: 16px; padding-top: 16px; border-top: 1px solid rgba(255,255,255,0.06); justify-content: space-between; }
        .btn-main { background: var(--yellow); color: #000; font-weight: 700; padding: 10px 22px; border-radius: 8px; border: none; cursor: pointer; font-size: 14px; transition: all 0.2s; display: inline-flex; align-items: center; gap: 8px; }
        .btn-main:hover { filter: brightness(1.1); transform: translateY(-1px); }
        .btn-secondary { background: #19202e; color: var(--text); padding: 10px 18px; border-radius: 8px; border: 1px solid var(--border); cursor: pointer; font-size: 14px; font-weight: 600; transition: all 0.2s; }
        .btn-secondary:hover { background: #232c40; color: #fff; }

        /* Stats Cards */
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 14px; margin-bottom: 20px; }
        .stat-card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 10px; padding: 14px 18px; }
        .stat-label { color: var(--text-dim); font-size: 12px; margin-bottom: 4px; }
        .stat-value { font-size: 20px; font-weight: 700; }
        .stat-value.green { color: var(--green); }
        .stat-value.red { color: var(--red); }
        .stat-value.yellow { color: var(--yellow); }
        .stat-value.blue { color: #64b5f6; }
        .stat-value.orange { color: var(--orange); }
        .stat-sub { font-size: 11px; color: var(--text-dim); margin-top: 3px; }

        /* Controls bar */
        .controls { display: flex; gap: 14px; margin-bottom: 14px; flex-wrap: wrap; align-items: center; justify-content: space-between; }
        .search-box { padding: 9px 14px; border-radius: 8px; border: 1px solid var(--border); background: var(--card-bg); color: #fff; font-size: 14px; min-width: 270px; outline: none; }
        .search-box:focus { border-color: var(--yellow); }

        /* Table */
        .table-container { background: var(--card-bg); border: 1px solid var(--border); border-radius: 12px; overflow-x: auto; box-shadow: 0 4px 20px rgba(0,0,0,0.3); }
        table { width: 100%; border-collapse: collapse; text-align: left; font-size: 13.5px; }
        th { background: #171d29; padding: 12px 14px; color: var(--text-dim); font-weight: 600; cursor: pointer; user-select: none; border-bottom: 1px solid var(--border); white-space: nowrap; }
        th:hover { color: #fff; }
        th.active-sort { color: var(--yellow); }
        th.col-focus { background: #1a2538; color: #fff; }
        td { padding: 12px 14px; border-bottom: 1px solid var(--border); white-space: nowrap; }
        td.col-focus { background: rgba(41, 98, 255, 0.05); }
        tr:hover td { background: var(--card-hover); }
        tr:hover td.col-focus { background: rgba(41, 98, 255, 0.12); }
        
        .pill-tag { display: inline-flex; align-items: center; padding: 3px 8px; border-radius: 4px; font-size: 11px; font-weight: 700; }
        .pill-tag.green { background: var(--green-bg); color: var(--green); border: 1px solid rgba(14, 203, 129, 0.3); }
        .pill-tag.red { background: var(--red-bg); color: var(--red); border: 1px solid rgba(246, 70, 93, 0.3); }
        .pill-tag.yellow { background: var(--yellow-bg); color: var(--yellow); border: 1px solid rgba(240, 185, 11, 0.3); }
        .pill-tag.orange { background: var(--orange-bg); color: var(--orange); border: 1px solid rgba(255, 152, 0, 0.4); }

        .diff-badge { display: inline-flex; align-items: center; gap: 4px; padding: 4px 10px; border-radius: 6px; font-weight: 700; font-size: 13px; }
        .diff-badge.green { background: rgba(14, 203, 129, 0.18); color: var(--green); border: 1px solid rgba(14, 203, 129, 0.4); }
        .diff-badge.red { background: rgba(246, 70, 93, 0.18); color: var(--red); border: 1px solid rgba(246, 70, 93, 0.4); }
        
        .text-green { color: var(--green); font-weight: 700; }
        .text-red { color: var(--red); font-weight: 700; }
        .text-purple { color: var(--purple); font-weight: 600; }
        .text-orange { color: var(--orange); font-weight: 700; }
        .symbol-cell { font-weight: 700; color: #fff; font-size: 14px; }
        .symbol-sub { font-size: 11px; color: var(--text-dim); margin-left: 3px; }

        /* Loading */
        #loading { display: none; text-align: center; padding: 50px 20px; }
        .spinner { border: 4px solid rgba(240, 185, 11, 0.15); border-top: 4px solid var(--yellow); border-radius: 50%; width: 40px; height: 40px; animation: spin 0.8s linear infinite; margin: 0 auto 14px; }
        @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
        .loading-text { font-size: 16px; font-weight: 600; color: var(--yellow); }
        .loading-sub { font-size: 13px; color: var(--text-dim); margin-top: 6px; }
    </style>
</head>
<body>
    <div class="header">
        <div class="title-group">
            <h1>Binance Futures Scanner <span class="utc-badge">🌐 UTC 00:00</span> <span class="live-tag"><span class="live-dot"></span> LIVE</span></h1>
            <p>Quét đa khung thời gian quốc tế, điền Market Cap tự do & theo dõi Funding Rate sàn Binance</p>
        </div>
        <div style="font-size:13px; color:var(--text-dim);" id="lastScanTime">Chưa cập nhật</div>
    </div>

    <!-- CONFIGURATION PANEL -->
    <div class="config-card">
        <div class="config-grid">
            <!-- 1. Chọn khung thời gian lọc -->
            <div>
                <div class="config-section-title">⏱️ 1. Khung Thời Gian (UTC)</div>
                <div class="pill-group" id="tfGroup">
                    <button class="pill-btn" data-tf="1d" onclick="selectFilterTf('1d')">1D (Hôm nay)</button>
                    <button class="pill-btn active" data-tf="1w" onclick="selectFilterTf('1w')">1W (Tuần này)</button>
                    <button class="pill-btn" data-tf="2w" onclick="selectFilterTf('2w')">2W (2 Tuần)</button>
                    <button class="pill-btn" data-tf="1m" onclick="selectFilterTf('1m')">1M (Tháng này)</button>
                    <button class="pill-btn" data-tf="3m" onclick="selectFilterTf('3m')">3M (Quý)</button>
                    <button class="pill-btn" data-tf="6m" onclick="selectFilterTf('6m')">6M (6 Tháng)</button>
                </div>
            </div>

            <!-- 2. Điều kiện trên/dưới mở cửa -->
            <div>
                <div class="config-section-title">🎯 2. Điều Kiện Vị Trí Giá</div>
                <div class="pill-group" id="condGroup">
                    <button class="pill-btn cond-btn active cond-above" data-cond="above" onclick="selectFilterCond('above')">🟢 TRÊN Mở Cửa (Xanh)</button>
                    <button class="pill-btn cond-btn cond-below" data-cond="below" onclick="selectFilterCond('below')">🔴 DƯỚI Mở Cửa (Đỏ)</button>
                    <button class="pill-btn cond-btn cond-all" data-cond="all" onclick="selectFilterCond('all')">⚪ Tất Cả</button>
                </div>
            </div>

            <!-- 3. Lọc Market Cap Điền Tay -->
            <div>
                <div class="config-section-title">
                    <span>💎 3. Điền Market Cap (Vốn Hoá)</span>
                    <span style="font-weight:400; text-transform:none; font-size:11px; color:#b388ff;" id="mcActiveLabel">Tất cả</span>
                </div>
                <div class="mc-inputs-row">
                    <input type="text" id="minMcInput" class="input-box" placeholder="Min (VD: 50M, 100M)" oninput="onMcInput()">
                    <span style="color:var(--text-dim);">-</span>
                    <input type="text" id="maxMcInput" class="input-box" placeholder="Max (VD: 1B, 500M)" oninput="onMcInput()">
                </div>
                <div class="pill-group">
                    <button class="pill-btn" style="padding:3px 7px; font-size:11px;" onclick="setMcPreset(0, 0)">Tất cả</button>
                    <button class="pill-btn" style="padding:3px 7px; font-size:11px;" onclick="setMcPreset(0, 50e6)">&lt; $50M</button>
                    <button class="pill-btn" style="padding:3px 7px; font-size:11px;" onclick="setMcPreset(50e6, 300e6)">$50M - $300M</button>
                    <button class="pill-btn" style="padding:3px 7px; font-size:11px;" onclick="setMcPreset(300e6, 1e9)">$300M - $1B</button>
                    <button class="pill-btn" style="padding:3px 7px; font-size:11px;" onclick="setMcPreset(1e9, 0)">&gt; $1B</button>
                </div>
            </div>

            <!-- 4. Lọc Funding Rate & Volume -->
            <div>
                <div class="config-section-title">⚡ 4. Funding Rate & Volume 24h</div>
                <div style="display:flex; gap:8px;">
                    <select id="frSelect" class="filter-select" onchange="renderTable()" style="flex:1;">
                        <option value="all">Mọi Funding Rate</option>
                        <option value="neg">🔥 Funding Âm (&lt; 0%)</option>
                        <option value="neg_deep">⚠️ Âm Sâu (&lt; -0.05%) Squeeze</option>
                        <option value="pos">📈 Funding Dương (&gt; 0%)</option>
                        <option value="pos_high">⚡ Dương Cao (&gt; 0.03%)</option>
                    </select>
                    <select id="minVolSelect" class="filter-select" onchange="renderTable()" style="flex:1;">
                        <option value="0">Mọi Volume</option>
                        <option value="1000000">&gt; $1M Vol</option>
                        <option value="5000000">&gt; $5M Vol</option>
                        <option value="20000000">&gt; $20M Vol</option>
                    </select>
                </div>
            </div>
        </div>

        <div class="action-row">
            <div style="font-size:13px; color:var(--text-dim);">
                Đang xét: <b style="color:var(--yellow);" id="activeConfigSummary">Khung 1W | 🟢 Nằm TRÊN giá mở cửa</b>
            </div>
            <div style="display:flex; gap:10px;">
                <button class="btn-main" onclick="fetchScan(true)">🚀 QUÉT LẠI DỮ LIỆU (RESCAN)</button>
                <button class="btn-secondary" onclick="exportCSV()">📥 Xuất File Excel (CSV)</button>
            </div>
        </div>
    </div>

    <!-- STATS SUMMARY -->
    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-label">Tổng cặp Futures</div>
            <div class="stat-value" id="statTotal">-</div>
            <div class="stat-sub">Hợp đồng USDT-M Perpetual</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">NẰM TRÊN mở cửa</div>
            <div class="stat-value green" id="statAbove">-</div>
            <div class="stat-sub" id="statAbovePct">-</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">NẰM DƯỚI mở cửa</div>
            <div class="stat-value red" id="statBelow">-</div>
            <div class="stat-sub" id="statBelowPct">-</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Cách Open TB (Đang xét)</div>
            <div class="stat-value blue" id="statAvgDist">-</div>
            <div class="stat-sub" id="statAvgDistSub">Trung bình độ lệch %</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Token Funding Âm 🔥</div>
            <div class="stat-value orange" id="statNegFr">-</div>
            <div class="stat-sub" id="statNegFrSub">Canh Short Squeeze</div>
        </div>
    </div>

    <!-- CONTROLS & SEARCH -->
    <div class="controls">
        <div style="font-size:14px; font-weight:600; color:#fff;">
            Bảng Thống Kê (<span id="resultCount">0</span> token thỏa mãn)
        </div>
        <input type="text" id="searchInput" class="search-box" placeholder="🔍 Tìm theo token (VD: BTC, SOL, PEPE, SUI)..." oninput="renderTable()">
    </div>

    <!-- LOADING -->
    <div id="loading">
        <div class="spinner"></div>
        <div class="loading-text">⚡ Đang quét toàn bộ thị trường Binance Futures & Funding Rate...</div>
        <div class="loading-sub">Quá trình này mất khoảng 5 - 8 giây, vui lòng chờ trong giây lát.</div>
    </div>

    <!-- DATA TABLE -->
    <div class="table-container" id="tableContainer">
        <table>
            <thead>
                <tr>
                    <th onclick="changeSort('index')">#</th>
                    <th onclick="changeSort('symbol')">Cặp Giao Dịch</th>
                    <th onclick="changeSort('current_price')">Giá Hiện Tại</th>
                    <th onclick="changeSort('funding_rate')">⚡ Funding Rate ⬍</th>
                    <th onclick="changeSort('market_cap')">💎 Market Cap ⬍</th>
                    <th class="col-focus" id="th_open_active">Giá Open UTC</th>
                    <th class="col-focus" id="th_diff_pct" onclick="changeSort('diff_pct')">🔥 Cách Open UTC (%) ⬍</th>
                    <th id="th_pct_1d" onclick="changeSort('pct_1d')">% 1D ⬍</th>
                    <th id="th_pct_1w" onclick="changeSort('pct_1w')">% 1W ⬍</th>
                    <th id="th_pct_2w" onclick="changeSort('pct_2w')">% 2W ⬍</th>
                    <th id="th_pct_1m" onclick="changeSort('pct_1m')">% 1M ⬍</th>
                    <th id="th_pct_3m" onclick="changeSort('pct_3m')">% 3M ⬍</th>
                    <th id="th_pct_6m" onclick="changeSort('pct_6m')">% 6M ⬍</th>
                    <th onclick="changeSort('volume_24h')">Volume 24h</th>
                    <th onclick="changeSort('green_count')">Sức Mạnh (0-6) ⬍</th>
                </tr>
            </thead>
            <tbody id="tableBody"></tbody>
        </table>
    </div>

    <script>
        let currentFilterTf = '1w';
        let currentFilterCond = 'above';
        let allTokens = [];
        let currentSort = { col: 'diff_pct', dir: 'desc' };

        const tfLabels = {
            '1d': 'Hôm Nay (1D UTC)',
            '1w': 'Nến Tuần (1W UTC)',
            '2w': 'Nến 2 Tuần (2W UTC)',
            '1m': 'Nến Tháng (1M UTC)',
            '3m': 'Nến Quý (3M UTC)',
            '6m': 'Nến 6 Tháng (6M UTC)'
        };

        function parseAmount(valStr) {
            if (!valStr) return null;
            valStr = valStr.toString().trim().toUpperCase().replace(/[$,\\s]/g, '');
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
            if (num >= 1e6) return '$' + (num / 1e6).toFixed(2) + 'M';
            if (num >= 1e3) return '$' + (num / 1e3).toFixed(1) + 'K';
            return '$' + num.toFixed(0);
        }

        function formatMarketCap(mc) {
            if (!mc || mc <= 0) return '<span style="color:var(--text-dim);">N/A</span>';
            return `<span class="text-purple">${formatVolume(mc)}</span>`;
        }

        function formatFundingRate(fr) {
            if (fr === null || fr === undefined) return '<td>-</td>';
            const sign = fr > 0 ? '+' : '';
            if (fr < -0.05) {
                return `<td><span class="pill-tag red">🔥 ${fr.toFixed(4)}%</span></td>`;
            } else if (fr < 0) {
                return `<td><span class="text-orange">${fr.toFixed(4)}%</span></td>`;
            } else if (fr > 0.03) {
                return `<td><span class="pill-tag green">⚡ +${fr.toFixed(4)}%</span></td>`;
            }
            return `<td><span style="color:var(--text-dim);">${sign}${fr.toFixed(4)}%</span></td>`;
        }

        function formatPctCell(pct) {
            if (pct === null || pct === undefined) return '<td>-</td>';
            const isGreen = pct >= 0;
            const sign = isGreen ? '+' : '';
            const cls = isGreen ? 'text-green' : 'text-red';
            return `<td class="${cls}">${sign}${pct.toFixed(2)}%</td>`;
        }

        function updateConfigSummary() {
            const condLabels = {
                'above': '🟢 Nằm TRÊN giá mở cửa',
                'below': '🔴 Nằm DƯỚI giá mở cửa',
                'all': '⚪ Tất cả'
            };
            const label = tfLabels[currentFilterTf];
            document.getElementById('activeConfigSummary').innerText = `${label} | ${condLabels[currentFilterCond]}`;
            
            document.getElementById('th_open_active').innerText = `Giá Open (${label})`;
            document.getElementById('th_diff_pct').innerText = `🔥 Cách Open ${label} (%) ⬍`;
        }

        function selectFilterTf(tf) {
            currentFilterTf = tf;
            document.querySelectorAll('#tfGroup .pill-btn').forEach(b => {
                b.classList.toggle('active', b.getAttribute('data-tf') === tf);
            });
            updateConfigSummary();
            updateStats();
            renderTable();
        }

        function selectFilterCond(cond) {
            currentFilterCond = cond;
            document.querySelectorAll('#condGroup .pill-btn').forEach(b => {
                b.classList.toggle('active', b.getAttribute('data-cond') === cond);
            });
            updateConfigSummary();
            renderTable();
        }

        function setMcPreset(minVal, maxVal) {
            document.getElementById('minMcInput').value = minVal > 0 ? (minVal >= 1e9 ? (minVal/1e9)+'B' : (minVal/1e6)+'M') : '';
            document.getElementById('maxMcInput').value = maxVal > 0 ? (maxVal >= 1e9 ? (maxVal/1e9)+'B' : (maxVal/1e6)+'M') : '';
            onMcInput();
        }

        function onMcInput() {
            const minMc = parseAmount(document.getElementById('minMcInput').value);
            const maxMc = parseAmount(document.getElementById('maxMcInput').value);
            let label = 'Tất cả';
            if (minMc && maxMc) label = `${formatVolume(minMc)} - ${formatVolume(maxMc)}`;
            else if (minMc) label = `≥ ${formatVolume(minMc)}`;
            else if (maxMc) label = `≤ ${formatVolume(maxMc)}`;
            document.getElementById('mcActiveLabel').innerText = label;
            renderTable();
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
            document.getElementById('statAbovePct').innerText = `${((aboveCount/total)*100).toFixed(1)}% thị trường`;
            document.getElementById('statBelowPct').innerText = `${((belowCount/total)*100).toFixed(1)}% thị trường`;
            document.getElementById('statNegFr').innerText = negFrCount;
            document.getElementById('statNegFrSub').innerText = `${((negFrCount/total)*100).toFixed(1)}% thị trường âm`;

            let targetGroup = allTokens;
            if (currentFilterCond === 'above') targetGroup = allTokens.filter(t => t[aboveKey]);
            else if (currentFilterCond === 'below') targetGroup = allTokens.filter(t => !t[aboveKey]);

            if (targetGroup.length > 0) {
                const avg = targetGroup.reduce((sum, t) => sum + t[pctKey], 0) / targetGroup.length;
                const sign = avg >= 0 ? '+' : '';
                document.getElementById('statAvgDist').innerText = `${sign}${avg.toFixed(2)}%`;
                document.getElementById('statAvgDistSub').innerText = `TB ${targetGroup.length} token (${tfLabels[currentFilterTf]})`;
            } else {
                document.getElementById('statAvgDist').innerText = '0.00%';
            }
        }

        async function fetchScan(force = false) {
            document.getElementById('loading').style.display = 'block';
            document.getElementById('tableContainer').style.opacity = '0.2';
            try {
                const url = force ? '/api/scan?force=1' : '/api/scan';
                const resp = await fetch(url);
                const data = await resp.json();

                allTokens = data.tokens;
                document.getElementById('lastScanTime').innerText = `Cập nhật lúc: ${data.scan_time} UTC`;
                updateConfigSummary();
                updateStats();
                renderTable();
            } catch (err) {
                alert('Lỗi quét dữ liệu: ' + err);
            } finally {
                document.getElementById('loading').style.display = 'none';
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
            renderTable();
        }

        function renderTable() {
            const search = document.getElementById('searchInput').value.toUpperCase().trim();
            const minVol = parseFloat(document.getElementById('minVolSelect').value) || 0;
            const frFilter = document.getElementById('frSelect').value;
            const minMc = parseAmount(document.getElementById('minMcInput').value);
            const maxMc = parseAmount(document.getElementById('maxMcInput').value);

            const aboveKey = `above_${currentFilterTf}`;
            const pctKey = `pct_${currentFilterTf}`;
            const openKey = `open_${currentFilterTf}`;

            let filtered = allTokens.filter(t => {
                if (currentFilterCond === 'above' && !t[aboveKey]) return false;
                if (currentFilterCond === 'below' && t[aboveKey]) return false;
                if (minVol > 0 && t.volume_24h < minVol) return false;
                if (search && !t.symbol.toUpperCase().includes(search) && !t.base.toUpperCase().includes(search)) return false;

                // Market Cap Filter
                if (minMc !== null && minMc > 0) {
                    if (!t.market_cap || t.market_cap < minMc) return false;
                }
                if (maxMc !== null && maxMc > 0) {
                    if (!t.market_cap || t.market_cap > maxMc) return false;
                }

                // Funding Rate Filter
                if (frFilter === 'neg' && t.funding_rate >= 0) return false;
                if (frFilter === 'neg_deep' && t.funding_rate >= -0.05) return false;
                if (frFilter === 'pos' && t.funding_rate <= 0) return false;
                if (frFilter === 'pos_high' && t.funding_rate <= 0.03) return false;

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

            document.getElementById('resultCount').innerText = filtered.length;

            document.querySelectorAll('th').forEach(th => th.classList.remove('active-sort'));
            const activeTh = document.getElementById(`th_${currentSort.col}`);
            if (activeTh) activeTh.classList.add('active-sort');

            const tbody = document.getElementById('tableBody');
            tbody.innerHTML = '';

            filtered.forEach((t, index) => {
                const tr = document.createElement('tr');
                const diffPct = t[pctKey];
                const openVal = t[openKey];
                const isGreen = diffPct >= 0;
                const diffCls = isGreen ? 'green' : 'red';
                const diffSymbol = isGreen ? '▲ +' : '▼ ';
                
                let strengthTag = '';
                if (t.green_count === 6) {
                    strengthTag = '<span class="pill-tag green">🟢 Siêu Khỏe (6/6)</span>';
                } else if (t.green_count >= 4) {
                    strengthTag = `<span class="pill-tag green">🟢 Khỏe (${t.green_count}/6)</span>`;
                } else if (t.green_count >= 2) {
                    strengthTag = `<span class="pill-tag yellow">🟡 Trung Lập (${t.green_count}/6)</span>`;
                } else {
                    strengthTag = `<span class="pill-tag red">🔴 Yếu (${t.green_count}/6)</span>`;
                }

                tr.innerHTML = `
                    <td style="color:var(--text-dim);">${index + 1}</td>
                    <td>
                        <span class="symbol-cell">${t.base}</span>
                        <span class="symbol-sub">USDT</span>
                    </td>
                    <td style="font-weight:600;">$${formatNumber(t.current_price)}</td>
                    ${formatFundingRate(t.funding_rate)}
                    <td>${formatMarketCap(t.market_cap)}</td>
                    <td class="col-focus" style="color:var(--text-dim);">$${formatNumber(openVal)}</td>
                    <td class="col-focus">
                        <span class="diff-badge ${diffCls}">
                            ${diffSymbol}${Math.abs(diffPct).toFixed(2)}%
                        </span>
                    </td>
                    ${formatPctCell(t.pct_1d)}
                    ${formatPctCell(t.pct_1w)}
                    ${formatPctCell(t.pct_2w)}
                    ${formatPctCell(t.pct_1m)}
                    ${formatPctCell(t.pct_3m)}
                    ${formatPctCell(t.pct_6m)}
                    <td>${formatVolume(t.volume_24h)}</td>
                    <td>${strengthTag}</td>
                `;
                tbody.appendChild(tr);
            });
        }

        function exportCSV() {
            const minVol = document.getElementById('minVolSelect').value || 0;
            const fr = document.getElementById('frSelect').value || 'all';
            const minMc = parseAmount(document.getElementById('minMcInput').value) || 0;
            const maxMc = parseAmount(document.getElementById('maxMcInput').value) || 0;
            window.location.href = `/api/export-csv?timeframe=${currentFilterTf}&condition=${currentFilterCond}&min_vol=${minVol}&fr=${fr}&min_mc=${minMc}&max_mc=${maxMc}`;
        }

        // Tự động quét lần đầu
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
