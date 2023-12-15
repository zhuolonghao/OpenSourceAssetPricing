# this script is to download info for a bunch of tickers.
# the info includes price, financials, balance sheet, cash flow and other basic info
# the script leverages the python package "yfinance" and builds upon the multi.py

# takes 49 secs
import yfinance as yf
import pandas as _pd
import time

def download(tickers, period="2y", interval="1d", progress=True):
    """Download yahoo tickers
    :Parameters:
        tickers : str, list
            List of tickers to download
        period : str
            Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
            Either Use period parameter or use start and end
        interval : str
            Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
            Intraday data cannot extend last 60 days
    """

    # create ticker list
    tickers = tickers if isinstance(
        tickers, (list, set, tuple)) else tickers.replace(',', ' ').split()
    #tickers = list(set([ticker.upper() for ticker in tickers]))

    if progress:
        yf.shared._PROGRESS_BAR = yf.utils.ProgressBar(len(tickers), 'completed')

    _DFS = {}
    _error_ticker = {}
    for i, ticker in enumerate(tickers):
        _DFS[ticker], _error_ticker[ticker] = _download_one(ticker, period=period, interval=interval, progress=progress)
        time.sleep(1)
    if progress:
        yf.shared._PROGRESS_BAR.completed()

    data = _pd.concat(_DFS.values(), axis=0, ignore_index=True)

    return data, _error_ticker

def _download_one(ticker, period="2y", interval="1d", progress=True):
    data = None
    error_ticker = None
    object = yf.Ticker(ticker)
    try:
        price = object.history(period=period, interval=interval)
        info = object.info
        fast_info = object.fast_info
        finra_13f = object.major_holders
        accounting = {}
        accounting['Balance_sheet'] = object.balance_sheet.T
        accounting['Cash_flow'] = object.cash_flow.T
        accounting['Income'] = object.financials.T
        accountingQ = {}
        accountingQ['Balance_sheet'] = object.get_balancesheet(freq='quarterly').T
        accountingQ['Cash_flow'] = object.get_cashflow(freq='quarterly').T
        accountingQ['Income'] = object.get_financials(freq='quarterly').T

        base = price.copy()
        base['date'] = base.index
        base.index = base.index.strftime('%Y%m%d')
        for key, df in accounting.items():
            df.index = df.index.map(lambda x: x + 0 * _pd.offsets.BDay())
            df.index = df.index.strftime('%Y%m%d')
            df.columns = ['A_' + x.lower()  for x in df.columns]
            base = base.join(df, how='left')
        for key, df in accountingQ.items():
            df.index = df.index.map(lambda x: x + 0 * _pd.offsets.BDay())
            df.index = df.index.strftime('%Y%m%d')
            df.columns = ['Q_' + x.lower() for x in df.columns]
            base = base.join(df, how='left')
        base['shr_of_IO'] = float(finra_13f.loc[finra_13f[1].eq('% of Shares Held by Institutions'), 0].values[0][:-1])
        base['obs_of_IO'] = float(
            finra_13f.loc[finra_13f[1].eq('Number of Institutions Holding Shares'), 0].values[0][:-1])
        base['short'] = info['sharesShort']
        base['short_prev'] = info['sharesShortPriorMonth']
        base['e_v'] = info['enterpriseValue']
        base['mkt_cap'] = info['marketCap']
        base['exchange'] = fast_info['exchange']
        base['symbol'] = ticker
        data = base.fillna(method='ffill')
    except Exception as e:
        # glob try/except needed as current thead implementation breaks if exception is raised.
        error_ticker = ticker
        print(f'\n Error in downloading {ticker}')

    if progress:
        yf.shared._PROGRESS_BAR.animate()
    return data, error_ticker

# import timeit
# start_time = timeit.default_timer()
# tickers=['MSFT', 'WFC'] * 50
# df = download(tickers)
# print(f'completed in: {timeit.default_timer() - start_time} seconds')

# import timeit
# start_time = timeit.default_timer()
# tickers=['MSFT']
# object = yf.Ticker('MSFT')
# start_time = timeit.default_timer()
# object.get_history_metadata()
# print(f'completed in: {timeit.default_timer() - start_time} seconds')
# start_time = timeit.default_timer()
# object.info
# print(f'completed in: {timeit.default_timer() - start_time} seconds')