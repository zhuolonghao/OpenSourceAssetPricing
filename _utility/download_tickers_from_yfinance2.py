# this script is to download info for a bunch of tickers.
# the info includes price, financials, balance sheet, cash flow and other basic info
# the script leverages the python package "yfinance" and builds upon the multi.py
import pandas as pd
import yfinance as yf
import ray as ray
import pandas as _pd

# taks 18 sec
def download(tickers, period="5y", interval="1d"):
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

    yf.shared._PROGRESS_BAR = yf.utils.ProgressBar(len(tickers), 'completed')

    _price = {}
    _fin = {}
    _finQ = {}
    _other = {}
    error = {}
    for t in tickers:
        _price[t], _fin[t], _finQ[t], _other[t], error[t] = \
            _download_one.remote(t, period=period, interval=interval)
        yf.shared._PROGRESS_BAR.animate()

    yf.shared._PROGRESS_BAR.completed()

    price = _pd.concat(ray.get(list(_price.values())), axis=0, ignore_index=True)
    fin = _pd.concat(ray.get(list(_fin.values())), axis=0, ignore_index=True)
    finQ = _pd.concat(ray.get(list(_finQ.values())), axis=0, ignore_index=True)
    other = _pd.DataFrame(ray.get(list(_other.values())))
    error = ray.get(list(error.values()))

    return price, fin, finQ, other, error

@ray.remote(num_returns=5)
def _download_one(ticker, period="5y", interval="1d"):
    price = None
    financials = None
    financialsQ = None
    other_info = None
    error_ticker = None
    object = yf.Ticker(ticker)
    try:
        price = object.history(period=period, interval=interval).\
            rename_axis('date_raw').reset_index().\
            assign(Ticker=ticker)
        financials = _pd.concat(
            [object.financials.T, object.balance_sheet.T, object.cash_flow.T], axis=1).\
            rename_axis('date_raw').reset_index().\
            assign(Ticker=ticker)
        financialsQ = _pd.concat(
            [object.get_financials(freq='quarterly').T, object.get_balancesheet(freq='quarterly').T, object.get_cashflow(freq='quarterly').T], axis=1).\
            rename_axis('date_raw').reset_index().\
            assign(Ticker=ticker)
        major_holders = dict(zip(object.major_holders[1], object.major_holders[0]))
        info = object.info
        del info['companyOfficers']
        other_info = _pd.DataFrame.from_dict([
            {**{'ticker': ticker},
              **object.info,
              **object.fast_info,
              **major_holders}])
    except Exception as e:
        error_ticker = 'fail'
        print(f'\n Error in downloading {ticker}')

    return price, financials, financialsQ, other_info, error_ticker

# import timeit
# start_time = timeit.default_timer()
# tickers=['MSFT', 'WFC', 'AAPL', 'BAC', 'IBM'] * 4
# df = download(tickers)
# print(f'Ray completed in: {timeit.default_timer() - start_time} seconds')
#
# tickers=['MSFT', 'WFC', 'AAPL', 'BAC', 'IBM', 'JPM']
# period="5y"
# interval="1d"
# yf.shared._PROGRESS_BAR = yf.utils.ProgressBar(len(tickers), 'completed')
# _price = {}
# _fin = {}
# _finQ = {}
# _other = {}
# error = {}
# for t in tickers:
#     _price[t], _fin[t], _finQ[t], _other[t], error[t] = \
#         _download_one.remote(t, period=period, interval=interval)
#     yf.shared._PROGRESS_BAR.animate()
#
# BATCH_SIZE = 4
# object_refs = list(_price.values())
# df = pd.DataFrame()
# while object_refs:
#     if BATCH_SIZE > len(object_refs):
#         BATCH_SIZE = len(object_refs)
#     # Process results in the finish order instead of the submission order.
#     ready_object_refs, object_refs = ray.wait(object_refs, num_returns=BATCH_SIZE)
#     _df = _pd.concat(ray.get(ready_object_refs), axis=0, ignore_index=True)
#     df = pd.concat([df, _df], axis=0, ignore_index=True)

