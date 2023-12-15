# this script is to download info for a bunch of tickers.
# the info includes price, financials, balance sheet, cash flow and other basic info
# the script leverages the python package "yfinance" and builds upon the multi.py
import yfinance as yf
import ray as ray
import pandas as _pd

# taks 28 sec
def download(tickers, data_type="price"):
    """Download yahoo tickers
    :Parameters:
        tickers : str, list
            List of tickers to download
        data_type: str
            select which type of data source to retrieve
            valid values: price, fin, finQ, others
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


    _dfs = [_download_price.remote(t) for t in tickers]
    price = _pd.concat(ray.get(_dfs), axis=0, ignore_index=True)

    _dfs = [_download_fin.remote(t) for t in tickers]
    fin = _pd.concat(ray.get(_dfs), axis=0, ignore_index=True)

    _dfs = [_download_finQ.remote(t) for t in tickers]
    finQ = _pd.concat(ray.get(_dfs), axis=0, ignore_index=True)

    _dfs = [_download_others.remote(t) for t in tickers]
    others = _pd.concat(ray.get(_dfs), axis=0, ignore_index=True)

    return price, fin, finQ, others

@ray.remote
def _download_price(ticker):
    df = None
    object = yf.Ticker(ticker)
    try:
        df = object.history(period="5y", interval="1d").\
            rename_axis('date_raw').reset_index().\
            assign(Ticker=ticker)
    except Exception as e:
        print(f'\n Error in downloading {ticker}')
    return df

@ray.remote
def _download_fin(ticker):
    df = None
    object = yf.Ticker(ticker)
    try:
        df = _pd.concat(
            [object.financials.T, object.balance_sheet.T, object.cash_flow.T], axis=1).\
            rename_axis('date_raw').reset_index().\
            assign(Ticker=ticker)
    except Exception as e:
        print(f'\n Error in downloading {ticker}')
    return df

@ray.remote
def _download_finQ(ticker):
    df = None
    object = yf.Ticker(ticker)
    try:
        df = _pd.concat(
            [object.get_financials(freq='quarterly').T, object.get_balancesheet(freq='quarterly').T, object.get_cashflow(freq='quarterly').T], axis=1).\
            rename_axis('date_raw').reset_index().\
            assign(Ticker=ticker)
    except Exception as e:
        print(f'\n Error in downloading {ticker}')
    return df

@ray.remote
def _download_others(ticker):
    df = None
    object = yf.Ticker(ticker)
    try:
        major_holders = dict(zip(object.major_holders[1], object.major_holders[0]))
        info = object.info
        del info['companyOfficers']
        df = _pd.DataFrame.from_dict([
            {**{'ticker': ticker},
              **object.info,
              **object.fast_info,
              **major_holders}])
    except Exception as e:
        print(f'\n Error in downloading {ticker}')
    return df



import timeit
start_time = timeit.default_timer()
tickers=['MSFT', 'WFC'] * 50
price, fin, finQ, others = download(tickers)
print(f'Ray completed in: {timeit.default_timer() - start_time} seconds')
