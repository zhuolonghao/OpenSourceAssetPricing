# Let's download key information of companies in investable universe

import pandas as pd
exec(open('_utility/download_tickers_from_yfinance3.py').read())

ref = pd.read_excel("./_data/total_stock_market_holdings.xlsx", sheet_name='reformatted')
tickers = [str(x).replace(".", "-") for x in ref['Ticker']]

df = download(tickers=tickers, data_type="price", period="max", interval="1mo")
df.to_parquet(r'./02.Signals/Data/price_monthly.parquet', compression='zstd', index=False)

df = download(tickers=tickers, data_type="price")
df.to_parquet(r'./02.Signals/Data/price.parquet', compression='zstd', index=False)

df = download(tickers=tickers, data_type="fin")
df.to_parquet(r'./02.Signals/Data/fin.parquet', compression='zstd', index=False)

df = download(tickers=tickers, data_type="finQ")
df.to_parquet(r'./02.Signals/Data/finQ.parquet', compression='zstd', index=False)

_dfs = [_download_others.remote(t) for t in tickers]
data = _pd.concat(ray.get(_dfs), axis=0, ignore_index=True)
data.replace('Infinity', None)\
    .to_parquet(r'./02.Signals/Data/others.parquet', compression='zstd', index=False)

#exec(open('_utility/download_tickers_from_yfinance2.py').read())
#price, fin, finQ, other, error = download(tickers=tickers[0:3])
# price.to_parquet(r'./02.Signals/Data/price.parquet', compression='zstd', index=False)
# fin.to_parquet(r'./02.Signals/Data/fin.parquet', compression='zstd', index=False)
# finQ.to_parquet(r'./02.Signals/Data/finQ.parquet', compression='zstd', index=False)
# other.to_parquet(r'./02.Signals/Data/other.parquet', compression='zstd', index=False)

# exec(open('_utility/download_tickers_from_yfinance.py').read())
# df, _tickers = download(tickers=tickers)
# df.to_parquet(r'./02.Signals/Data/data_yahoo.parquet', compression='zstd', index=False)
# tickers = [key for key, value in _tickers.items() if value=='fail']
