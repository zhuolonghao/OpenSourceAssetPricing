# Let's download key information of companies in investable universe

import pandas as pd

exec(open('_utility/download_tickers_from_yfinance3.py').read())

ref = pd.read_excel("./_data/_total_gics_style.xlsx")
tickers = [str(x).replace(".", "-") for x in ref['ticker']]

df = download(tickers=tickers, data_type="price", period="max", interval="1mo")
df.to_parquet(r'./02.Signals/Data/price_monthly.parquet', compression='zstd', index=False)
print('Completed: Monthly price')

df = download(tickers=tickers, data_type="price")
df.to_parquet(r'./02.Signals/Data/price.parquet', compression='zstd', index=False)
print('Completed: Daily price')

df = download(tickers=tickers, data_type="fin")
df.to_parquet(r'./02.Signals/Data/fin.parquet', compression='zstd', index=False)
print('Completed: Financials, Annual')

df = download(tickers=tickers, data_type="finQ")
df.to_parquet(r'./02.Signals/Data/finQ.parquet', compression='zstd', index=False)
print('Completed: Financials, Quarterly')

_dfs = [_download_others.remote(t) for t in tickers]
data = _pd.concat(ray.get(_dfs), axis=0, ignore_index=True)
data.replace('Infinity', None) \
    .to_parquet(r'./02.Signals/Data/others.parquet', compression='zstd', index=False)
print('Completed: other information')

S2SS = ['VOO',
        'MGK', 'MGV', 'VUG', 'VTV', 'VOT', 'VOE', 'VBK', 'VBR',
        'FSTA', 'FDIS', 'FIDU', 'XLE', 'XLC', 'XLF', 'XLV', 'XLK', 'XLB', 'XLRE', 'XLU']
data = yf.download(S2SS, period='max', interval='1mo', group_by="ticker")
cols = data.columns.get_level_values(1) == 'Adj Close'
data_close = data.iloc[:, cols]
data_close.columns = data_close.columns.get_level_values(0)
data_close = data_close.rename_axis('date_ym').reset_index()
data_close['date_ym'] = data_close['date_ym'].dt.strftime("%Y%m")
data_close = pd.melt(data_close, id_vars='date_ym')
data_close.columns = ['date_ym', 'ticker', 'close']
data_close.to_parquet(r'./02.Signals/Data/etf_price_monthly.parquet', compression='zstd', index=False)
