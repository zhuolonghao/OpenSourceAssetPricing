import os, re, time, shutil
import webbrowser as web
import pandas as pd
import numpy as np
import os
link = 'https://institutional.vanguard.com/investments/product-details/fund/0968'
web.open(link)

date_ymd = '24.5.29'
path_downloads = r"C:\Users\longh\Downloads"
downloads = [x for x in os.listdir(path_downloads) if 'ProductDetailsHoldings' in x]
raw = pd.read_csv(f"{path_downloads}\{downloads[0]}", skiprows=4, usecols=list(range(1, 10)), header=None)
last_row = np.where(raw.iloc[:, 0].isna())[0][0]
df = pd.DataFrame(raw.values[1:last_row], columns=raw.iloc[0]).drop_duplicates()
df.columns = [x.lower() for x in df.columns]

exec(open('_utility/download_tickers_from_yfinance3.py').read())
tickers = ['SPY'] + [str(x).replace(".", "-").replace("/", "-") for x in df['ticker']]
price = download(tickers=tickers, data_type="price", period="5y", interval="1d")
price.columns = [x.lower() for x in price.columns]
price.sort_values(by=['ticker', 'date_raw'], inplace=True)
price['ret'] = price.groupby(['ticker'])['close'].pct_change()
price2 = price.pivot(index='date_raw', columns='ticker', values='ret').iloc[1:,]

c20 = price2.rolling(20).corr(pairwise=True).xs('SPY', level=1, drop_level=True).drop('SPY', axis=1).mean(axis=1)
c50 = price2.rolling(50).corr(pairwise=True).xs('SPY', level=1, drop_level=True).drop('SPY', axis=1).mean(axis=1)
c65 = price2.rolling(65).corr(pairwise=True).xs('SPY', level=1, drop_level=True).drop('SPY', axis=1).mean(axis=1)

output = pd.concat([
    c20.to_frame().assign(variable='c20'),
    c50.to_frame().assign(variable='c50'),
    c65.to_frame().assign(variable='c65')], axis=0)
output.columns = ['value', 'variable']
output['MA5_max'] = output.groupby('variable')['value'].rolling(5).max().reset_index(0,drop=True)
output['MA5_min'] = output.groupby('variable')['value'].rolling(5).min().reset_index(0,drop=True)
output['MA5_avg'] = output.groupby('variable')['value'].rolling(5).mean().reset_index(0,drop=True)


trend = price[price['ticker'].eq('SPY')].set_index('date_raw')['close']

volume = price.pivot(index='date_raw', columns='ticker', values='volume').drop('SPY', axis=1)
volume['volume'] = volume.sum(axis=1)

output = output.join(trend).join(volume['volume']).reset_index()
output['date'] = output['date_raw'].dt.strftime('%Y-%m-%d')
output.drop('date_raw', axis=1).to_excel(f'sp500_component_{date_ymd}.xlsx', index=False)


output = pd.concat([trend,  c20, c50, c65, c100, volume], axis=1)
output.reset_index(inplace=True)
output['date'] = output['date_raw'].dt.strftime('%Y-%m-%d')
output.drop('date_raw', axis=1).to_excel(f'sp500_component_{date_ymd}.xlsx', index=False)