import os
import re
import pandas as pd


total = pd.read_excel("./_data/total_stock_market_holdings.xlsx", sheet_name='reformatted')
total.columns = [x.lower() for x in total.columns]
total['sector'] = total['sector'].transform(lambda x: x[:36])
total['Symbol'] = total['ticker'].transform(lambda x: str(x).replace(".", ""))

gics = pd.read_excel("./_data/GICS_Map_2023.xlsx", sheet_name='reformatted')
gics.columns = [x.lower() for x in gics.columns]
gics = gics.fillna(method='ffill')
gics = gics.applymap(lambda x: re.sub("[\(\[].*?[\)\]]", "", x).strip()[:36])

_dfs = {}
for f in os.listdir("./_data/style/"):
    _dfs[f] = pd.read_excel(fr"./_data/style/{f}", sheet_name='reformatted').assign(ETF=f[:-5])
style = pd.concat(_dfs.values(), ignore_index=True)\
    .groupby(['Ticker', 'ETF'])[['% of fund*', 'Shares']].sum().reset_index()\
    .pivot(index='Ticker', columns='ETF', values='% of fund*')

exchange = pd.read_csv("02.Signals/Data/equityshortinterest_20231001_20231130.csv", low_memory=False)
exchange = exchange.groupby('Symbol').head(1)
exchange['exchange'] = exchange['Market']
exchange = exchange[["Symbol", "exchange"]].drop_duplicates()

total_gics_style = total.rename(columns={'sector': 'sub-industry'}).merge(
    exchange, how='left', left_on='Symbol', right_on='Symbol').merge(
    gics, how='left', left_on='sub-industry', right_on='sub-industry').merge(
    style, how='left', left_on='ticker', right_on='Ticker')


total_gics_style['ticker'] = total_gics_style['ticker'].apply(lambda x: str(x).replace(".", "-"))
cols = ['ticker', 'holdings name', 'exchange', 'market', '% of fund',
        'sector', 'industry group', 'industry',  'sub-industry',
        'Mega_K', 'Mega_V', 'LG_K', 'LG_V',  'Mid_K', 'Mid_V', 'Small_K', 'Small_V']
total_gics_style[cols].to_excel("./_data/_total_gics_style.xlsx", index=False)
