################################
# The code summarizes anomalies/risk factors prepared by open source asset pricing website.
################################
import os
import yfinance as yf #yf.__version__=='0.2.32'
import pandas as pd
import numpy as np

writer = pd.ExcelWriter(r'./01.Assessment/_rank_anomalies.xlsx')

SignalDoc = pd.read_csv(r'./01.Assessment/SignalDoc.csv')
cols = ['Acronym', 'Cat.Data', 'Cat.Economic', 'Sign', 'Portfolio Period', 'Start Month', 'Filter', 'LS Quantile']
SignalDoc = SignalDoc[cols]
SignalDoc.set_index('Acronym', inplace=True); SignalDoc.index.names=['signalname']
SignalDoc.columns = pd.MultiIndex.from_product([['Overview'], SignalDoc.columns])

benchmark = yf.download('spy', period='10y', interval='1mo').reset_index()
benchmark['ret_spy'] = benchmark['Close'].pct_change(1)*100
benchmark['Date'] = benchmark['Date'].dt.strftime('%Y-%m')

path = r"./01.Assessment/Portfolios"
implementations = os.listdir(path)
for x in implementations:
    print(fr"-----------------------{x}------------------------------")
    df = pd.read_csv(path + '/' + x)
    df['date'] = df['date'].str[:7]
    source = x[:-4][18:]
    if x in ['PredictorPortsFull.csv', 'PlaceboPortsFull.csv']:
        source = x[:-4]
    df1 = pd.DataFrame()
    for t in df['signalname'].unique():
        try:
            tmp = df[df['signalname'].eq(t)]
            which_port = tmp['port'].unique()[-2] # sign has been considered when creating portfolios.
            rows = tmp['port'].eq(which_port)
            df1 = pd.concat([df1, tmp[rows]], ignore_index=True)
        except:
            print(fr"{t} is not found in database {x} post 2015")

    df1 = df1[df1.date.ge('2015-01')].merge(
        benchmark, how='left', left_on='date', right_on='Date'
    )
    df1['ret_mkt'] = df1['ret'] - df1['ret_spy']

    df2 = df1.assign(
        timeframe='Total'
    ).groupby(['signalname',  'timeframe'], as_index=False).agg(
        Nlong=('Nlong', lambda x: int(np.mean(x))),
        ret=('ret', 'mean'),
        ret_SR=('ret', lambda x: np.mean(x) / np.std(x)),
        ret_mkt=('ret_mkt', 'mean'),
        ret_mkt_SR=('ret_mkt', lambda x: np.mean(x) / np.std(x)),
    )
    df2['rank ret'] = df2['ret'].rank(ascending=False, method='dense').astype(int)
    df2['rank ret_mkt'] = df2['ret_mkt'].rank(ascending=False, method='dense').astype(int)
    df2 = df2.pivot(index='signalname', columns='timeframe')\
        .reorder_levels(['timeframe', None], axis=1)

    df3 = df1.assign(
        timeframe=df1['date'].str[:4]
    ).groupby(['signalname', 'timeframe'], as_index=False).agg(
        Nlong=('Nlong', lambda x: int(np.mean(x))),
        ret=('ret', 'mean'),
        ret_SR=('ret', lambda x: np.mean(x) / np.std(x) if np.std(x)>0 else None),
        ret_mkt=('ret_mkt', 'mean'),
        ret_mkt_SR=('ret_mkt', lambda x: np.mean(x) / np.std(x)if np.std(x)>0 else None),
    )
    df3['rank ret'] = df3.groupby('timeframe')['ret'].rank(ascending=False, method='dense').astype(int)
    df3['rank ret_mkt'] = df3.groupby('timeframe')['ret_mkt'].rank(ascending=False, method='dense').astype(int)
    df3 = df3.pivot(index='signalname', columns='timeframe')

    output = SignalDoc\
        .merge(df2, how='right', left_index=True, right_index=True)\
        .merge(df3, how='left', left_index=True, right_index=True)

    output.to_excel(writer, sheet_name=source, merge_cells=True)

writer.close()
