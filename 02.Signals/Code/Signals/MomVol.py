# https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/MomRev.do
# strategy: consider short-term momentum and high-volatility. (independent sorting)
def MomVol(base=base, keep_all=False):
    df = base.copy()
    df['mom6m'] = df.groupby('ticker')['close'].pct_change(6)
    df['vol'] = df.groupby('ticker')['volume'].transform(lambda x: x.rolling(6, 5).mean())

    df = df[['ticker', 'date_ym', 'close', 'mom6m', 'vol']][
        df.date_ym.ge("202301")]

    df['mom6m_cut'] = df.groupby('date_ym', group_keys=False)['mom6m'].apply(lambda x: _bin(x, 10)).astype(str)
    df['vol_cut'] = df.groupby('date_ym', group_keys=False)['vol'].apply(lambda x: _bin(x, 3)).astype(str)
    df['MomVol'] = df['mom6m_cut']
    df.loc[df['vol_cut'].ne("3"), 'MomVol'] = None
    if keep_all:
        return df
    else:
        return df[['ticker', 'date_ym', 'MomVol']]


#df = MomVol()