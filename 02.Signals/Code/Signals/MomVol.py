# https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/MomVol.do
# strategy: consider short-term momentum and high-volatility. (independent sorting)
def MomVol(base=base, keep_all=False):
    df = base.copy()
    df['ret_6m'] = df.groupby('ticker')['close'].pct_change(6)
    df['vol_6ma'] = df.groupby('ticker')['volume'].transform(lambda x: x.rolling(6, 5).mean())

    df = df[['ticker', 'exchange', 'date_ym', 'close', 'ret_6m', 'vol_6ma']][
        df.date_ym.ge("202301")]

    # Filter is applied before cut-and-sort
    row = df['close'].gt(1) & df['exchange'].isin(['NYSE', 'AMEX'])
    df = df[row]

    df['mom6m_cut'] = df.groupby('date_ym', group_keys=False)['ret_6m'].apply(lambda x: _bin(x, 10)).astype(str)
    df['vol_cut'] = df.groupby('date_ym', group_keys=False)['vol_6ma'].apply(lambda x: _bin(x, 3)).astype(str)
    df['MomVol'] = df['mom6m_cut']
    df.loc[df['vol_cut'].ne("10.0"), 'MomVol'] = None
    if keep_all:
        return df
    else:
        return df[['ticker', 'date_ym', 'MomVol']]


#df = MomVol()