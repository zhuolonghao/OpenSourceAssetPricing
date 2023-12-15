# https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/MomRev.do
# strategy: consider short-term momentum and mid-term reversal. (independent sorting)
def MomRev(base=base, keep_all=False):
    df = base.copy()
    df['mom6m'] = df.groupby('ticker')['close'].pct_change(6)
    df['mom36m'] = df.groupby('ticker')['close'].pct_change(24).shift(12)

    df = df[['ticker', 'date_ym', 'close', 'mom6m', 'mom36m']][
        df.date_ym.ge("202301")]

    df['mom6m_cut'] = df.groupby('date_ym', group_keys=False)['mom6m'].apply(lambda x: _bin(x, 5)).astype(str)
    df['mom36m_cut'] = df.groupby('date_ym', group_keys=False)['mom36m'].apply(lambda x: _bin(x, 5)).astype(str)
    df = df.assign(
        MomRev= df['mom6m_cut'].eq("5") & df['mom36m_cut'].eq("1")
    )
    if keep_all:
        return df
    else:
        return df[['ticker', 'date_ym', 'MomRev']]


#df = MomRev(base, 5)