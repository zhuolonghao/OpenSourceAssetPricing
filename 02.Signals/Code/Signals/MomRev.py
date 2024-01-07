# https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/MomRev.do
# strategy: consider short-term momentum and mid-term reversal. (independent sorting)
def MomRev(base=base, keep_all=False):
    df = base.copy()
    df['ret_6m'] = df.groupby('ticker')['close'].pct_change(6)
    df['ret_24m'] = df.groupby('ticker')['close'].pct_change(24)
    df['ret_24m_gap12m'] = df.groupby('ticker')['ret_24m'].shift(13)

    df = df[['ticker', 'date_ym', 'close', 'ret_6m', 'ret_24m', 'ret_24m_gap12m']][
        df.date_ym.ge("202301")]

    df['ret_6m_cut'] = df.groupby('date_ym', group_keys=False)['ret_6m'].apply(lambda x: _bin(x, 5)).astype(str)
    df['ret_24m_gap12m_cut'] = df.groupby('date_ym', group_keys=False)['ret_24m_gap12m'].apply(lambda x: _bin(x, 5)).astype(str)
    df = df.assign(
        MomRev= df['ret_6m_cut'].eq("10.0") & df['ret_24m_gap12m_cut'].eq("1.0")
    )
    if keep_all:
        return df
    else:
        return df[['ticker', 'date_ym', 'MomRev']]


#df = MomRev(base, 5)