# https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/MomRev.do
# strategy: consider short-term momentum and mid-term reversal. (independent sorting)
def MomInt(base=base, keep_all=False):
    df = base.copy()
    df['mom7_12m'] = df.groupby('ticker')['close'].pct_change(6).shift(6)

    df = df[['ticker', 'date_ym', 'close', 'mom7_12m']][
        df.date_ym.ge("202301")]

    df['MomInt'] = df.groupby('date_ym', group_keys=False)['mom7_12m'].apply(lambda x: _bin(x, 10)).astype(str)
    if keep_all:
        return df
    else:
        return df[['ticker', 'date_ym', 'MomInt']]


df = MomInt()