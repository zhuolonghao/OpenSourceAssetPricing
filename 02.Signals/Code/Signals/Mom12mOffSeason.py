# https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/MomRev.do
# strategy: buy average return of months in the preceding year, excluding the most recent and the same month.
# it's different from other momentum strategies (MomXXX) where the cumulative return is considered instead.
def Mom12mOffSeason(base=base, keep_all=False):
    df = base.copy()
    df['ret_shift'] = df.groupby('ticker')['close'].pct_change(1).shift(13)
    df['ret_m02_m11'] = df.groupby('ticker')['ret_shift'].transform(lambda x: x.rolling(10, 6).mean())

    df = df[['ticker', 'date_ym', 'close', 'ret_m02_m11']][
        df.date_ym.ge("202301")]

    df['Mom12mOffSeason'] = df.groupby('date_ym', group_keys=False)['ret_m02_m11'].apply(lambda x: _bin(x, 10)).astype(str)

    if keep_all:
        return df
    else:
        return df[['ticker', 'date_ym', 'Mom12mOffSeason']]

df = Mom12mOffSeason()