# https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/Mom12mOffSeason.do
# strategy: buy average return of months in the preceding year, excluding the most recent and the same month.
# it's different from other momentum strategies (MomXXX) where the cumulative return is considered instead.
def MomCurve(base=base, keep_all=False):
    df = base.copy()
    df['ret_1m'] = df.groupby('ticker')['close'].pct_change(1)
    df['ret_3m'] = df.groupby('ticker')['close'].pct_change(3)
    df['ret_6m'] = df.groupby('ticker')['close'].pct_change(6)
    df['ret_6m_gap6m'] = df.groupby('ticker')['close'].pct_change(6).shift(7)
    df['ret_12m'] = df.groupby('ticker')['close'].pct_change(12)

    df['ret_1m_gap1'] = df.groupby('ticker')['ret_1m'].shift(1)
    df['ret_m02_m11'] = df.groupby('ticker')['ret_1m_gap1'].transform(lambda x: x.rolling(10, 6).mean())

    df = df[['ticker', 'exchange', 'date_ym', 'close',
             'ret_1m',  'ret_3m', 'ret_6m', 'ret_12m', 'ret_m02_m11', 'ret_6m_gap6m']][
        df.date_ym.ge("202301")]

    df['Mom_1m'] = df.groupby('date_ym', group_keys=False)['ret_1m'].apply(lambda x: _bin(x, 10)).astype(str)
    df['Mom_3m'] = df.groupby('date_ym', group_keys=False)['ret_3m'].apply(lambda x: _bin(x, 10)).astype(str)
    df['Mom_6m'] = df.groupby('date_ym', group_keys=False)['ret_6m'].apply(lambda x: _bin(x, 10)).astype(str)
    df['Mom_12m'] = df.groupby('date_ym', group_keys=False)['ret_12m'].apply(lambda x: _bin(x, 10)).astype(str)
    df['Mom_m02m11'] = df.groupby('date_ym', group_keys=False)['ret_m02_m11'].apply(lambda x: _bin(x, 10)).astype(str)

    if keep_all:
        return df
    else:
        print('Completed: Mom Curve')
        return df[['ticker', 'date_ym', 'Mom_1m', 'Mom_3m', 'Mom_6m', 'Mom_12m', 'Mom_m02m11']]

# df = Mom12mOffSeason()