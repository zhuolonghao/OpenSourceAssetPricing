# https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/Mom12mOffSeason.do
# strategy: buy average return of months in the preceding year, excluding the most recent and the same month.
# it's different from other momentum strategies (MomXXX) where the cumulative return is considered instead.
def Mom12mOffSeason(base=base, keep_all=False):
    df = base.copy()
    df['ret_1m'] = df.groupby('ticker')['close'].pct_change(1)
    df['ret_1m_gap1'] = df.groupby('ticker')['ret_1m'].shift(1)
    df['ret_m02_m11'] = df.groupby('ticker')['ret_1m_gap1'].transform(lambda x: x.rolling(10, 6).mean())
    df['ret_1m_gap1_pos'] = df.groupby('ticker')['ret_1m_gap1'].transform(lambda x: x >= 0)
    df['ret_m02_m11_pos'] = df.groupby('ticker')['ret_1m_gap1_pos'].transform(lambda x: x.rolling(10, 10).sum())

    df = df[['ticker', 'exchange', 'date_ym', 'close', 'ret_1m', 'ret_1m_gap1', 'ret_m02_m11', 'ret_1m_gap1_pos', 'ret_m02_m11_pos']][
        df.date_ym.ge("202301")]

    # Filter is applied before cut-and-sort
    row = df['exchange'].isin(['NYSE', 'AMEX'])
    df = df[row]

    df['Mom_m02_m11'] = df.groupby('date_ym', group_keys=False)['ret_m02_m11'].apply(lambda x: _bin(x, 10)).astype(str)
    df['Mom_m02_m11_pos'] = df['ret_m02_m11_pos'] >= 8

    if keep_all:
        return df
    else:
        print('Completed: Mom12mOffSeason')
        return df[['ticker', 'date_ym', 'Mom_m02_m11', 'Mom_m02_m11_pos']]

# df = Mom12mOffSeason()