# https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/IntMom.do
# strategy: A = stocks that have the best ret_6m but so-so ret_6m_gap6m
#           B = stocks that have the so-so ret_6m but the best ret_6m_gap6m
#           B > A, in other words, the past performance over the period from 12 to 7 months prior is important.
# this article looks at MomInt's relation with Mom12mOffSeason
# https://citeseerx.ist.psu.edu/document?repid=rep1&type=pdf&doi=a287c74ec1d378770d1ae787aa04d1cacefc3473

def MomInt(base=base, keep_all=False):
    df = base.copy()
    df['ret_6m'] = df.groupby('ticker')['close'].pct_change(6)
    df['ret_6m_gap6m'] = df.groupby('ticker')['ret_6m'].shift(7)

    df = df[['ticker', 'date_ym', 'close', 'ret_6m', 'ret_6m_gap6m']][
        df.date_ym.ge("202301")]

    df['MomInt'] = df.groupby('date_ym', group_keys=False)['ret_6m_gap6m'].apply(lambda x: _bin(x, 10)).astype(str)
    if keep_all:
        return df
    else:
        return df[['ticker', 'date_ym', 'MomInt']]

# df = MomInt()