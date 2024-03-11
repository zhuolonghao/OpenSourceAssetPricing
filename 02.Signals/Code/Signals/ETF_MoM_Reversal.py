# https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/MomRev.do
# strategy: consider short-term momentum and mid-term reversal. (independent sorting)
def Mom_Rev(base=base):
    df = base.copy()
    df['ret_1m'] = df.groupby('ticker')['close'].pct_change(1)
    df['ret_6m'] = df.groupby('ticker')['close'].pct_change(6)
    df['ret_24m'] = df.groupby('ticker')['close'].pct_change(24)
    df['ret_24m_gap12m'] = df.groupby('ticker')['ret_24m'].shift(13)
    df['ret_6m_gap6m'] = df.groupby('ticker')['ret_6m'].shift(7)
    df['ret_1m_gap1'] = df.groupby('ticker')['ret_1m'].shift(1)
    df['ret_m02_m11'] = df.groupby('ticker')['ret_1m_gap1'].transform(lambda x: x.rolling(10, 6).mean())
    df['ret_1m_gap1_pos'] = df.groupby('ticker')['ret_1m_gap1'].transform(lambda x: x >= 0)
    df['ret_m02_m11_pos'] = df.groupby('ticker')['ret_1m_gap1_pos'].transform(lambda x: x.rolling(10, 10).sum())
    df['ret_6m_gap12m'] = df.groupby('ticker')['ret_6m'].shift(13)

    df = df[df.date_ym.ge("202301")]

    # Traditional 6M
    df['Mom'] = df.groupby('date_ym', group_keys=False)['ret_6m'].apply(lambda x: _bin(x, 5)).astype(str)
    # MomRev
    df['ret_6m_cut2'] = df.groupby('date_ym', group_keys=False)['ret_6m'].apply(lambda x: _bin(x, 3)).astype(str)
    df['ret_24m_gap12m_cut2'] = df.groupby('date_ym', group_keys=False)['ret_24m_gap12m'].apply(lambda x: _bin(x, 3)).astype(str)
    df['MomRev'] = df['ret_6m_cut2'].eq("3.0") & df['ret_24m_gap12m_cut2'].eq("1.0")
    # MomInt
    df['MomInt'] = df.groupby('date_ym', group_keys=False)['ret_6m_gap6m'].apply(lambda x: _bin(x, 5)).astype(str)
    # Mom12mOffSeason
    df['Mom_m02_m11_ret'] = df.groupby('date_ym', group_keys=False)['ret_m02_m11'].apply(lambda x: _bin(x, 5)).astype(str)
    df['Mom_m02_m11_sign'] = df['ret_m02_m11_pos'] >= 8
    # reversal, ST, MR, LR
    df['st'] = -1 * df['ret_1m']
    df['STreversal'] = df.groupby('date_ym', group_keys=False)['st'].apply(lambda x: _bin(x, 5)).astype(str)
    df['mr'] = -1 * df['ret_6m_gap12m']
    df['MRreversal'] = df.groupby('date_ym', group_keys=False)['mr'].apply(lambda x: _bin(x, 5)).astype(str)
    df['lr'] = -1 * df['ret_24m_gap12m']
    df['LRreversal'] = df.groupby('date_ym', group_keys=False)['lr'].apply(lambda x: _bin(x, 5)).astype(str)

    cols = ['ticker', 'date_ym', 'close', 'ret_1m', 'ret_6m', 'ret_m02_m11',
            'Mom', 'MomInt', 'MomRev', 'Mom_m02_m11_ret', 'Mom_m02_m11_sign', 'STreversal',  'MRreversal', 'LRreversal']
    return df[cols]
