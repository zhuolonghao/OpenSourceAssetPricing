# https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/MomRev.do
# strategy: consider short-term momentum and mid-term reversal. (independent sorting)

# rows = 'STreversal'
# SignalDoc[SignalDoc.Acronym.eq(rows)][cols].T
# print(SignalDoc[SignalDoc.Acronym.eq(rows)]["Detailed Definition"].values)

def Reversals(base=base, others=others, keep_all=False):

    df = base.merge(others[['ticker', 'sharesoutstanding']], how='left', left_on='ticker', right_on='ticker').copy()
    df['turnover'] = df['volume'] / df['sharesoutstanding']
    df['ret'] = df.groupby('ticker')['close'].pct_change()
    df['ret_6m'] = df.groupby('ticker')['close'].pct_change(6)
    df['ret_6m_gap12'] = df.groupby('ticker')['ret_6m'].shift(13)
    df['ret_24m'] = df.groupby('ticker')['close'].pct_change(24)
    df['ret_24m_gap12'] = df.groupby('ticker')['ret_24m'].shift(13)

    df = df[['ticker', 'date_ym', 'close', 'ret', 'ret_6m_gap12', 'ret_24m_gap12', 'turnover']][
        df.date_ym.ge("202301")]

    df['st'] = -1 * df['ret']
    df['STreversal'] = df.groupby('date_ym', group_keys=False)['st'].apply(
        lambda x: _bin(x, 10)).astype(str)

    # https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3150525
    # high-turnover stocks exhibited short-term momentum (Mom1m)
    # short-term reversal is mainly due to thinly traded stocks.
    df['Mom1m'] = df.groupby('date_ym', group_keys=False)['ret'].apply(
        lambda x: _bin(x, 10)).astype(str)
    df['turnover_cut'] = df.groupby('date_ym', group_keys=False)['turnover'].apply(
        lambda x: _bin(x, 10)).astype(str)
    df['MomTurnover'] = 0
    df.loc[df.Mom1m.eq('10.0') & df.turnover_cut.eq('10.0'), 'MomTurnover'] = 1

    df['mr'] = -1 * df['ret_6m_gap12']
    df['MRreversal'] = df.groupby('date_ym', group_keys=False)['mr'].apply(
        lambda x: _bin(x, 10)).astype(str)

    df['lr'] = -1 * df['ret_24m_gap12']
    df['LRreversal'] = df.groupby('date_ym', group_keys=False)['lr'].apply(
        lambda x: _bin(x, 10)).astype(str)

    if keep_all:
        return df
    else:
        return df[['ticker', 'date_ym', 'STreversal',  'MRreversal', 'LRreversal', 'MomTurnover']]

# df = Reverasls(base)