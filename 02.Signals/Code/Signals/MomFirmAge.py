# https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/FirmAgeMom.do
# strategy: consider short-term(6M) momentum for newly listed companies. (dependent sorting)
def MomFirmAge(base=base, keep_all=False):
    df = base.copy()
    df['ret_6m'] = df.groupby('ticker')['close'].pct_change(6)
    df['age'] = df.groupby('ticker')['close'].cumcount()

    df = df[['ticker', 'date_ym', 'close', 'ret_6m', 'age']][
        df.date_ym.ge("202301")]

    df['ret_6m_screened'] = df['ret_6m']
    df.loc[(df['age'].lt(12)|df['close'].lt(5)), 'ret_6m_screened'] = None
    df['age_cut'] = df.groupby('date_ym', group_keys=False)['age'].apply(lambda x: _bin(x, 5)).astype(str)
    df.loc[df['age_cut'].ne("1.0"), 'ret_6m_screened'] = None
    df['MomFirmAge'] = df.groupby('date_ym', group_keys=False)['ret_6m_screened'].apply(lambda x: _bin(x, 5)).astype(str)
    if keep_all:
        return df
    else:
        return df[['ticker', 'date_ym', 'MomFirmAge']]

#df = MomFirmAge(base, 5)