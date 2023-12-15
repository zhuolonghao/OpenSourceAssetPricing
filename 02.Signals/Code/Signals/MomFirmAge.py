# https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/FirmAgeMom.do
# strategy: consider short-term(6M) momentum for newly listed companies. (dependent sorting)
def MomFirmAge(base=base,keep_all=False):
    df = base.copy()
    df['mom6m'] = df.groupby('ticker')['close'].pct_change(6)
    df['age'] = df.groupby('ticker')['close'].cumcount()

    df = df[['ticker', 'date_ym', 'close', 'mom6m', 'age']][
        df.date_ym.ge("202301")]

    df.loc[(df['age'].lt(12)|df['close'].lt(5)), 'mom6m'] = None
    df['age_cut'] = df.groupby('date_ym', group_keys=False)['age'].apply(lambda x: _bin(x, 5)).astype(str)
    df.loc[df['age_cut'].eq("1"), 'mom6m'] = None
    df['MomFirmAge'] = df.groupby('date_ym', group_keys=False)['mom6m'].apply(lambda x: _bin(x, 5)).astype(str)
    if keep_all:
        return df
    else:
        return df[['ticker', 'date_ym', 'MomFirmAge']]

#df = MomFirmAge(base, 5)