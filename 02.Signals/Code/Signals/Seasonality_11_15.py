# https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/MomRev.do
# strategy: buy seasonalities in returns,
# Say current month is Dec-2023, one should skip the preceding 1 year sample (from Dec-2022 to Nov-2023.)
#   buy winners in Decembers prior to 2022 (that is Dec-2021, Dec-2020, Dec-2019, .....)
#   do not buy winners in other months in preceding years (that is winner in Jan-Nov 2021, 2020, 2019, ....)
def Seasonality_11_15(base=base, keep_all=False):
    df = base.copy()
    df['ret_shift'] = df.groupby('ticker')['close'].pct_change(1).shift(121)
    df['ret_total'] = df.groupby('ticker')['ret_shift'].transform(lambda x: x.rolling(60, 1).sum())
    df['ret_cnt'] = df.groupby('ticker')['ret_shift'].transform(lambda x: x.rolling(60, 1).count())
    df['ret_shift2'] = df.groupby('ticker')['close'].pct_change(1).shift(132)
    df['ret_shift3'] = df.groupby('ticker')['close'].pct_change(1).shift(144)
    df['ret_shift4'] = df.groupby('ticker')['close'].pct_change(1).shift(156)
    df['ret_shift5'] = df.groupby('ticker')['close'].pct_change(1).shift(168)
    df['ret_shift6'] = df.groupby('ticker')['close'].pct_change(1).shift(180)
    df['ret_total_s'] = df[['ret_shift2', 'ret_shift3', 'ret_shift4', 'ret_shift5', 'ret_shift6']].sum(axis=1)
    df['ret_cnt_s'] = df[['ret_shift2', 'ret_shift3', 'ret_shift4', 'ret_shift5', 'ret_shift6']].count(axis=1)

    df['on_season'] = df['ret_total_s'] / df['ret_cnt_s']
    df['off_season'] = -1 * (df['ret_shift'] - df['ret_total_s'] ) / (df['ret_cnt'] - df['ret_cnt_s'])

    df = df[['ticker', 'date_ym', 'close', 'on_season', 'off_season']][
        df.date_ym.ge("202301")]

    df['Season_1115'] = df.groupby('date_ym', group_keys=False)['on_season'].apply(lambda x: _bin(x, 10)).astype(str)
    df['OffSeason_1115'] = df.groupby('date_ym', group_keys=False)['off_season'].apply(lambda x: _bin(x, 10)).astype(str)

    if keep_all:
        return df
    else:
        return df[['ticker', 'date_ym', 'Season_1115', 'OffSeason_1115']]

# df = Seasonality_11_15()