# Factor driving Earnings:
#   Shareholders payout can decrease due to high CapEx that lowers cash flow available for distribution.
#   Earnings/revenue reflects accruals that 1) involves estimates and judgement calls, and 2) are not ALWAYS align with actual cash flow.
#       1) earning reliability/quality is subject to more questions if accruals are high.
#       2) earning stability is a concern and subject to substantial revision when accruals are high.

# Earnings strategy: buy stocks with low investments, suggesting more payout to shareholders.

# Investment: https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/Investment.do
#   context: substantial increase in CapEx indicates good investment opportunity in long-run, and market confidence in stocks.
#               it's often announced when share price is high (cheaper to fund by equity)
#   however: shareholders don't like such firms especially when management tend to have "empire building" intention.
#               such intention is more likely when they have higher cash flows and lower debt ratios.
#   conclusion: do not buy firms with high investments, especially when they have high cash and low leverage

# ChInv: https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/ChInv.do
#   context:  inventory changes is main driver behind negative relation between accruals and future abnormal returns
#   rationale:
#   conclusion: buy firms with low inventory change.

# DelDRC: https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/DelDRC.do
#   context:  Deferred revenue is part of accruals.
#   conclusion: buy firms with high deferred revenue.


# rows = 'DelDRC'
# SignalDoc[SignalDoc.Acronym.eq(rows)][cols].T
# print(SignalDoc[SignalDoc.Acronym.eq(rows)]["Detailed Definition"].values)
# finQ[[x for x in finQ.columns if 'deferred' in x.lower()]].count()

def investment(base, keep_all=False):
    var_base = ['ticker', 'date_ym', 'sector', 'exchange']
    var_Investment = [
        # numerator
        'capitalexpenditure',
        # denominator
        'totalrevenue']
    var_ChInv = [
        # numerator
        'inventory',
        # denominator
        'totalassets']
    var_DelDRC = ['currentdeferredrevenue', 'totalassets', 'commonstockequity', 'totalrevenue']

    vars = list(set(var_base+var_Investment+var_ChInv+var_DelDRC))
    rows = base.sector.ne('Financials')
    df = base[rows][vars].copy()

    df['CapEx_to_rev'] = df['capitalexpenditure'] / df['totalrevenue']
    df['CapEx_to_rev_adj'] = df['CapEx_to_rev'] / df.groupby('ticker')['CapEx_to_rev'].transform(lambda x: x.rolling(5, 5).mean())
    df.loc[df.totalrevenue.le(10e6), 'CapEx_to_rev_adj'] = None
    # missing values in these variables are not systematic
    vars_fill = ['currentdeferredrevenue']
    for v in vars_fill: df[v] = df[v].fillna(0)
    # calcualte annual change
    vars_chg = ['inventory', 'currentdeferredrevenue']
    for v in vars_chg: df[f"{v}_chg"] = df[v] - df.groupby('ticker')[v].shift(4)
    df['asset_avg'] = 0.5*(df['totalassets'] + df.groupby('ticker')['totalassets'].shift(4))

    df = df.groupby('ticker').tail(1)

    df['ChInv'] = -1 * df['inventory_chg'] / df['asset_avg']
    df['ChInv_q'] = df['ChInv'].transform(lambda x: _bin(x, 10)).astype(str)

    df['Investment'] = -1 * df['CapEx_to_rev_adj']
    df['Investment_q'] = df['Investment'].transform(lambda x: _bin(x, 5)).astype(str)

    df['DelDRC'] = df['currentdeferredrevenue_chg'] / df['asset_avg']
    df['DelDRC_q'] = df['DelDRC'].transform(lambda x: _bin(x, 10)).astype(str)
    rows = df['commonstockequity'].lt(0) | (df['currentdeferredrevenue'].eq(0) & df['currentdeferredrevenue_chg'].eq(0)) | df['totalrevenue'].le(5e6)
    df.loc[rows, 'DelDRC_q'] = None

    print('Complete: investment')

    if keep_all:
        return df
    else:
        return df[['ticker', 'ChInv_q', 'Investment_q', 'DelDRC_q']]

# df = investment(finQ)



