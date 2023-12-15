# strategy: buy stocks with high profit-to-asset.
# rational: profitable firms are less prone to distress, have longer cash flow durations, and have lower levels of operating leverage.

# profit is calculated in different kinds:
# gross profit,                 https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Placebos/GPlag_q.do
# operating profit,             https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Placebos/OperProfRDLagAT_q.do
    # it captures the performance of the firm’s operations and is not affected by non-operating items, such as leverage and taxes.
# cash-based operating profit.  https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Placebos/CBOperProfLagAT_q.do
    # cash-based operating profit takes out the Accruals (i.e., non-cash component of earnings)
    # data quality should be an issue. create another version for benchmark.

# [x for x in finQ.columns if 'revenue' in x.lower()]
def profitability(base, keep_all=False):
    vars = ['ticker', 'date_ym', 'sector', 'exchange',
            'grossprofit', 'sellinggeneralandadministration', 'researchanddevelopment',
            'accountsreceivable', 'receivables', 'inventory', 'currentdeferredrevenue', 'noncurrentdeferredrevenue', 'accountspayable', 'currentaccruedexpenses', 'noncurrentaccruedexpenses',
            'totalassets', 'operatingcashflow', 'netincome']
    rows = base.sector.ne('Financials')
    df = base[rows][vars].copy()

    vars_fill = ['researchanddevelopment', 'accountsreceivable', 'inventory', 'currentdeferredrevenue', 'noncurrentdeferredrevenue', 'accountspayable', 'currentaccruedexpenses', 'noncurrentaccruedexpenses']
    for v in vars_fill:
        # some firm has missing values except for the most recent quarter.
        # the sequence of calculations below is used to control for such effect.
        df[f"{v}_chg"] = df[v] - df.groupby('ticker')[v].shift(4)
        df[v] = df[v].fillna(0)
        df[f"{v}_chg"] = df[f"{v}_chg"].fillna(0)

    df['asset_lag'] = df.groupby('ticker')['totalassets'].shift(1)
    df['OperProf'] = df['grossprofit'] - df['sellinggeneralandadministration'] + df['researchanddevelopment']
    df['CashOperProf'] = df['OperProf'] - df['accountsreceivable_chg'] - df['inventory_chg'] +\
        df['currentdeferredrevenue_chg'] + df['noncurrentdeferredrevenue_chg'] + \
        df['accountspayable_chg'] + df['currentaccruedexpenses_chg'] + df['noncurrentaccruedexpenses_chg']
    df['CashOperProf_alt'] = df['OperProf'] - (df['netincome'] - df['operatingcashflow'])

    df = df.groupby('ticker').tail(1)
    df['profit_to_asset'] = df['grossprofit'] / df['asset_lag']
    df['GPlag_q'] = df['profit_to_asset'].transform(lambda x: _bin(x, 10)).astype(str)

    df['OperProf_to_asset'] = df['OperProf'] / df['asset_lag']
    df['OperProfRDLagAT_q'] = df['OperProf_to_asset'].transform(lambda x: _bin(x, 10)).astype(str)

    df['CashOperProf_to_asset'] = df['CashOperProf'] / df['asset_lag']
    df['CBOperProfLagAT_q'] = df['CashOperProf_to_asset'].transform(lambda x: _bin(x, 10)).astype(str)

    df['CashOperProf_to_asset_alt'] = df['CashOperProf_alt'] / df['asset_lag']
    df['CBOperProfLagAT_alt_q'] = df['CashOperProf_to_asset_alt'].transform(lambda x: _bin(x, 10)).astype(str)

    if keep_all:
        return df
    else:
        return df[['ticker', 'GPlag_q', 'OperProfRDLagAT_q', 'CBOperProfLagAT_q', 'CBOperProfLagAT_alt_q']]

# df = profitability(finQ)



