# strategy: buy value stocks

# value vs glamour stocks
#   value means     low past sale growth,   high book-to-market,    high earnings-to-price,     and high cash flow-to-price
#   glamour means   high past sale growth,  low book-to-market,     low earnings-to-price,      and low cash flow-to-price

# AccrualsBM: https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/AccrualsBM.do
#   a joint identification based on BM and accruals
#   conclusion: buying (selling) stocks with a high (low) B/M and low (high) accruals

# EBM: https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/ZZ1_EBM_BPEBM.do
#   a decomposition of BM to two sources of risk; operating risk (EBM) and financial/leverage risk (BP - EBM).
#   conclusion: buying (selling) stocks with a high EBM.

# cfp: https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/cfp.do
#   cash flow-to-price, but cash flow was measured by cash flow generated from core business: operating cash flow (or net income - accruals)
#   conclusion: buying (selling) stocks with a high cash flow-to-price.

# EntMult: https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/EntMult.do
#   practitioners: (MVE + Debt + Preferred stocks - Cash) / Earning
#   conclusion: buying (selling) stocks with a low EntMult, b/c they earn same money with less resources.


finQ[[x for x in finQ.columns if 'prefer' in x.lower()]].count()

def valuation(base, keep_all=False):
    var_base = ['ticker', 'date_ym', 'sector', 'exchange']
    var_AccrualsBM = [
        # Accruals
        'currentassets', 'cashandcashequivalents', 'cashcashequivalentsandshortterminvestments',
        'currentliabilities', 'currentdebt', 'currentdebtandcapitalleaseobligation',
        'taxprovision', 'totalassets',
        # BM
        'commonstockequity', 'close', 'ordinarysharesnumber']
    var_EntMult = [
        # numerator
        'close', 'ordinarysharesnumber',
        'longtermdebt', 'longtermdebtandcapitalleaseobligation',
        'currentdebt', 'currentdebtandcapitalleaseobligation',
        'preferredstock',
        'cashandcashequivalents', 'cashcashequivalentsandshortterminvestments',
        # denominator
        'ebitda']
    var_CFP = [
        # numerator: operating cash flow
        'operatingcashflow',
        # numerator: net income - accruals
        'netincome',
        'currentassets', 'cashandcashequivalents', 'cashcashequivalentsandshortterminvestments',
        'currentliabilities', 'currentdebt', 'currentdebtandcapitalleaseobligation',
        'taxprovision', 'reconcileddepreciation',
        # denominator
        'close', 'ordinarysharesnumber']
    var_EBM = [
        # enterprise component in BM
        'cashandcashequivalents', 'cashcashequivalentsandshortterminvestments',
        'longtermdebt', 'longtermdebtandcapitalleaseobligation',
        'currentdebt', 'currentdebtandcapitalleaseobligation',
        'currentdeferredassets', 'noncurrentdeferredassets',
        'preferredstockdividendpaid', 'preferredstockdividends',
        'treasurystock', 'treasurysharesnumber', 'close',
        # BM
        'commonstockequity', 'close', 'ordinarysharesnumber']
    var_CF = [
        # numerator
        'netincome', 'reconcileddepreciation',
        # denominator
        'close', 'ordinarysharesnumber']
    var_BM = ['commonstockequity', 'close', 'ordinarysharesnumber']

    vars = list(set(var_base+var_BM+var_CF+var_EBM+var_CFP+var_AccrualsBM+var_EntMult))
    rows = base.sector.ne('Financials')
    df = base[rows][vars].copy()

    df['mve'] = df['close'] * df['ordinarysharesnumber'] / 1e3
    df['mve_adj'] = df['mve'].transform(lambda x: x if x > 0 else None)

    df['cash'] = df[['cashandcashequivalents', 'cashcashequivalentsandshortterminvestments']].max(axis=1)
    df['debt_current'] = df[['currentdebt', 'currentdebtandcapitalleaseobligation']].max(axis=1)
    df['debt_noncurrent'] = df[['longtermdebt', 'longtermdebtandcapitalleaseobligation']].max(axis=1)
    df['dividends_prefer'] = df[['preferredstockdividends', 'preferredstockdividendpaid']].max(axis=1)
    df['treasurystock2'] = df['treasurysharesnumber'] * df['close']
    df['treasury_stock'] = df[['treasurystock', 'treasurystock2']].max(axis=1)

    df['deferred_charges'] = df[['currentdeferredassets', 'noncurrentdeferredassets']].sum(axis=1)

    df['asset_avg'] = 0.5*(df['totalassets'] + df.groupby('ticker')['totalassets'].shift(4))

    # missing values in these variables are not systematic
    vars_fill = ['taxprovision', 'dividends_prefer', 'deferred_charges', 'preferredstock', 'reconcileddepreciation']
    for v in vars_fill: df[v] = df[v].fillna(0)
    # calcualte annual change
    vars_chg = ['taxprovision', 'currentassets', 'cash', 'currentliabilities', 'debt_current']
    for v in vars_chg: df[f"{v}_chg"] = df[v] - df.groupby('ticker')[v].shift(4)
    df['Accruals'] = (df['currentassets_chg'] - df['cash_chg'] \
                     - df['currentliabilities_chg'] - df['debt_current_chg'] \
                     - df['taxprovision_chg'])
    df['Accruals_v2'] = (df['currentassets_chg'] - df['cash_chg'] \
                     - df['currentliabilities_chg'] - df['debt_current_chg'] \
                     - df['taxprovision_chg']) \
                     - df['reconcileddepreciation']
    df['Accruals_v3'] = df['netincome'] - df['operatingcashflow']
    df['EBM_drift'] = df['cash'] - df['debt_noncurrent'] - df['debt_current'] - df['deferred_charges'] - df['dividends_prefer'] + df['treasury_stock']
    df['ent_value'] = df['mve'] + df['debt_current'] + df['debt_noncurrent'] + df['preferredstock'] - df['cash']

    df = df.groupby('ticker').tail(1)

    df['BM'] = df['commonstockequity'] / df['mve_adj']
    df['BM_q5'] = df['BM'].transform(lambda x: _bin(x, 5)).astype(str)
    df['BM_q'] = df['BM'].transform(lambda x: _bin(x, 10)).astype(str)

    df['Accruals_to_asset'] = df['Accruals'] / df['asset_avg']
    df['Accruals_q'] = df['Accruals_to_asset'].transform(lambda x: _bin(x, 5)).astype(str)

    df['AccrualsBM_q'] = None
    df.loc[df['BM_q5'].eq('5') & df['Accruals_q'].eq('1'), 'AccrualsBM_q'] = 1
    df.loc[df['BM_q5'].eq('1') & df['Accruals_q'].eq('5'), 'AccrualsBM_q'] = 0
    df.loc[df['commonstockequity'].le(0), 'AccrualsBM_q'] = None

    df['cfp'] = df['operatingcashflow'] / df['mve_adj']
    df.loc[df['operatingcashflow'].isna(), 'cfp'] = (df['netincome'] - df['Accruals_v2']) / df['mve_adj']
    df['cfp_q'] = df['cfp'].transform(lambda x: _bin(x, 5)).astype(str)

    df['EBM'] = (df['commonstockequity'] + df["EBM_drift"]) / (df['mve_adj'] + df["EBM_drift"] )
    df['EBM_q'] = df['EBM'].transform(lambda x: _bin(x, 10)).astype(str)

    df['EntMult'] = None
    df.loc[df['ebitda'].ge(0) & df['commonstockequity'].ge(0), 'EntMult'] =  -1 * df['ent_value'] / df['ebitda']
    df['EntMult_q'] = df['EntMult'].transform(lambda x: _bin(x, 10)).astype(str)

    df['CF'] = None
    df.loc[df['exchange'].isin(['NYSE', 'AMEX']), 'EntMult'] = (df['netincome'] + df['reconcileddepreciation'])/ df['mve_adj']
    df['CF_q'] = df['EntMult'].transform(lambda x: _bin(x, 10)).astype(str)

    if keep_all:
        return df
    else:
        return df[['ticker', 'BM_q', 'AccrualsBM_q', 'CF_q', 'EntMult_q', 'EBM_q', 'cfp_q']]

df = valuation(finQ)



