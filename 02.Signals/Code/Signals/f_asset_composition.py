# strategy: buy stocks with high concentration in certain asset component(s).

# note these asset components are mainly items in the current asset (short-term)
# rationale: firms tend to increase current asset in anticipation of higher revenue of today and tomorrow.
#   more cash is to lower funding cost of future investment.
#   higher receivables is to allow higher demand of today
#   higher inventory is to allow higher demand of tomorrow.

# Cash: https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/Cash.do
#   setup: management can fund investment via internal (which is cheaper) and/or external financing (which is expensive).
#   trade-off: decide btw distributing dividends vs saving (i.e., retained earnings ---> cash).
#   conclusion: riskier firms tend to save more b/c higher likelihood of a cash flow shortfall in which external financing is needed most.


# tang: https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Placebos/tang_q.do
# https://www.law.nyu.edu/node/29319
# https://fintel.io/industry#consumer-discretionary
# use asset tangibility (liquid asset) to define "debt capacity". Higher debt capacity can borrow more
#   Debt capacity is positively associated with stock returns in the cross section of financially constrained firms Debt capacity has no systematic relation with the cross section of financially unconstrained firms stock returns
#   financial constrain is measured by four criteria, respectively, for robustness:
#   asset size in terms of the book value of total assets,
#   payout ratio,
#   bond ratings, and
#   commercial paper ratings.

# Limiting to manufacturing industry groups, key observations include
#   rationale: higher rank indicates the management forecast of higher revenues/sales
#   Growth stocks tend to have higher rank than Value stocks, after controlling for size
#           conclusion: buy growth over value
#   Looking at value stocks only, small stocks tend to have higher ratio than large stocks,
#           conclusioN: buy small-value over big-value
def asset_composition(base, keep_all=False):
    vars = ['ticker', 'date_ym',
            'cashandcashequivalents', 'cashcashequivalentsandshortterminvestments', 'accountsreceivable', 'inventory', 'netppe', 'grossppe',
            'totalassets']
    rows = base['industry group'].isin([ # focus on manufacturing industry group
        'Materials',
        'Capital Goods',
        'Automobiles & Components', 'Consumer Durables & Apparel',
        'Food, Beverage & Tobacco', 'Household & Personal Products',
        'Technology Hardware & Equipment', 'Semiconductors & Semiconductor Equip',
    ])

    df = base[rows][vars].copy()

    df['cash'] = df[['cashandcashequivalents', 'cashcashequivalentsandshortterminvestments']].max(axis=1)
    df['gross_ppe'] = df[['netppe', 'grossppe']].max(axis=1)
    df['tang'] = df['cash'] + 0.715*df['accountsreceivable'] + 0.547*df['inventory'] + 0.535*df['gross_ppe']

    df = df.groupby('ticker').tail(1)
    df['cash_to_asset'] = df['cash'] / df['totalassets']
    df['cash_q'] = df['cash_to_asset'].transform(lambda x: _bin(x, 10)).astype(str)

    df['tang_to_asset'] = df['tang'] / df['totalassets']
    df['tang_q'] = df['tang_to_asset'].transform(lambda x: _bin(x, 10)).astype(str)

    print('Complete: asset composition')

    if keep_all:
        return df
    else:
        return df[['ticker',  'cash_q', 'tang_q']]