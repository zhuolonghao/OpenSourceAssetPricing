for f in dir():
    if f != 'output': del globals()[f]

import pandas as pd
import numpy as np
from datetime import datetime
from functools import reduce
exec(open('_utility/_binning.py').read())

_dfs = {}
_dfs['ref'] = output
###########################################################
# Read data
###########################################################

others = pd.read_parquet('02.Signals/Data/others.parquet')
others.columns = [x.lower() for x in others.columns]
others['dateshortinterest'] = others['dateshortinterest'].transform(lambda x: datetime.fromtimestamp(x).date())
others['sharesshortpreviousmonthdate'] = others['sharesshortpreviousmonthdate'].transform(lambda x: datetime.fromtimestamp(x).date())
vars = ['ticker', 'fulltimeemployees', 'beta', 'exchange', 'marketcap', 'fiftytwoweeklow', 'fiftytwoweekhigh', 'enterprisevalue',
        'floatshares', 'sharesoutstanding', 'sharesshort', 'dateshortinterest', 'sharesshortpriormonth', 'sharesshortpreviousmonthdate', 'sharespercentsharesout', 'shortratio', 'shortpercentoffloat',
        'currentprice', 'targethighprice', 'targetlowprice', 'targetmeanprice', 'targetmedianprice', 'numberofanalystopinions', 'recommendationmean',
        '% of shares held by all insider', '% of shares held by institutions', '% of float held by institutions', 'number of institutions holding shares'
        ]
others = others[vars]

fin = pd.read_parquet('02.Signals/Data/fin.parquet')
fin.columns = [x.lower() for x in fin.columns]
fin['date_ymd'] = fin['date_raw'].dt.strftime("%Y%m%d")
fin['date_ym'] = fin['date_ymd'].str[:6]
fin = fin.sort_values(by=['ticker', 'date_raw'])

finQ = pd.read_parquet('02.Signals/Data/finQ.parquet')
finQ.columns = [x.lower() for x in finQ.columns]
finQ['date_ymd'] = finQ['date_raw'].dt.strftime("%Y%m%d")
finQ['date_ym'] = finQ['date_ymd'].str[:6]
finQ = finQ.sort_values(by=['ticker', 'date_raw'])

base = pd.read_parquet('02.Signals/Data/price_monthly.parquet')
base.columns = [x.lower() for x in base.columns]
base['date_ymd'] = base['date_raw'].dt.strftime("%Y%m%d")
base['date_ym'] = base['date_ymd'].str[:6]
base = base.sort_values(by=['ticker', 'date_raw'])

ref = pd.read_excel("./_data/_total_gics_style.xlsx")

fin = fin.merge(
    base[['ticker', 'date_ym', 'close']], how='left', left_on=['ticker', 'date_ym'], right_on=['ticker', 'date_ym']).merge(
    ref, how='left', left_on='ticker', right_on='ticker').sort_values(by=['ticker', 'date_raw'])
fin.groupby('date_ym')['ticker'].nunique()

finQ = finQ.merge(
    base[['ticker', 'date_ym', 'close']], how='left', left_on=['ticker', 'date_ym'], right_on=['ticker', 'date_ym']).merge(
    ref, how='left', left_on='ticker', right_on='ticker').sort_values(by=['ticker', 'date_raw'])
finQ.groupby('date_ym')['ticker'].nunique()

###########################################################
# Valuation
###########################################################
exec(open('02.Signals/Code/Signals/f_valuation.py').read())
_dfs['valuation'] = valuation(finQ)
###########################################################
# profitability
###########################################################
exec(open('02.Signals/Code/Signals/f_profitability.py').read())
_dfs['profitability'] = profitability(finQ)
###########################################################
# investment
###########################################################
exec(open('02.Signals/Code/Signals/f_investment.py').read())
_dfs['investment'] = investment(finQ)
###########################################################
# asset_composition
###########################################################
exec(open('02.Signals/Code/Signals/f_asset_composition.py').read())
_dfs['asset'] = asset_composition(finQ)
###########################################################
# regulatory_reporting
###########################################################
exec(open('02.Signals/Code/Signals/r_13F_FINRA.py').read())
_dfs['regulatory_reporting'] = regulatory_reporting(others)


###########################################################
# Merging
###########################################################
output = reduce(lambda left, right:
                pd.merge(left, right, on=['ticker'],
                         how='left'), _dfs.values())