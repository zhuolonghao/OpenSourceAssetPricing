date = '202312'

import pandas as pd
import numpy as np
from datetime import datetime
from functools import reduce
exec(open('_utility/_binning.py').read())
exec(open('_utility/_data_loading.py').read())

###########################################################
# Metadata
###########################################################
SignalDoc = pd.read_csv(r'./01.Assessment/SignalDoc.csv')

ref = pd.read_excel("./_data/_total_gics_style.xlsx")
ref.columns = [x.lower() for x in ref.columns]
ref['ticker'] = ref['ticker'].transform(lambda x: str(x).replace(".", "-") )
ref['date_ym'] = date

_dfs = {}
_dfs['ref'] = ref
###########################################################
# Read data
###########################################################
# monthly price
base = pd.read_parquet('02.Signals/Data/price_monthly.parquet')
base = normalize_date(base)
base = base.merge(
    ref[['ticker', 'exchange']], how='left', left_on='ticker', right_on='ticker')
# quarterly financials
finQ = pd.read_parquet('02.Signals/Data/finQ.parquet')
finQ = normalize_date(finQ)
finQ = finQ.merge(
    base, how='left', left_on=['ticker', 'date_ym'], right_on=['ticker', 'date_ym'], suffixes=('_fin', '_price')).merge(
    ref, how='left', left_on='ticker', right_on='ticker', suffixes=('_fin', ''))
# key statistics
others = pd.read_parquet('02.Signals/Data/others.parquet')
others.columns = [x.lower() for x in others.columns]
others['date_ym'] = date
# fama-french factor price
ff = pd.read_excel('02.Signals/Data/F-F_Research_Data_Factors.xlsx', sheet_name='reformatted')
ff.columns = [x.lower() for x in ff.columns]
ff['date_ym'] = ff['date_ym'].astype(str)
ff[['mkt-rf', 'smb', 'hml', 'rf']] = ff[['mkt-rf', 'smb', 'hml', 'rf']]/100

###########################################################
# Price-based factors
###########################################################
# Momentum
exec(open('02.Signals/Code/Signals/MomRev.py').read())
_dfs['MomRev'] = MomRev()
exec(open('02.Signals/Code/Signals/MomFirmAge.py').read())
_dfs['MomFirmAge'] = MomFirmAge()
exec(open('02.Signals/Code/Signals/MomVol.py').read())
_dfs['MomVol'] = MomVol()
exec(open('02.Signals/Code/Signals/MomInt.py').read())
_dfs['MomInt'] = MomInt()
exec(open('02.Signals/Code/Signals/MomResiduals.py').read())
_dfs['MomResiduals'] = MomResiduals().assign(date_ym=date)
exec(open('02.Signals/Code/Signals/Mom12mOffSeason.py').read())
_dfs['Mom12mOffSeason'] = Mom12mOffSeason()
# Seasonality
exec(open('02.Signals/Code/Signals/Seasonality_02_05.py').read())
_dfs['Seasonality_02_05'] = Seasonality_02_05()
exec(open('02.Signals/Code/Signals/Seasonality_06_10.py').read())
_dfs['Seasonality_06_10'] = Seasonality_06_10()
exec(open('02.Signals/Code/Signals/Seasonality_11_15.py').read())
_dfs['Seasonality_11_15'] = Seasonality_11_15()
exec(open('02.Signals/Code/Signals/Seasonality_16_20.py').read())
_dfs['Seasonality_16_20'] = Seasonality_16_20()
# Reversals
exec(open('02.Signals/Code/Signals/Reversals.py').read())
_dfs['Reversals'] = Reversals(base, others)
###########################################################
# Accounting-based factors
###########################################################
# Valuation
exec(open('02.Signals/Code/Signals/f_valuation.py').read())
_dfs['valuation'] = valuation(finQ).assign(date_ym=date)
# profitability
exec(open('02.Signals/Code/Signals/f_profitability.py').read())
_dfs['profitability'] = profitability(finQ).assign(date_ym=date)
# investment
exec(open('02.Signals/Code/Signals/f_investment.py').read())
_dfs['investment'] = investment(finQ).assign(date_ym=date)
# asset_composition
exec(open('02.Signals/Code/Signals/f_asset_composition.py').read())
_dfs['asset'] = asset_composition(finQ).assign(date_ym=date)
# regulatory_reporting
exec(open('02.Signals/Code/Signals/r_13F_FINRA.py').read())
_dfs['regulatory_reporting'] = regulatory_reporting(others).assign(date_ym=date)

###########################################################
# Merging
###########################################################
reduce(lambda left, right:
       pd.merge(left, right, on=['ticker', 'date_ym'],how='left'),
       _dfs.values())\
    .fillna('-999')\
    .replace({'nan': '-998'})\
    .to_excel(fr'.\02.Signals\{date}.xlsx', index=False)
