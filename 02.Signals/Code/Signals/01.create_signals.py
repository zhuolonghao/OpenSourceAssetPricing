# Instruction: change the date
date = '202403'

import pandas as pd
import pyarrow.parquet
import numpy as np
from datetime import datetime
from functools import reduce
exec(open('_utility/_binning.py').read())
exec(open('_utility/_data_loading.py').read())

_dfs = {}
print('Process-1: read the metadata and append its ranks in each anomaly')
ref = pd.read_excel("./_data/_total_gics_style.xlsx")
ref.columns = [x.lower() for x in ref.columns]
ref['ticker'] = ref['ticker'].transform(lambda x: str(x).replace(".", "-") )
ref['date_ym'] = date
_dfs['ref'] = ref # this is important to make sure the anomalies aligned.
print(' Process-1a: read metadata from ./_data/_total_gics_style.xlsx')

print(' Process-1b-1: read the monthly price data')
base = pd.read_parquet('02.Signals/Data/price_monthly.parquet')
base.columns = [x.lower() for x in base.columns]
base = base.merge(
    ref[['ticker', 'exchange']], how='left', left_on='ticker', right_on='ticker')
print(' Process-1b-2: read the quarter financials')
finQ = pd.read_parquet('02.Signals/Data/finQ.parquet')
finQ.columns = [x.lower() for x in finQ.columns]
finQ = finQ.merge(
    base, how='left', left_on=['ticker', 'date_ym'], right_on=['ticker', 'date_ym'], suffixes=('_fin', '_price')).merge(
    ref, how='left', left_on='ticker', right_on='ticker', suffixes=('_fin', ''))
finQ = finQ.sort_values(['ticker', 'date_ym_fin'], ascending=True)
print(' Process-1b-3: read the other information')
others = pd.read_parquet('02.Signals/Data/others.parquet')
others.columns = [x.lower() for x in others.columns]
others['date_ym'] = date
print(' Process-1b-4: read the fama-french datase')
ff = pd.read_csv('02.Signals/Data/F-F_Research_Data_Factors.csv',
                 skiprows=4, usecols=list(range(0, 5)), header=None)
last_row = np.where(ff.iloc[:, 0].isna())[0][0]-1
ff = pd.DataFrame(ff.values[1:last_row], columns=['date_ym', 'mkt-rf', 'smb', 'hml', 'rf'])
ff.columns = [x.lower() for x in ff.columns]
ff['date_ym'] = ff['date_ym'].astype(str)
ff[['mkt-rf', 'smb', 'hml', 'rf']] = ff[['mkt-rf', 'smb', 'hml', 'rf']].astype(float)
ff[['mkt-rf', 'smb', 'hml', 'rf']] = ff[['mkt-rf', 'smb', 'hml', 'rf']]/100

###########################################################
# Price-based factors
###########################################################
print('Process-1c: create the anomaly attributes')
exec(open('02.Signals/Code/Signals/MomCurve.py').read())
_dfs['MomCurve'] = MomCurve()
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
# Rely on Yahoo
exec(open('02.Signals/Code/Signals/f_alternative.py').read())
_dfs['yahoo'] = valuation_profitability(others).assign(date_ym=date)
# Valuation
exec(open('02.Signals/Code/Signals/f_valuation.py').read())
_dfs['valuation'] = valuation(finQ).assign(date_ym=date).drop(columns=['BM_q', 'EntMult_q', 'cfp_q'])
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
print("""Process-1d: merge all anomaly attributes. 
  -999 = not found in anomaly inputs
  -998 = found in anomaly inputs, but not found in output     
""")

reduce(lambda left, right:
       pd.merge(left, right, on=['ticker', 'date_ym'], how='left'),
       _dfs.values())\
    .fillna('-999')\
    .replace({'nan': '-998'})\
    .to_excel(fr'.\02.Signals\{date}.xlsx', index=False)

