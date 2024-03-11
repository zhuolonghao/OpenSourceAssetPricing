# Instruction: change the date
date = '202401'

import pandas as pd
import pyarrow.parquet
import numpy as np
from datetime import datetime
from functools import reduce
exec(open('_utility/_binning.py').read())
exec(open('_utility/_data_loading.py').read())

_dfs = {}
print('Process-1a: read the metadata and append its ranks in each anomaly')
base = pd.read_parquet('02.Signals/Data/etf_price_monthly.parquet')
base.columns = [x.lower() for x in base.columns]
rows = base['date_ym'].le(date)
base = base[rows]
print('Process-1b: read the fama-french datase')
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
exec(open('02.Signals/Code/Signals/ETF_MoM_Reversal.py').read())
_dfs['MomRev'] = Mom_Rev()
exec(open('02.Signals/Code/Signals/MomResiduals.py').read())
_dfs['MomResiduals'] = MomResiduals().assign(date_ym=date)
# Seasonality
exec(open('02.Signals/Code/Signals/ETF_Seasonality_02_05.py').read())
_dfs['Seasonality_02_05'] = Seasonality_02_05()
exec(open('02.Signals/Code/Signals/ETF_Seasonality_06_10.py').read())
_dfs['Seasonality_06_10'] = Seasonality_06_10()
exec(open('02.Signals/Code/Signals/ETF_Seasonality_11_15.py').read())
_dfs['Seasonality_11_15'] = Seasonality_11_15()
exec(open('02.Signals/Code/Signals/ETF_Seasonality_16_20.py').read())
_dfs['Seasonality_16_20'] = Seasonality_16_20()

###########################################################
# Merging
###########################################################
print("""Process-1d: merge all anomaly attributes. 
  -999 = not found in anomaly inputs
  -998 = found in anomaly inputs, but not found in output     
""")

anomaly = reduce(lambda left, right:
       pd.merge(left, right, on=['ticker', 'date_ym'], how='left'),
       _dfs.values())\
    .fillna('-999')\
    .replace({'nan': '-998'})
rows = anomaly['date_ym'].eq(date)
anomaly2 = anomaly[rows].set_index('ticker').astype('float')
anomaly2['vote'] = anomaly2[['Mom',
       'MomInt', 'MomRev', 'Mom_m02_m11_ret', 'Mom_m02_m11_sign', 'STreversal',
       'MRreversal', 'LRreversal', 'MomResiduals6m', 'MomResiduals12m',
       'Season_0205', 'OffSeason_0205']].sum(axis=1)
anomaly2.set_index('vote', append=True).to_excel(fr'02.Signals\Reporting\ETF_{date}.xlsx')

print('Completed: ETF')