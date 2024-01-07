import pandas as pd
import numpy as np
from functools import reduce
exec(open('_utility/_binning.py').read())

ref = pd.read_excel("./_data/total_stock_market_holdings.xlsx", sheet_name='reformatted')
ref.columns = [x.lower() for x in ref.columns]
ref['ticker'] = ref['ticker'].transform(lambda x: str(x).replace(".", "-") )
ref['date_ym'] = '202311'

_dfs = {}
_dfs['ref'] = ref
###########################################################
# Read data
###########################################################

SignalDoc = pd.read_csv(r'./01.Assessment/SignalDoc.csv')

base = pd.read_parquet('02.Signals/Data/price_monthly.parquet')
base.columns = [x.lower() for x in base.columns]
base['date_ymd'] = base['date_raw'].dt.strftime("%Y%m%d")
base['date_ym'] = base['date_ymd'].str[:6]
base = base.sort_values(by=['ticker', 'date_raw'])

ff = pd.read_excel('02.Signals/Data/F-F_Research_Data_Factors.xlsx', sheet_name='reformatted')
ff.columns = [x.lower() for x in ff.columns]
ff['date_ym'] = ff['date_ym'].astype(str)
ff[['mkt-rf', 'smb', 'hml', 'rf']] = ff[['mkt-rf', 'smb', 'hml', 'rf']]/100


###########################################################
# Momentum
###########################################################
exec(open('02.Signals/Code/Signals/MomRev.py').read())
_dfs['MomRev'] = MomRev()
exec(open('02.Signals/Code/Signals/MomFirmAge.py').read())
_dfs['MomFirmAge'] = MomFirmAge()
exec(open('02.Signals/Code/Signals/MomVol.py').read())
_dfs['MomVol'] = MomVol()
exec(open('02.Signals/Code/Signals/Mom12mOffSeason.py').read())
_dfs['Mom12mOffSeason'] = Mom12mOffSeason()
exec(open('02.Signals/Code/Signals/MomInt.py').read())
_dfs['MomInt'] = MomInt()
exec(open('02.Signals/Code/Signals/MomResiduals.py').read())
_dfs['MomResiduals'] = MomResiduals()

###########################################################
# Seasonality
###########################################################
exec(open('02.Signals/Code/Signals/Seasonality_02_05.py').read())
_dfs['Seasonality_02_05'] = Seasonality_02_05()
exec(open('02.Signals/Code/Signals/Seasonality_06_10.py').read())
_dfs['Seasonality_06_10'] = Seasonality_06_10()
exec(open('02.Signals/Code/Signals/Seasonality_11_15.py').read())
_dfs['Seasonality_11_15'] = Seasonality_11_15()
exec(open('02.Signals/Code/Signals/Seasonality_16_20.py').read())
_dfs['Seasonality_16_20'] = Seasonality_16_20()


###########################################################
# Reversals
###########################################################
exec(open('02.Signals/Code/Signals/Reversals.py').read())
_dfs['Reversals'] = Reversals()


###########################################################
# Merging
###########################################################

output = reduce(lambda left, right:
                pd.merge(left, right, on=['ticker', 'date_ym'],
                         how='left'), _dfs.values())
