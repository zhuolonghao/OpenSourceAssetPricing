# Instruction: change the date
date = '202402'

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

###########################################################
# Price-based factors
###########################################################
print('Process-1c: create the anomaly attributes')
exec(open('02.Signals/Code/Signals/MomCurve.py').read())
_dfs['MomCurve'] = MomCurve(keep_all=True)

###########################################################
# Merging
###########################################################
print("""Process-1d: merge all anomaly attributes. 
  -999 = not found in anomaly inputs
  -998 = found in anomaly inputs, but not found in output     
""")


output = pd.merge(_dfs['ref'], _dfs['MomCurve'], on=['ticker', 'date_ym'], how='left')\
    .fillna('-999')\
    .replace({'nan': '-998'})

rows = (output['ticker'].isin(['-999', '-998'])) | (output['sector'].isin(['-999', '-998'])) |\
       (output['ret_1m'].isin(['-999', '-998'])) | \
       (output['ret_6m'].isin(['-999', '-998'])) | \
       (output['ret_6m_gap6m'].isin(['-999', '-998']))
output2 = output[~rows].copy()

_dfs={}
_dfs_wgt={}
cols = ['mega_growth', 'mega_value',
        'large_growth', 'large_value', 'mid_growth', 'mid_value','small_growth', 'small_value']
metrics = ['ret_1m', 'ret_6m', 'ret_6m_gap6m']
output2[cols+metrics] = output2[cols+metrics].astype(float)
for ss in cols:
    rows = (output2[ss] >= 0)
    tmp = output2[rows].copy()
    _dfs[ss] = tmp.groupby('sector', as_index=False)[metrics].mean().assign(ss=ss)
    tmp[metrics] = tmp[metrics].multiply(tmp[ss], axis=0)/100
    _dfs_wgt[ss] = tmp.groupby('sector', as_index=False)[metrics].sum().assign(ss=ss)