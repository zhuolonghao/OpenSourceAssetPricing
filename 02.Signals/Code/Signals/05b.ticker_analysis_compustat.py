# Instruction: change the date
date = '202402'
focus = 'non_micro_tickers'

import pandas as pd
import pyarrow.parquet
import numpy as np
from datetime import datetime
from functools import reduce
import matplotlib.pyplot as plt
import matplotlib.ticker as plt_ticker
from scipy.stats.mstats import winsorize

exec(open('_utility/_anomaly_portfolio.py').read())
exec(open('_utility/_binning.py').read())
exec(open('_utility/_data_loading.py').read())

_dfs = {}
print('Process-1: read the metadata and append its ranks in each anomaly')

print(' Process-1a: read metadata from ./_data/_total_gics_style.xlsx')
ref = pd.read_excel("./_data/_total_gics_style.xlsx")
ref.columns = [x.lower() for x in ref.columns]
ref['ticker'] = ref['ticker'].transform(lambda x: str(x).replace(".", "-") )
ref['date_ym'] = date
_dfs['ref'] = ref # this is important to make sure the anomalies aligned.

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

print(' Process-1b-2: read the other information')
others = pd.read_parquet('02.Signals/Data/others.parquet')
others.columns = [x.lower() for x in others.columns]
others['date_ym'] = date
###########################################################
# Price-based factors
###########################################################
print('Process-1c: create the anomaly attributes')
exec(open('02.Signals/Code/Signals/MomCurve.py').read())
cols =['ticker', 'date_ym', 'ret_1m', 'ret_6m', 'ret_m02_m11', 'ret_6m_gap6m']
_dfs['MomCurve'] = MomCurve(keep_all=True)[cols]
exec(open('02.Signals/Code/Signals/f_alternative.py').read())
cols = ["ticker", "marketcap", "enterprisetoebitda", "pricetobook", "returnonassets"]
_dfs['yahoo'] = valuation_profitability(others, keep_all=True)[cols].assign(date_ym=date)
# profitability
exec(open('02.Signals/Code/Signals/f_profitability.py').read())
cols = ['ticker', 'profit_to_asset']
_dfs['profitability'] = profitability(finQ, keep_all=True)[cols].assign(date_ym=date)
###########################################################
# Merging
###########################################################
print("""Process-1d: merge all anomaly attributes. 
  -999 = not found in anomaly inputs
  -998 = found in anomaly inputs, but not found in output     
""")

output = reduce(lambda left, right: pd.merge(left, right, on=['ticker', 'date_ym'], how='left'),
                _dfs.values())\
    .fillna('-999')\
    .replace({'nan': '-998'})\
    .set_index('ticker')

rows = (output[_size_style].astype(float) < 0).all(axis=1)
output['micro_flag']='non-micro'
output.loc[rows,'micro_flag']='micro'
#output = output[~rows]

del base, finQ, others, ref
#######################################################################################
df = pd.read_excel(fr'.\02.Signals\rankings_{date}.xlsx',  sheet_name=focus)
for key, value in _anomalies.items():
    df[key] = df[key] / len(value)
df2 = df.reset_index()

for port, conditions_dict in _portfolios.items():
    df_filter = df2.copy()
    print(f'producing: {port}')
    try:
        for column, condition in conditions_dict.items():
            df_filter = df_filter[condition(df_filter[column])]
        print(f'it has {df_filter.shape[0]} tickers')

        ticker = 'CVNA'
        metric = 'marketcap'
        dim = 'sector'
        dim_value = df_filter[df_filter.ticker == ticker][dim].values[0]
        df_plot = output[output[dim].eq(dim_value)].copy()
        rows = df_plot[metric].isin(['-999', '-998'])
        df_plot2 = df_plot[~rows][metric].astype(float).sort_values().copy()
        min, max = df_plot2.min(), df_plot2.max()
        df_plot3 = np.log((df_plot2 - min) / (max - min) * (1 - 2 * np.finfo(float).eps) + np.finfo(float).eps)
        tmp = winsorize(df_plot3, limits=[0.005, 0.005], inclusive=(False, False))
        df_plot3 = pd.Series(tmp - tmp.min(), index=df_plot3.index)

        fig, ax = plt.subplots(figsize=(5, 2.5))
        num_data_points = len(df_plot3)
        df_plot3.plot(kind='bar', use_index=True, ax=ax)

        ax.set_xticklabels([])
        ax.yaxis.set_visible(False)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)

        v_loc = df_plot3.index.get_loc(ticker)
        ax.axvline(x=v_loc, color='r', linestyle='--')

        tick_positions = np.linspace(0, num_data_points - 1, 11).astype(int)
        ax.xaxis.set_major_locator(plt_ticker.FixedLocator(tick_positions))

        plt.tight_layout()
        plt.show()



    except Exception as e:
        print(f"-------{e}---------")


ticker = 'CVNA'
metric = 'marketcap'
dim = 'sector'
dim_value = df_filter[df_filter.ticker == ticker][dim].values[0]
df_plot = output[output[dim].eq(dim_value)].copy()