# Instruction: change the date
date = '202405'
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

print(' Process-1a: read metadata from ./_data/_total_gics_style_sub_industry.xlsx')
ref = pd.read_excel("./_data/_total_gics_style.xlsx")
ref.columns = [x.lower() for x in ref.columns]
ref['ticker'] = ref['ticker'].transform(lambda x: str(x).replace(".", "-"))
ref['date_ym'] = date
_dfs['ref'] = ref  # this is important to make sure the anomalies aligned.

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
cols = ['ticker', 'date_ym', 'ret_1m', 'ret_6m', 'ret_m02_m11', 'ret_6m_gap6m']
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
                _dfs.values()) \
    .fillna('-999') \
    .replace({'nan': '-998'}) \
    .set_index('ticker')

rows = (output[_size_style].astype(float) < 0).all(axis=1)
output['micro_flag'] = 'non-micro'
output.loc[rows, 'micro_flag'] = 'micro'
output2 = output[~rows] # excluding micro stocks.

del base, finQ, others, ref
#######################################################################################
key_rows = []
key_index = ['ticker', 'size_style', 'gics']
key_size_style = ['mega_growth', 'mega_value', 'large_growth','large_value',
                  'mid_growth', 'mid_value', 'small_growth', 'small_value']
key_gics = ['sector', 'industry group', 'industry', 'sub-industry']
key_metrics = {
    'Count': 'count', 'Mkt_Cap': 'marketcap',
    'Mom_1m': 'ret_1m', 'Mom_6m': 'ret_6m', 'Mom_int': 'ret_6m_gap6m', 'Mom_m02m11': 'ret_m02_m11',
    'P/B': 'pricetobook', 'EV/EBITDA': 'enterprisetoebitda', 'GP/At': 'profit_to_asset', 'RoA': 'returnonassets'
}

df_ticker_total = pd.DataFrame(columns=key_index + list(key_metrics.keys()))
df_ticker_nonmicro = pd.DataFrame(columns=key_index + list(key_metrics.keys()))

df = pd.read_excel(fr'.\02.Signals\rankings_{date}.xlsx', sheet_name=focus)
for port, conditions_dict in _portfolios.items():
    df_filter = df.copy()
    print(f'producing: {port}')
    try:
        for column, condition in conditions_dict.items():
            df_filter = df_filter[condition(df_filter[column])]
        print(f'it has {df_filter.shape[0]} tickers')

        ticker_to_process = [x for x in df_filter.ticker.unique() if x not in key_rows]
        for ticker in ticker_to_process:
            key_rows.append(ticker)
            for dim in key_gics:
                dim_value = df_filter[df_filter.ticker == ticker][dim].values[0]

                df_plot = output[output[dim].eq(dim_value)].copy()
                tmp_ticker = df_plot.loc[ticker][key_size_style].astype(float)
                size_style_value = tmp_ticker.index[tmp_ticker>=0].str.cat(sep='//')
                count_value = df_plot.shape[0]
                cols = list(key_metrics.values())[1:]
                metric_values = df_plot[cols].astype(float)\
                    .apply(lambda x: f"{(x.loc[ticker] >= x).mean()*100:.0f}%")
                tmp = pd.DataFrame(
                    [[ticker, size_style_value, f"{dim}={dim_value}", count_value] + list(metric_values)],
                    columns=key_index + list(key_metrics.keys())
                    )
                df_ticker_total = pd.concat([df_ticker_total, tmp], ignore_index=True)

                df_plot = output2[output2[dim].eq(dim_value)].copy()
                tmp_ticker = df_plot.loc[ticker][key_size_style].astype(float)
                size_style_value = tmp_ticker.index[tmp_ticker>=0].str.cat(sep='//')
                count_value = df_plot.shape[0]
                cols = list(key_metrics.values())[1:]
                metric_values = df_plot[cols].astype(float)\
                    .apply(lambda x: f"{(x.loc[ticker] >= x).mean()*100:.0f}%")
                tmp = pd.DataFrame(
                    [[ticker, size_style_value, f"{dim}={dim_value}", count_value] + list(metric_values)],
                    columns=key_index + list(key_metrics.keys())
                    )
                df_ticker_nonmicro = pd.concat([df_ticker_nonmicro, tmp], ignore_index=True)

    except Exception as e:
        print(f"-------{e}---------")

df_ticker_total.set_index(key_index, inplace=True)
df_ticker_total.columns = pd.MultiIndex.from_product([['Total'], df_ticker_total.columns],
                                                     names=['Population', 'Metric'])
df_ticker_nonmicro.set_index(key_index, inplace=True)
df_ticker_nonmicro.columns = pd.MultiIndex.from_product([['Excl. Micro'], df_ticker_nonmicro.columns],
                                                        names=['Population', 'Metric'])
final = df_ticker_total.join(df_ticker_nonmicro, how='left')
final.sort_index(level=0).to_excel(fr'.\03.Portfolios\tickers.xlsx')

