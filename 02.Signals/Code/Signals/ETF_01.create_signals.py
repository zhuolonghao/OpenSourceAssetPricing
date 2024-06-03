# Instruction: change the date
date = '202405'

import pandas as pd
import pyarrow.parquet
import numpy as np
from datetime import datetime
from functools import reduce
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap,ListedColormap, BoundaryNorm, Normalize
from matplotlib.backends.backend_pdf import PdfPages


exec(open('_utility/_binning.py').read())
exec(open('_utility/_data_loading.py').read())
exec(open('_utility/_anomaly_portfolio.py').read())

_dfs = {}
print('Process-1a: read the metadata and append its ranks in each anomaly')
base = pd.read_parquet('02.Signals/Data/etf_price_monthly.parquet')
base.columns = [x.lower() for x in base.columns]
rows = base['date_ym'].le(date)
base = base[rows]
base = base.sort_values(["ticker", "date_ym"])

df_plot = base.copy()
df_plot['close'] = base.groupby('ticker')['close'].pct_change(1)
df_plot['date_ym'] = pd.to_datetime(df_plot['date_ym'], format='%Y%m')
df_plot['year'] = df_plot['date_ym'].dt.strftime("%Y")
df_plot['month'] = df_plot['date_ym'].dt.strftime("%b")
df_plot = df_plot[df_plot['year'] >= '2013']

cols =['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep',
       'Oct', 'Nov', 'Dec']
# colors = ['red', 'green']  # Assign colors to the ranges
# cmap = ListedColormap(colors)
colors = [(0, 'red'), (0.5, 'white'), (1, 'green')]
cmap = LinearSegmentedColormap.from_list('CustomMap', colors)
norm = Normalize(vmin=-0.03, vmax=0.03)

with PdfPages('03.Portfolios/ETF_s2.pdf') as pdf:
    num_plots = len(_etf_s2)
    num_pages = (num_plots + 5) // 6  # Calculate the number of pages needed (each page will contain 3x2 plots)

    for page in range(num_pages):
        fig = plt.figure(figsize=(12, 8))
        for ix in range(6):
            plot_index = page * 6 + ix
            if plot_index < num_plots:
                plt.subplot(2, 3, ix + 1)
                ticker = list(_etf_s2)[plot_index]
                rows = df_plot.ticker.eq(ticker)
                df = df_plot[rows].pivot(index='year', columns='month', values='close')
                df = df[cols]
                plt.imshow(df, cmap=cmap, norm=norm)
                for i in range(df.shape[0]):
                    for j in range(df.shape[1]):
                        if ~np.isnan(df.iloc[i, j]):
                            plt.text(j, i, f"{df.iloc[i, j] * 100: .1f}", fontsize=6, horizontalalignment='center',
                                     verticalalignment='center')
                plt.title(f'Monthly Return: {_etf_s2[ticker]}')
                plt.xticks(range(df.shape[1]), df.columns, rotation='vertical')  # Setting x-axis labels
                plt.yticks(range(df.shape[0]), df.index)  # Setting y-axis labels
        plt.tight_layout()
        pdf.savefig(fig)  # Save the current page to the PDF
        plt.close()

with PdfPages('03.Portfolios/ETF_sector.pdf') as pdf:
    num_plots = len(_etf_s)
    num_pages = (num_plots + 5) // 6  # Calculate the number of pages needed (each page will contain 3x2 plots)

    for page in range(num_pages):
        fig = plt.figure(figsize=(12, 8))
        for i in range(6):
            plot_index = page * 6 + i
            if plot_index < num_plots:
                plt.subplot(2, 3, i + 1)
                ticker = list(_etf_s)[plot_index]
                rows = df_plot.ticker.eq(ticker)
                df = df_plot[rows].pivot(index='year', columns='month', values='close')
                df = df[cols]
                plt.imshow(df, cmap=cmap, norm=norm)
                for i in range(df.shape[0]):
                    for j in range(df.shape[1]):
                        if ~np.isnan(df.iloc[i, j]):
                            plt.text(j, i, f"{df.iloc[i, j] * 100: .1f}", fontsize=6, horizontalalignment='center',
                                     verticalalignment='center')
                plt.title(f'Monthly Return: {_etf_s[ticker]}')
                plt.xticks(range(df.shape[1]), df.columns, rotation='vertical')  # Setting x-axis labels
                plt.yticks(range(df.shape[0]), df.index)  # Setting y-axis labels
        plt.tight_layout()
        pdf.savefig(fig)  # Save the current page to the PDF
        plt.close()


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
anomaly2.set_index('vote', append=True).to_excel(fr'02.Signals\ETF_{date}.xlsx')

print('Completed: ETF')