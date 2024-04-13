import pandas as pd
import numpy as np
import beaapi
from functools import reduce
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(style='ticks')
from matplotlib.backends.backend_pdf import PdfPages

years = ['2020', '2021', '2022', '2023', '2024']

lvl1 = ['DPCERA', 'DGDSRA', 'DDURRA', 'DSERRA']
lvl2 = ['DMOTRA', 'DFDHRA', 'DREQRA', 'DODGRA', 'DFXARA', 'DCLORA', 'DGOERA', 'DONGRA', 'DHCERA', 'DHUTRA', 'DHLCRA', 'DTRSRA', 'DRCARA', 'DFSARA', 'DIFSRA', 'DOTSRA']
lvl3 = ['DPCERA', 'DGDSRA', 'DDURRA', 'DNMVRA', 'DNPVRA', 'DMVPRA', 'DFFFRA', 'DAPPRA', 'DUTERA', 'DTOORA', 'DVAPRA', 'DSPGRA', 'DWHLRA', 'DRBKRA', 'DMSCRA', 'DJRYRA', 'DTAERA', 'DEBKRA', 'DLUGRA', 'DTCERA', 'DNDGRA', 'DTFDRA', 'DAOPRA', 'DFFDRA', 'DGARRA', 'DWGCRA', 'DMBCRA', 'DCICRA', 'DOCCRA', 'DMFLRA', 'DFULRA', 'DPHMRA', 'DREIRA', 'DHOURA', 'DOPCRA', 'DTOBRA', 'DNEWRA', 'DSERRA', 'DHSGRA', 'DTENRA', 'DOWNRA', 'DFARRA', 'DGRHRA', 'DUTLRA', 'DWRSRA', 'DELGRA', 'DELCRA', 'DGHERA', 'DOUTRA', 'DPHYRA', 'DDENRA', 'DPMSRA', 'DHPNRA', 'DHSPRA', 'DNRSRA', 'DMVSRA', 'DVMRRA', 'DOVSRA', 'DPUBRA', 'DGRDRA', 'DAITRA', 'DWATRA', 'DRLSRA', 'DAVPRA', 'DGAMRA', 'DOTRRA', 'DFSERA', 'DPMBRA', 'DFOORA', 'DACCRA', 'DFNLRA', 'DIMPRA', 'DOFIRA', 'DINSRA', 'DLIFRA', 'DFINRA', 'DHINRA', 'DTINRA', 'DCOMRA', 'DTCSRA', 'DPSSRA', 'DINTRA', 'DTEDRA', 'DHEDRA', 'DNEHRA', 'DVEDRA', 'DPRSRA', 'DPERRA', 'DSOCRA', 'DHHMRA', 'DFTRRA']

beakey = '0FAD9DAD-B778-42E9-BA0E-454F0DE7BC7D'
_real = {}
_price = {}
for y in years:
    _real[y] = beaapi.get_data(beakey, datasetname='NIPA', TableName='T20803', Frequency='M', Year=y)
    _price[y] = beaapi.get_data(beakey, datasetname='NIPA', TableName='T20804', Frequency='M', Year=y)

real = pd.concat(_real.values(), axis=0).sort_values(['SeriesCode', 'TimePeriod'])
price = pd.concat(_price.values(), axis=0).sort_values(['SeriesCode', 'TimePeriod'])

for name, lag in {'MoM': 1, 'QoQ': 3, 'YoY': 12}.items():
    real['pct_chg'] = real.groupby(['SeriesCode'])['DataValue'].pct_change(lag)
    rows = real['SeriesCode'].isin(lvl2)
    df = real[rows].copy()
    df['LineDescription'] = df['LineDescription'].str[:20]
    df['rank'] = df.groupby('TimePeriod')['pct_chg'].transform(lambda x: x.rank())
    df2 = df.groupby('SeriesCode').tail(18).pivot(index='TimePeriod', columns="LineDescription", values='rank')
    ax = df2.plot(style='--^',  ax=plt.gca(), figsize=[16,6], legend=False, title=f'Real Spending: {name}')
    ###
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.set_xlabel('')
    ax.set_xticks(range(len(df2.index)))
    ax.set_xticklabels(df2.index)
    ####
    lines = ax.get_lines()
    for line in lines:
        x, y = line.get_data()
        last_x, last_y = x[-1], y[-1]
        ax.text(last_x+0.1, last_y, line.get_label(), verticalalignment='center')
    plt.xticks(rotation=0)
    plt.tick_params(left=False, labelleft=False)
    plt.tight_layout()
    plt.savefig(fr'02.Signals\econ_PCER_{name}.pdf', bbox_inches='tight')
    plt.show()


price['pct_chg'] = price.groupby(['SeriesCode'])['DataValue'].pct_change(12)
lvl2 = [ x[:-1] + 'G' for x in lvl2]
rows = price['SeriesCode'].isin(lvl2)
df = price[rows].copy()
df['LineDescription'] = df['LineDescription'].str[:20]
df['rank'] = df.groupby('TimePeriod')['pct_chg'].transform(lambda x: x.rank())
df2 = df.groupby('SeriesCode').tail(18).pivot(index='TimePeriod', columns="LineDescription", values='rank')
ax = df2.plot(style='--^',  ax=plt.gca(), figsize=[16,6], legend=False, title=f'PCEPI: {name}')
###
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)
ax.set_xlabel('')
ax.set_xticks(range(len(df2.index)))
ax.set_xticklabels(df2.index)
####
lines = ax.get_lines()
for line in lines:
    x, y = line.get_data()
    last_x, last_y = x[-1], y[-1]
    ax.text(last_x+0.1, last_y, line.get_label(), verticalalignment='center')
plt.xticks(rotation=0)
plt.tick_params(left=False, labelleft=False)
plt.tight_layout()
plt.savefig(fr'02.Signals\econ_PCEPI_{name}.pdf', bbox_inches='tight')
plt.show()
###########################################################################################
_real = {}
_price = {}
for y in years:
    try:
        _real[y] = beaapi.get_data(beakey, datasetname='NIPA', TableName='T20403', Frequency='Q', Year=y)
        _price[y] = beaapi.get_data(beakey, datasetname='NIPA', TableName='T20404', Frequency='Q', Year=y)
    except:
        print(f'Year {y} is not available')

Qreal = pd.concat(_real.values(), axis=0).sort_values(['SeriesCode', 'TimePeriod'])
Qprice = pd.concat(_price.values(), axis=0).sort_values(['SeriesCode', 'TimePeriod'])

for name, lag in { 'QoQ': 1, 'YoY': 4}.items():
    Qreal['pct_chg'] = Qreal.groupby(['SeriesCode'])['DataValue'].pct_change(lag)
    rows = Qreal['SeriesCode'].isin(lvl3)
    df = Qreal[rows].copy()
    df['LineDescription'] = df['LineDescription'].str[:20]
    df['rank'] = df.groupby('TimePeriod')['pct_chg'].transform(lambda x: x.rank())
    df2 = df.groupby('SeriesCode').tail(6).pivot(index='TimePeriod', columns="LineDescription", values='rank')
    ax = df2.plot(style='--^',  ax=plt.gca(), figsize=[9.35,14.1], legend=False, title=f'Real Spending: {name}')
    ###
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)

    ax.set_xlabel('')
    ax.set_xticks(range(len(df2.index)))
    ax.set_xticklabels(df2.index)
    ####
    lines = ax.get_lines()
    for line in lines:
        x, y = line.get_data()
        last_x, last_y = x[-1], y[-1]
        ax.text(last_x+0.1, last_y, line.get_label(), verticalalignment='center')
    plt.xticks(rotation=0)
    plt.tick_params(left=False, labelleft=False)
    plt.tight_layout()
    plt.savefig(f'02.Signals\econ_PCER_details_{name}.pdf', bbox_inches='tight')  # Save the plot as a PDF file
    plt.show()


Qprice['pct_chg'] = Qprice.groupby(['SeriesCode'])['DataValue'].pct_change(4)
lvl3 = [ x[:-1] + 'G' for x in lvl3]
rows = Qprice['SeriesCode'].isin(lvl3)
df = Qprice[rows].copy()
df['LineDescription'] = df['LineDescription'].str[:20]
df['rank'] = df.groupby('TimePeriod')['pct_chg'].transform(lambda x: x.rank())
df2 = df.groupby('SeriesCode').tail(6).pivot(index='TimePeriod', columns="LineDescription", values='rank')
ax = df2.plot(style='--^',  ax=plt.gca(), figsize=[9.35,14.1], legend=False, title=f'PCEPI: {name}')
###
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(False)

ax.set_xlabel('')
ax.set_xticks(range(len(df2.index)))
ax.set_xticklabels(df2.index)
####
lines = ax.get_lines()
for line in lines:
    x, y = line.get_data()
    last_x, last_y = x[-1], y[-1]
    ax.text(last_x+0.1, last_y, line.get_label(), verticalalignment='center')
plt.xticks(rotation=0)
plt.tick_params(left=False, labelleft=False)
plt.tight_layout()
plt.savefig(f'02.Signals\econ_PCEPI_details_{name}.pdf', bbox_inches='tight')  # Save the plot as a PDF file
plt.show()

