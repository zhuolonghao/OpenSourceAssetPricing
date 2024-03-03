date = '202402'

import pandas as pd
import numpy as np
from fredapi import Fred
from functools import reduce
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

fred = Fred(api_key='beecf55d03e9987f682fa20b249f9975')

#######################################################################
# Page 1: Cover page - overview
#######################################################################
_econs = {
    'sp500': ['SP500', 'mompct'],
    'UE': ['UNRATE', None],
    'CPI.headline': ['CPIAUCSL', 'yoypct'],
    'PCE.headline': ['PCEPI', 'yoypct'],
    'UMich.Exp': ['MICH', None],
    'CPI.core': ['CPILFESL', 'yoypct'],
    'PCE.core': ['PCEPILFE', 'yoypct'],
    '10y.UST': ['DGS10', None],
    '3m.UST': ['DGS3MO', None],
    '3m.Fwd_3m': [['DGS6MO', 'DGS3MO'], 'forward'],
    'FFT': ['EFFR', None],
    'FOMC.UST': ['TREAST', None],
    'FOMC.MBS': ['WSHOMCB', None],
    'M2': ['M2SL', None],
    'M1': ['M1SL', None],
}
_dict = {}
for name, [series, variant] in _econs.items():

    if isinstance(series, list):
        series_list = [fred.get_series(x, observation_start='2010-01-01') for x in series]
        df = pd.concat(series_list, axis=1)
        df.columns = series
        df = df.reset_index()
    else:
        df = fred.get_series(series, observation_start='2010-01-01').rename(name).reset_index()

    df['date_tmp'] = pd.to_datetime(df['index'])
    df = df.groupby(df['date_tmp'].dt.to_period("M")).last()
    df['date'] = df['date_tmp'].dt.strftime("%Y-%m")

    if variant == 'mompct':
        df[name] = df[name].pct_change(periods=1) * 100
        df[name] = df[name].transform(lambda x: f"{x:.2f}")
    elif variant == 'yoypct':
        df[name] = df[name].pct_change(periods=12) * 100
        df[name] = df[name].transform(lambda x: f"{x:.2f}")
    elif name == '3m.Fwd_3m': #https://shorturl.at/ijQ09
        df[name] = 100*((((1+df[series[0]]/100)**.5)/(1+df[series[1]]/100)**.25)**(1/.25)-1)
        df[name] = df[name].transform(lambda x: f"{x:.2f}")
    elif 'FOMC' in name:
        df[name] = df[name].transform(lambda x: f"${x/1e3:,.0f}")
    elif name in ['M1', 'M2']:
        df[name] = df[name].transform(lambda x: f"${x:,.0f}")
    _dict[name] = df[['date', name]]
econ = reduce(lambda left, right: pd.merge(left, right, on='date', how='left'), _dict.values())
econ = econ.sort_values('date', ascending=False).set_index('date').head(13).T
econ.replace(np.nan, "-", inplace=True)


charts = {
    'Category': [
        'Labor',
        '',
        '',
        'Price',
        '',
        'Policy',
        ''
    ],
    'Topics': [
        'Labor Recession Indicator',
        'Labor Price vs Productivity',
        'Labor Supply vs Demand',
        '3M forward rate_3M ahead',
        'Inflation vs Expectation',
        'Fiscal Policy: US Debt',
        'Monetary Policy: FFT + FOMC'],
    'Link': [
        'https://fred.stlouisfed.org/graph/?g=1e9Bf',
        'https://fred.stlouisfed.org/graph/?g=1e9B3',
        'https://fred.stlouisfed.org/graph/?g=1e9AI',
        'https://fred.stlouisfed.org/graph/?g=1eath',
        'https://fred.stlouisfed.org/graph/?g=1e9xu',
        'https://fred.stlouisfed.org/graph/?g=1e9wm',
        'https://fred.stlouisfed.org/graph/?g=1e9wD',],
    # 'Fred Blog':[
    #     'None',
    #     'https://fredblog.stlouisfed.org/2023/03/when-comparing-wages-and-worker-productivity-the-price-measure-matters/',
    #     'https://fredblog.stlouisfed.org/2023/02/are-labor-supply-and-labor-demand-out-of-balance/',
    #     'https://fredblog.stlouisfed.org/2023/05/constructing-forward-interest-rates-in-fred/',
    #     'https://fredblog.stlouisfed.org/2021/03/are-we-expecting-too-much-inflation/',
    #     'https://fredblog.stlouisfed.org/2019/12/a-lesson-in-measuring-the-federal-debt/',
    #     'https://fredblog.stlouisfed.org/2023/08/explaining-the-feds-recent-conventional-and-unconventional-monetary-policy/',
    # ]
}
charts = pd.DataFrame(charts)
#######################################################################
# Printing
#######################################################################
with PdfPages(fr'.\02.Signals\Reporting\econ_{date}.pdf', keep_empty=False) as pdf:

    # Set letter size (8.5 x 11 inches) for each page
    fig, axes = plt.subplots(figsize=(8.5, 11), nrows=2, ncols=1)
    # Creating a table from the DataFrame
    axes[0].axis('off')
    axes[0].set_title('Recent 12 Months', fontsize=12)
    table = axes[0].table(cellText=econ.reset_index().values, colLabels=['Econ', *econ.columns], cellLoc='right', loc='upper center')
    # Bold the column index names
    for (i, key), cell in table.get_celld().items():
        if i == 0:  # Bold the header row
            cell.set_text_props(fontweight='bold')
        if i != 0 and key == 0:  # Bold the first column (row index)
            cell.set_text_props(fontweight='bold')
    table.auto_set_column_width(col=range(2))
    table.auto_set_font_size(False)
    table.set_fontsize(7)
    table.scale(xscale=1.5, yscale=1)

    axes[1].axis('off')
    axes[1].set_title('Historical Trends', fontsize=12)
    table2 = axes[1].table(cellText=charts.values, colLabels=charts.columns, cellLoc='left', loc='upper center')
    # Bold the column index names
    for (i, key), cell in table2.get_celld().items():
        if i == 0:  # Bold the header row
            cell.set_text_props(fontweight='bold')
    table2.auto_set_column_width(col=range(3))
    table2.auto_set_font_size(False)
    table2.set_fontsize(8)
    table2.scale(xscale=1.5, yscale=1.2)

    plt.suptitle(f'Cover page for economic analysis', fontsize=14, fontweight='bold', x=0.01, ha='left')
    plt.tight_layout()
    pdf.savefig()

