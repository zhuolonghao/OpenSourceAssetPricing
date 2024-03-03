import os, re, time, shutil
import webbrowser as web
import pandas as pd
import numpy as np
date = '202402'

###########################################################################################################
# GICS_8/Style/Size
###########################################################################################################
###########################################################################################################
###########################################################################################################
# Download etf
_etf = {
    'sector': 'https://institutional.vanguard.com/investments/product-details/fund/0970',
    'mega_growth': 'https://institutional.vanguard.com/investments/product-details/fund/3138',
    'mega_value': 'https://institutional.vanguard.com/investments/product-details/fund/3139',
    'large_growth': 'https://institutional.vanguard.com/investments/product-details/fund/0967',
    'large_value': 'https://institutional.vanguard.com/investments/product-details/fund/0966',
    'mid_growth': 'https://institutional.vanguard.com/investments/product-details/fund/0932',
    'mid_value': 'https://institutional.vanguard.com/investments/product-details/fund/0935',
    'small_growth': 'https://institutional.vanguard.com/investments/product-details/fund/0938',
    'small_value': 'https://institutional.vanguard.com/investments/product-details/fund/0937',
}
for etf, link in _etf.items():
    web.open(link)
    time.sleep(5)

###########################################################################################################
# Process the downloads and generate one file
_path_downloads = r"C:\Users\zlhte\Downloads"
downloads = [x for x in os.listdir(_path_downloads) if 'ProductDetailsHoldings' in x]
downloads.insert(0, downloads.pop())
writer = pd.ExcelWriter(rf".\_data\{date}.xlsx")
_csv = zip(_etf.keys(), downloads)
_dict = {}
for etf, csv in _csv:
    raw = pd.read_csv(f"{_path_downloads}\{csv}", skiprows=4, usecols=list(range(1, 10)), header=None)
    last_row = np.where(raw.iloc[:,0].isna())[0][0]
    df = pd.DataFrame(raw.values[1:last_row], columns=raw.iloc[0])
    df.to_excel(writer, sheet_name=f"{etf}", index=False)
    print(f"{etf}: {df.shape[0]}")
writer.close()


###########################################################################################################
# GICS_8, Size, Style
###########################################################################################################
###########################################################################################################
###########################################################################################################
reader = pd.ExcelFile(rf".\_data\{date}.xlsx")
_total = {}
for nm in reader.sheet_names:
    df = reader.parse(nm)
    if nm != 'sector':
        df = df.rename(columns={'% of fund*': nm})
        df = df[['SEDOL', f"{nm}"]]
    rows = df.SEDOL.str.contains('-')
    _total[nm] = df[~rows].set_index('SEDOL')
total = pd.concat(_total.values(), axis=1).reset_index()
total.columns = [x.lower() for x in total.columns]
total['symbol'] = total['ticker'].str.replace('\.', '', regex=True)
total['sector'] = total['sector'].str[:36]

###########################################################################################################
# GICS_2, 4, 6, and 8
###########################################################################################################
###########################################################################################################
###########################################################################################################
gics = pd.read_excel("./_data/GICS_Map_2023.xlsx", sheet_name='reformatted')
gics.columns = [x.lower() for x in gics.columns]
gics = gics.fillna(method='ffill')
gics = gics.applymap(lambda x: re.sub("[\(\[].*?[\)\]]", "", x).strip()[:36])


###########################################################################################################
# Exchange
###########################################################################################################
###########################################################################################################
###########################################################################################################
exchange = pd.read_csv("02.Signals/Data/equityshortinterest_20231001_20231130.csv", low_memory=False)
exchange = exchange.groupby('Symbol').head(1)
exchange['exchange'] = exchange['Market']
exchange['symbol'] = exchange['Symbol']
exchange = exchange[["symbol", "exchange"]].drop_duplicates()

###########################################################################################################
# Combined
###########################################################################################################
###########################################################################################################
###########################################################################################################
total_gics_style = total.rename(columns={'sector': 'sub-industry'}).merge(
    exchange, how='left', left_on='symbol', right_on='symbol').merge(
    gics, how='left', left_on='sub-industry', right_on='sub-industry')
total_gics_style['ticker'] = total_gics_style['ticker'].apply(lambda x: str(x).replace(".", "-"))
cols = ['ticker', 'holdings name', 'exchange', 'market',
        'sector', 'industry group', 'industry',  'sub-industry',
        'mega_growth', 'mega_value', 'large_growth', 'large_value', 'mid_growth', 'mid_value', 'small_growth', 'small_value']
total_gics_style[cols].to_excel("./_data/_total_gics_style.xlsx", index=False)


###########################################################################################################
# Clean up
###########################################################################################################
###########################################################################################################
###########################################################################################################
# Move and remove the downloads
_path_archive = rf".\_data\style"
for csv in downloads:
    shutil.copy(f"{_path_downloads}\{csv}", f"{_path_archive}\{csv}")
    os.remove(f"{_path_downloads}\{csv}")

