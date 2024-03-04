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
# Download etf holdings
writer = pd.ExcelWriter(rf".\_data\{date}.xlsx")

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
    time.sleep()


input("Checkpoint: Download ETF holdings manually. Press Enter once done")
print(f'Process-1: write all ETF holdings to the file .\_data\{date}.xlsx')
path_downloads = r"C:\Users\zlhte\Downloads"
downloads = [x for x in os.listdir(path_downloads) if 'ProductDetailsHoldings' in x]
downloads.insert(0, downloads.pop())
for etf, csv in zip(_etf.keys(), downloads):
    raw = pd.read_csv(f"{path_downloads}\{csv}", skiprows=4, usecols=list(range(1, 10)), header=None)
    last_row = np.where(raw.iloc[:,0].isna())[0][0]
    df = pd.DataFrame(raw.values[1:last_row], columns=raw.iloc[0]).drop_duplicates()
    df.to_excel(writer, sheet_name=f"{etf}", index=False)
    print(f"{etf}: {df.shape[0]}")
writer.close()
input("Checkpoint: Process-1 is done. # of holdings should be stable in each ETF. Press Enter to continue")

###########################################################################################################
# GICS_8, Size, Style
###########################################################################################################
###########################################################################################################
###########################################################################################################
print(f'Process-2: create metadata ./_data/_total_gics_style.xlsx that includes all tickers and their sector, '
      f'size and style info.')
reader = pd.ExcelFile(rf".\_data\{date}.xlsx")
_total = {}
print(f'    Process-2a: create a temp metadata that has GICS-8, size, and style')
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

print(f'    Process-2b: add GICS-2, 4, 6')
gics = pd.read_excel("./_data/GICS_Map_2023.xlsx", sheet_name='reformatted')
gics.columns = [x.lower() for x in gics.columns]
gics = gics.fillna(method='ffill')
gics = gics.applymap(lambda x: re.sub("[\(\[].*?[\)\]]", "", x).strip()[:36])

print(f'    Process-2c: add stock exchange')
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
input("Checkpoint: Process-2 is done. Press Enter to purge raw files in .\Downloads after archiving in .\_data\style")

###########################################################################################################
# Clean up
###########################################################################################################
###########################################################################################################
###########################################################################################################
# Move and remove the downloads
path_archive = rf".\_data\style"
for csv in downloads:
    shutil.copy(f"{path_downloads}\{csv}", f"{path_archive}\{csv}")
    os.remove(f"{path_downloads}\{csv}")

# sector: 3709
# mega_growth: 82
# mega_value: 141
# large_growth: 207
# large_value: 350
# mid_growth: 150
# mid_value: 200
# small_growth: 637
# small_value: 853