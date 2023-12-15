import pandas as pd
import numpy as np
exec(open('_utility/_binning.py').read())


fin = pd.read_parquet('02.Signals/Data/fin.parquet')
fin.columns = [x.lower() for x in fin.columns]
fin['date_ymd'] = fin['date_raw'].dt.strftime("%Y%m%d")
fin['date_ym'] = fin['date_ymd'].str[:6]
fin = fin.sort_values(by=['ticker', 'date_raw'])

finQ = pd.read_parquet('02.Signals/Data/finQ.parquet')
finQ.columns = [x.lower() for x in finQ.columns]
finQ['date_ymd'] = finQ['date_raw'].dt.strftime("%Y%m%d")
finQ['date_ym'] = finQ['date_ymd'].str[:6]
finQ = finQ.sort_values(by=['ticker', 'date_raw'])


base = pd.read_parquet('02.Signals/Data/price_monthly.parquet')
base.columns = [x.lower() for x in base.columns]
base['date_ymd'] = base['date_raw'].dt.strftime("%Y%m%d")
base['date_ym'] = base['date_ymd'].str[:6]
base = base.sort_values(by=['ticker', 'date_raw'])

ref = pd.read_excel("./_data/_total_gics_style.xlsx")

fin = fin.merge(
    base[['ticker', 'date_ym', 'close']], how='left', left_on=['ticker', 'date_ym'], right_on=['ticker', 'date_ym']).merge(
    ref, how='left', left_on='ticker', right_on='ticker').sort_values(by=['ticker', 'date_raw'])
fin.groupby('date_ym')['ticker'].nunique()

finQ = finQ.merge(
    base[['ticker', 'date_ym', 'close']], how='left', left_on=['ticker', 'date_ym'], right_on=['ticker', 'date_ym']).merge(
    ref, how='left', left_on='ticker', right_on='ticker').sort_values(by=['ticker', 'date_raw'])
finQ.groupby('date_ym')['ticker'].nunique()

del base, ref

ff = pd.read_excel('02.Signals/Data/F-F_Research_Data_Factors.xlsx', sheet_name='reformatted')
ff.columns = [x.lower() for x in ff.columns]
ff['date_ym'] = ff['date_ym'].astype(str)
ff[['mkt-rf', 'smb', 'hml', 'rf']] = ff[['mkt-rf', 'smb', 'hml', 'rf']]/100




SignalDoc = pd.read_csv(r'./01.Assessment/SignalDoc.csv')
cols = ['Acronym', 'Authors', 'Year', 'LongDescription', 'Sign', 'Stock Weight', 'LS Quantile', 'Portfolio Period', 'Start Month','Quantile Filter','Filter']
rows = 'cfp'
SignalDoc[SignalDoc.Acronym.eq(rows)][cols].T
print(SignalDoc[SignalDoc.Acronym.eq(rows)]["Detailed Definition"].values)
