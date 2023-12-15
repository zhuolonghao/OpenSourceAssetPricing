import pandas as pd
import numpy as np
exec(open('_utility/_binning.py').read())

SignalDoc = pd.read_csv(r'./01.Assessment/SignalDoc.csv')
cols = ['Acronym', 'Authors', 'Year', 'LongDescription', 'Sign', 'Stock Weight', 'LS Quantile', 'Portfolio Period', 'Quantile Filter','Filter']
rows = 'Coskewness'
SignalDoc[SignalDoc.Acronym.eq(rows)][cols].T
print(SignalDoc[SignalDoc.Acronym.eq(rows)]["Detailed Definition"].values)


base = pd.read_parquet('02.Signals/Data/price_monthly.parquet')
base.columns = [x.lower() for x in base.columns]
base['date_ymd'] = base['date_raw'].dt.strftime("%Y%m%d")
base['date_ym'] = base['date_ymd'].str[:6]
base = base.sort_values(by=['ticker', 'date_raw'])

ff = pd.read_excel('02.Signals/Data/F-F_Research_Data_Factors.xlsx', sheet_name='reformatted')
ff.columns = [x.lower() for x in ff.columns]
ff['date_ym'] = ff['date_ym'].astype(str)
ff[['mkt-rf', 'smb', 'hml', 'rf']] = ff[['mkt-rf', 'smb', 'hml', 'rf']]/100

exec(open('02.Signals/Code/Signals/MomRev.py').read())
MomRev()
exec(open('02.Signals/Code/Signals/MomFirmAge.py').read())
MomFirmAge()
exec(open('02.Signals/Code/Signals/MomVol.py').read())
MomVol()
