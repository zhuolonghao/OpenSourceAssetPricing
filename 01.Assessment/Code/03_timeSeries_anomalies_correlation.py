################################
# The code summarizes anomalies/risk factors prepared by open source asset pricing website.
################################
import os
import yfinance as yf #yf.__version__=='0.2.32'
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

exec(open("_utility/_data_loading.py").read())

SignalDoc = pd.read_csv(r'./01.Assessment/SignalDoc.csv')
cols = ['Acronym', 'Cat.Data', 'Cat.Economic', 'Sign', 'Portfolio Period', 'Start Month', 'Filter', 'LS Quantile']
SignalDoc = SignalDoc[cols]

benchmark = yf.download('spy', period='10y', interval='1mo').reset_index()
benchmark['ret_spy'] = benchmark['Close'].pct_change(1)*100
benchmark['date'] = benchmark['Date'].dt.strftime('%Y-%m')

df = pd.read_csv("./01.Assessment/Portfolios/PredictorPortsFull.csv")

df1 = long_2_wide(df)
_base = df1[df1.date.ge("2015-01-01")]\
    .assign(date=df1.date.str[:7])\
    .merge(benchmark, how='left', left_on='date', right_on='date')

_base = _base.assign(ret_ex=_base['ret']-_base['ret_spy'])\
    .pivot(index='date', columns='signalname', values='ret_ex')\
    .dropna(axis=1)
_colnames = _base.columns

_signals = {
    "Price": ['MomRev', 'FirmAgeMom', 'MomVol', 'Mom6m', 'Mom6mJunk', 'ResidualMomentum', 'IntMom'],
    "Price2": ['MomSeason06YrPlus', 'MomOffSeason06YrPlus','Mom12mOffSeason', 'MomSeason16YrPlus', 'MomOffSeason16YrPlus', 'MomOffSeason11YrPlus', 'MomSeason11YrPlus', 'MomOffSeason'],
    "Price_others": ['Coskewness', 'betaVIX', 'MaxRet'],
    "Volume": ['ShareVol', 'VolSD', 'MomVol', 'RIO_Volatility', 'RealizedVol'],
    "Credit": ['CredRatDG', 'OScore', 'Mom6mJunk', 'FailureProbability'],
    "ShortInterest": ['ShortInterest', 'Recomm_ShortInterest', 'IO_ShortInterest'],
    "Profitability": ["CBOperProf", "OperProfRD", "GP"],
    "Asset_Composition": ["tang", "Cash", "realestate"],
    "Valuation": ['BM', 'AccrualsBM', 'EntMult', 'cfp', 'EBM', 'CF', 'DivYieldST', 'Frontier'],  # valuation is interesting.
    "Accouting_others": ['DelDRC', 'RDAbility', 'OrderBacklogChg'],
    "13F": ['DelBreadth', 'RIO_Volatility', 'RIO_MB', ],
    "Reversals": ["IntanCFP", "IntanEP", "IntanBM", "IntanSP"],
    "Others": ['CustomerMomentum', 'sinAlgo', 'Spinoff', 'ChNAnalyst']
}
_target =  sum(_signals.values(), [])

from statsmodels.graphics.tsaplots import plot_acf
for x in _target:
    try:
        plot_acf(_base[x], title=f"{x}")
        plt.show()
    except Exception as e:  # work on python 3.x
        print(e)
