_size_style = ['mega_growth', 'mega_value', 'large_growth', 'large_value', 'mid_growth', 'mid_value', 'small_growth', 'small_value']
_dimension = ['ticker', 'holdings name', 'exchange', 'market', 'sector', 'industry group', 'industry', 'sub-industry', 'date_ym'] + _size_style
_anomaly = ['MomRev', 'MomFirmAge', 'MomVol', 'MomInt', 'MomResiduals6m',
       'MomResiduals12m', 'Mom_m02_m11', 'Mom_m02_m11_pos', 'Season_0205',
       'OffSeason_0205', 'Season_0610', 'OffSeason_0610', 'Season_1115',
       'OffSeason_1115', 'Season_1620', 'OffSeason_1620', 'STreversal',
       'MRreversal', 'LRreversal', 'MomTurnover', 'BM_q', 'EBM_q',
       'AccrualsBM_q', 'EntMult_q', 'CF_q', 'cfp_q', 'GPlag_q',
       'OperProfRDLagAT_q', 'CBOperProfLagAT_q', 'CBOperProfLagAT_alt_q',
       'ChInv_q', 'Investment_q', 'DelDRC_q', 'cash_q', 'tang_q',
       'ShortInterest_q', 'IO_ShortInterest_q', 'Recomm_ShortInterest_q']

_anomalies = {# this list is based on 1) the economic meaning, and 2) clustering analysis in 01.Assessment
    'momentum': ['MomTurnover', 'MomRev', 'MomFirmAge', 'MomVol', 'MomInt', 'MomResiduals6m', 'MomResiduals12m',
                 'Mom_m02_m11', 'Mom_m02_m11_pos'],
    'seasonality': ['Season_0205', 'OffSeason_0205', 'Season_0610', 'OffSeason_0610', 'Season_1115', 'OffSeason_1115',
                    'Season_1620', 'OffSeason_1620'],
    'reversal': ['STreversal', 'MRreversal', 'LRreversal'],
    'valuation': ['BM_q', 'EBM_q',  'EntMult_q', 'CF_q', 'cfp_q'],
    'accruals': ['AccrualsBM_q', 'ChInv_q'],
    'profitability': ['GPlag_q', 'OperProfRDLagAT_q', 'CBOperProfLagAT_q', 'CBOperProfLagAT_alt_q'],
    'investment': ['Investment_q', 'DelDRC_q'],
    'asset_comp': ['cash_q', 'tang_q'],
    '13F': ['ShortInterest_q', 'IO_ShortInterest_q', 'Recomm_ShortInterest_q'],
}
_portfolios = {
    'selected_wgt_accruals_gt_0': {'selected, wgt': lambda x: x>3, 'accruals': lambda x: x>0},
    'selected_wgt_val_gt_0': {'selected, wgt': lambda x: x>3, 'valuation': lambda x: x>0},
    'selected_wgt_prof_gt_0': {'selected, wgt': lambda x: x>3, 'profitability': lambda x: x>0},
    'selected_wgt_season_gt_2': {'selected, wgt': lambda x: x > 3, 'seasonality': lambda x: x > 0.24},
    'seasonality': {'seasonality': lambda x: x>0.24},
    'MomSeason': {'Mom_m02_m11': lambda x: x==True, 'Mom_m02_m11_pos': lambda x: x==True},
    'MomRev': {'MomRev': lambda x: x==True},
    'MomTurnover': {'MomTurnover': lambda x: x==True},
    'MomInt': {'MomInt': lambda x: x==True},
    'IO_ShortInterest': {'IO_ShortInterest_q': lambda x: x==True},
    'GPlag': {'GPlag_q': lambda x: x==True},
    'AccrualsBM': {'AccrualsBM_q': lambda x: x==True},
}

_etf_s2 = {
    'MGK': 'MG', 'VUG': 'LG','VOT': 'MidG',
    'MGV': 'MV', 'VTV': 'LV','VOE': 'MidV',
    'VBK': 'SG', 'VBR': 'SV','VOO': 'SPY'
}


_etf_s = {
    'XLE': 'Energy',
    'XLB': 'Material',
    'XLU': 'Utility',

    'FDIS': 'Consumer Staple',
    'FSTA': 'Consumer Discretionary',
    'FIDU': 'Industrial',

    'XLC': 'Communication',
    'XLK': 'I.T.',
    'XLV': 'Health Care',

    'XLF': 'Financials',
    'XLRE': 'Real Estate',

}