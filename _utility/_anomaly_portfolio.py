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
portfolios = {
    'selected_wgt_accruals_gt_0': {'selected, wgt': lambda x: x>3, 'accruals': lambda x: x>0},
    'selected_wgt_val_gt_0': {'selected, wgt': lambda x: x>3, 'valuation': lambda x: x>0},
    'selected_wgt_prof_gt_0': {'selected, wgt': lambda x: x>3, 'profitability': lambda x: x>0},
    'selected_wgt_season_gt_2': {'selected, wgt': lambda x: x > 3, 'seasonality': lambda x: x > 2},
    'seasonality': {'seasonality': lambda x: x>2},
    'MomSeason': {'Mom_m02_m11': lambda x: x==True, 'Mom_m02_m11_pos': lambda x: x==True},
    'MomRev': {'MomRev': lambda x: x==True},
    'MomTurnover': {'MomTurnover': lambda x: x==True},
    'MomInt': {'MomInt': lambda x: x==True},
    'IO_ShortInterest': {'IO_ShortInterest_q': lambda x: x==True},
    'GPlag': {'GPlag_q': lambda x: x==True},
    'AccrualsBM': {'AccrualsBM_q': lambda x: x==True},
}