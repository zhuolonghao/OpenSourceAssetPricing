# Instruction: change the date
date = '202403'
date_prev = '202402'

import pandas as pd

writer = pd.ExcelWriter(fr".\02.Signals\Reporting\validate_{date}.xlsx")
###########################################################
# Read data
###########################################################
print('Process-1: Compare the missing anomalies in current against the previous')

print('Process-1a: load the common attributes such as industry, size, style and anomaly names')
exec(open('_utility/_anomaly_portfolio.py').read()) # size_style
size_style = _size_style
index = _dimension
cols = _anomaly

print('Process-1b: bifurcate the anomalies by micro companies vs non-micro companies')
base = pd.read_excel(fr'.\02.Signals\{date}.xlsx')
rows = (base[size_style] < 0).all(axis=1)
dfs_by_micro = {"non_micro": base[~rows].set_index(index)[cols], "micro": base[rows].set_index(index)[cols]}

for p, df in dfs_by_micro.items():
    dfs = {}
    dfs['Total'] = df.apply(lambda x: ~x.isna())
    dfs['Buying'] = df.apply(lambda x: x == max(x) if max(x) > 0 else None)
    dfs['Non-buying'] = df.apply(lambda x: x >= 0)
    dfs['Invalid: total'] = df.apply(lambda x: x < 0 )
    dfs['Invalid: input missing'] = df.apply(lambda x: x == -999)
    dfs['Invalid: output missing'] = df.apply(lambda x: x == -998)


    # Summarize
    output = [df.sum().rename(col) for col, df in dfs.items()]
    curr = pd.concat(output, axis=1)
    curr.sort_values('Buying').to_excel(writer, sheet_name=f"{p}")
    # Add printouts to compare with previous records
    prev = pd.read_excel(fr'.\02.Signals\Reporting\validate_{date_prev}.xlsx', sheet_name=f"{p}", engine='openpyxl').set_index('Unnamed: 0')
    compare = curr.merge(prev, how='left', left_index=True, right_index=True)
    compare['diff'] = compare['Invalid: total_x'] - compare['Invalid: total_y']

    print(compare.sort_values('diff', ascending=False)['diff'])
    print(f"Above shows changes in total invalid observations (curr - prev) for: {p} \n",
          f"    ideal case: close to 0 (invalid data remains stable) \n",
          f"    worse case: positive and big number (invalid data points increased compared to last pull)\n",
          f"\n\n ")
    # Summarize by key segmentation
    for d in ['exchange', 'sector']:
        output = [df.groupby(d).sum().melt(ignore_index=False).rename(columns={'variable': 'anomaly', 'value': col}).set_index('anomaly', append=True)
                  for col, df in dfs.items()]
        pd.concat(output, axis=1).to_excel(writer, sheet_name=f"{p}_{d}")
    # Summarize by Style
    outputs = {}
    for d in size_style:
        output = [df[df.index.get_level_values(d) > 0].sum().rename(col)for col, df in dfs.items()]
        outputs[d] = pd.concat(output, axis=1).assign(style=d)
    pd.concat(outputs.values(), axis=0).reset_index().sort_values(['index', 'style'])\
        .set_index(['style', 'index']).to_excel(writer, sheet_name=f"{p}_style")

writer.close()
# 202403 updated:
# MomFirmAge                 42
# MomResiduals6m             13
# MomResiduals12m             9
# MRreversal                  9
# MomRev                      8
# STreversal                  8
# Mom_m02_m11_pos             8
# Season_0205                 8
# Mom_1m                      8
# OffSeason_0610              8
# MomTurnover                 8
# Mom_12m                     7
# Mom_6m                      7
# Mom_3m                      7
# Mom_m02m11                  7
# MomInt                      7
# OffSeason_0205              7
# Mom_m02_m11                 7
# MomVol                      7
# EntMult_q                   6
# Season_1620                 6
# OffSeason_1620              6
# OffSeason_1115              5
# Season_0610                 5
# LRreversal                  5
# eps2p_q                     4
# Season_1115                 4
# roe_q                       3
# IO_ShortInterest_q          3
# BM_q                        2
# Recomm_ShortInterest_q      1
# ShortInterest_q             0
# sale2p_q                    0
# EBM_q                      -2
# roa_q                      -3
# AccrualsBM_q               -6
# free_cfp_q                -10
# cfp_q                     -10
# tang_q                    -18
# cash_q                    -19
# CF_q                      -26
# CBOperProfLagAT_alt_q     -54
# CBOperProfLagAT_q         -54
# OperProfRDLagAT_q         -54
# GPlag_q                   -54
# DelDRC_q                 -209
# ChInv_q                  -222
# Investment_q             -319
# Name: diff, dtype: int64
# Above shows changes in total invalid observations (curr - prev) for: non_micro
#      ideal case: close to 0 (invalid data remains stable)
#      worse case: positive and big number (invalid data points increased compared to last pull)
#
#
# STreversal                 10
# Mom_1m                     10
# Mom_12m                     9
# MomInt                      8
# Mom_6m                      8
# Mom_3m                      8
# OffSeason_0205              8
# MRreversal                  8
# Season_0205                 7
# eps2p_q                     7
# MomResiduals6m              6
# MomRev                      5
# MomTurnover                 5
# Mom_m02m11                  5
# MomFirmAge                  4
# sale2p_q                    2
# OffSeason_0610              2
# MomResiduals12m             1
# EntMult_q                   0
# Season_0610                 0
# BM_q                        0
# ShortInterest_q             0
# OffSeason_1115             -4
# Season_1620                -6
# LRreversal                 -6
# Mom_m02_m11_pos            -7
# OffSeason_1620             -7
# Season_1115                -7
# Mom_m02_m11                -8
# IO_ShortInterest_q        -11
# MomVol                    -13
# Recomm_ShortInterest_q    -13
# roe_q                     -14
# roa_q                     -17
# free_cfp_q                -17
# CF_q                      -18
# EBM_q                     -18
# cash_q                    -27
# tang_q                    -28
# CBOperProfLagAT_alt_q     -30
# CBOperProfLagAT_q         -30
# OperProfRDLagAT_q         -30
# GPlag_q                   -34
# AccrualsBM_q              -70
# cfp_q                    -106
# DelDRC_q                 -312
# ChInv_q                  -413
# Investment_q             -416
# Name: diff, dtype: int64
# Above shows changes in total invalid observations (curr - prev) for: micro
#      ideal case: close to 0 (invalid data remains stable)
#      worse case: positive and big number (invalid data points increased compared to last pull)
#
#
