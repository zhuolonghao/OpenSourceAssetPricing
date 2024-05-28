# Instruction: change the date
date = '202404'
date_prev = '202403'

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
# 202404 updated:
# Investment_q              757
# ChInv_q                   532
# DelDRC_q                  516
# cfp_q                     245
# free_cfp_q                139
# roa_q                      27
# AccrualsBM_q               26
# roe_q                      21
# EntMult_q                  15
# MomFirmAge                 10
# MomRev                      4
# STreversal                  4
# MomTurnover                 4
# Mom_1m                      4
# OffSeason_0205              3
# Mom_3m                      3
# MomInt                      3
# MRreversal                  3
# Mom_12m                     2
# eps2p_q                     2
# Mom_m02m11                  2
# BM_q                        1
# MomVol                      1
# Mom_6m                      1
# Mom_m02_m11                 1
# Mom_m02_m11_pos             1
# Season_0205                 1
# OffSeason_0610              1
# OffSeason_1620              0
# GPlag_q                     0
# sale2p_q                    0
# Season_1115                 0
# ShortInterest_q             0
# EBM_q                      -1
# tang_q                     -1
# IO_ShortInterest_q         -2
# Season_0610                -2
# cash_q                     -3
# OffSeason_1115             -4
# Season_1620                -4
# CF_q                       -4
# CBOperProfLagAT_alt_q      -5
# CBOperProfLagAT_q          -5
# OperProfRDLagAT_q          -5
# MomResiduals12m            -5
# LRreversal                 -6
# Recomm_ShortInterest_q    -13
# MomResiduals6m            -14
# Name: diff, dtype: int64
# Above shows changes in total invalid observations (curr - prev) for: non_micro
#      ideal case: close to 0 (invalid data remains stable)
#      worse case: positive and big number (invalid data points increased compared to last pull)
#
#
# <input>:50: FutureWarning: The default value of numeric_only in DataFrameGroupBy.sum is deprecated. In a future version, numeric_only will default to False. Either specify numeric_only or select only columns which should be valid for the function.
# <input>:50: FutureWarning: The default value of numeric_only in DataFrameGroupBy.sum is deprecated. In a future version, numeric_only will default to False. Either specify numeric_only or select only columns which should be valid for the function.
# Investment_q              537
# ChInv_q                   537
# DelDRC_q                  404
# cfp_q                     174
# AccrualsBM_q               86
# free_cfp_q                 28
# MomFirmAge                 28
# EBM_q                      23
# roe_q                      16
# BM_q                       15
# eps2p_q                    11
# MomRev                     10
# MomTurnover                10
# Mom_3m                     10
# Mom_12m                     7
# Mom_6m                      7
# Mom_m02m11                  7
# OffSeason_0205              7
# MomInt                      7
# MRreversal                  5
# STreversal                  5
# Mom_1m                      5
# EntMult_q                   5
# roa_q                       4
# sale2p_q                    4
# ShortInterest_q             3
# Season_0205                 3
# MomVol                      2
# OffSeason_1620              2
# Mom_m02_m11_pos             2
# Mom_m02_m11                 1
# Season_1620                 0
# cash_q                      0
# tang_q                      0
# IO_ShortInterest_q          0
# OperProfRDLagAT_q          -1
# GPlag_q                    -1
# OffSeason_0610             -1
# CBOperProfLagAT_q          -1
# CBOperProfLagAT_alt_q      -2
# Season_1115                -2
# CF_q                       -3
# MomResiduals12m            -5
# OffSeason_1115             -5
# LRreversal                -11
# Season_0610               -12
# MomResiduals6m            -26
# Recomm_ShortInterest_q    -32
# Name: diff, dtype: int64
# Above shows changes in total invalid observations (curr - prev) for: micro
#      ideal case: close to 0 (invalid data remains stable)
#      worse case: positive and big number (invalid data points increased compared to last pull)
#
#
