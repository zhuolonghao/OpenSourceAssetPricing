# Instruction: change the date
date = '202402'
date_prev = '202401'

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
    prev = pd.read_excel(fr'.\02.Signals\validate_{date_prev}.xlsx', sheet_name=f"{p}", engine='openpyxl').set_index('Unnamed: 0')
    compare = curr.merge(prev, how='left', left_index=True, right_index=True)
    compare['diff'] = compare['Invalid: total_x'] - compare['Invalid: total_y']

    print(compare.sort_values('diff')['diff'])
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
# 202402 updated:
# cfp_q                      60
# BM_q                       60
# CBOperProfLagAT_alt_q      52
# CBOperProfLagAT_q          52
# OperProfRDLagAT_q          52
# GPlag_q                    52
# MomFirmAge                 26
# CF_q                       21
# cash_q                      9
# tang_q                      6
# EntMult_q                   5
# OffSeason_0205             -6
# MRreversal                 -6
# MomInt                     -6
# Season_0205                -7
# ShortInterest_q            -8
# STreversal                 -8
# MomRev                    -10
# MomTurnover               -10
# MomResiduals12m           -10
# LRreversal                -11
# OffSeason_1115            -13
# OffSeason_0610            -14
# MomResiduals6m            -15
# Season_1115               -16
# OffSeason_1620            -17
# Mom_m02_m11               -17
# Mom_m02_m11_pos           -17
# Season_0610               -17
# Season_1620               -17
# EBM_q                     -21
# Recomm_ShortInterest_q    -21
# IO_ShortInterest_q        -22
# MomVol                    -23
# AccrualsBM_q              -52
# DelDRC_q                 -520
# ChInv_q                  -633
# Investment_q             -865
# Name: diff, dtype: int64
# Above shows changes in total invalid observations for: non_micro
#      ideal case: close to 0 (invalid data remains stable)
#      worse case: negative and big number (invalid data points increased compared to last pull)
#
#
# GPlag_q                    18
# BM_q                       16
# cfp_q                      16
# CBOperProfLagAT_q          15
# OperProfRDLagAT_q          15
# CBOperProfLagAT_alt_q      14
# EntMult_q                   3
# tang_q                     -6
# CF_q                       -8
# cash_q                     -9
# IO_ShortInterest_q        -17
# MomVol                    -18
# Mom_m02_m11               -20
# Mom_m02_m11_pos           -22
# Season_0205               -22
# MRreversal                -23
# MomFirmAge                -23
# MomResiduals6m            -24
# OffSeason_0205            -25
# MomResiduals12m           -25
# MomInt                    -25
# OffSeason_1620            -25
# Recomm_ShortInterest_q    -26
# Season_1620               -26
# OffSeason_1115            -26
# Season_1115               -26
# OffSeason_0610            -26
# ShortInterest_q           -30
# Season_0610               -31
# EBM_q                     -33
# LRreversal                -34
# STreversal                -38
# MomRev                    -40
# MomTurnover               -40
# AccrualsBM_q              -53
# DelDRC_q                 -142
# ChInv_q                  -179
# Investment_q             -185
# Name: diff, dtype: int64
# Above shows changes in total invalid observations for: micro
#      ideal case: close to 0 (invalid data remains stable)
#      worse case: negative and big number (invalid data points increased compared to last pull)
