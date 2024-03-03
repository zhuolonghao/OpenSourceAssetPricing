curr_date, prev_date = '202402', '202401'
import pandas as pd
exec(open('_utility/_anomaly_portfolio.py').read())
exec(open('_utility/_data_loading.py').read())

writer = pd.ExcelWriter(fr'.\02.Signals\turnover_{curr_date}.xlsx')

###########################################################
# Read data
###########################################################
df_curr = pd.read_excel(fr'.\02.Signals\{curr_date}.xlsx')
df_prev = pd.read_excel(fr'.\02.Signals\{prev_date}.xlsx')

cat = []
anomalies = []
for c, a in _anomalies.items():
    cat.append(c)
    anomalies.extend(a)
    df_curr[a] = df_curr[a].apply(lambda x: x == max(x) if max(x) > 0 else None)
    df_prev[a] = df_prev[a].apply(lambda x: x == max(x) if max(x) > 0 else None)
    df_curr[c] = df_curr[a].sum(axis=1)
    df_prev[c] = df_prev[a].sum(axis=1)

df_curr['selected'] = df_curr[cat].sum(axis=1)
df_prev['selected'] = df_prev[cat].sum(axis=1)
df_curr['selected, wgt'] = df_curr[cat].gt(0).sum(axis=1)
df_prev['selected, wgt'] = df_prev[cat].gt(0).sum(axis=1)

dims = ['ticker', 'holdings name', 'exchange', 'market', 'sector', 'industry group', 'industry', 'sub-industry']
cols = ['mega_growth', 'mega_value', 'large_growth', 'large_value', 'mid_growth', 'mid_value', 'small_growth', 'small_value']
rows = (df_curr[cols] < 0).all(axis=1)
dfs_curr = {"non_micro": df_curr[~rows].set_index(dims), "micro": df_curr[rows].set_index(dims)}
rows = (df_prev[cols] < 0).all(axis=1)
dfs_prev = {"non_micro": df_prev[~rows].set_index(dims), "micro": df_prev[rows].set_index(dims)}

###########################################################
# Analysis: only focus on the key anomalies and their combinations.
###########################################################


for size in ["non_micro", "micro"]:
    tmp_curr = dfs_curr[size]
    tmp_prev = dfs_prev[size]
    _outs = {}
    print(f"""
    {size}: reporting turnover under each anomaly
        format is like size - anomaly: # in curr, # in prev, % of new tickers in curr
    """)
    for port, conditions_dict in portfolios.items():
       tmp_curr_filter = tmp_curr.copy()
       tmp_prev_filter = tmp_prev.copy()
       for column, condition in conditions_dict.items():
           tmp_curr_filter = tmp_curr_filter[condition(tmp_curr_filter[column])]
           tmp_prev_filter = tmp_prev_filter[condition(tmp_prev_filter[column])]
       tmp_curr_filter[curr_date] = True
       tmp_prev_filter[prev_date] = True
       try:
           base = pd.DataFrame(tmp_curr_filter[curr_date]).merge(
               tmp_prev_filter[prev_date], how='outer', on = dims, suffixes=(f'_{curr_date}', f'_{prev_date}'))
           base.sort_values('industry').to_excel(writer, sheet_name=f"{port}_{size}"[:31])
           _outs[port] = base.assign(anomaly=port)
           n_curr = base[curr_date].sum()
           n_prev = base[prev_date].sum()
           n0 = sum(base.sum(axis=1) == 2.0)
           if n_prev > 0:
               print(f"    {size} - {port}: {n_curr:.0f} - {n_prev:.0f} - {(1 - n0 / n_prev) * 100:.2f}%")
           else:
               print(f"    {size} - {port}: New")
       except:
           print(f"    {size} - {port}: Error")
    pd.concat(_outs.values()).sort_values(['anomaly', 'industry']).to_excel(writer, sheet_name=f"summary_{size}")
writer.close()

# 202402
#
# micro: reporting turnover under each anomaly
# format is like size - anomaly:  # in curr, # in prev, % of new tickers in curr
#
# non_micro - selected_wgt_accruals_gt_0: 23 - 22 - 4.55 %
# non_micro - selected_wgt_val_gt_0: 60 - 57 - 24.56 %
# non_micro - selected_wgt_prof_gt_0: 65 - 65 - 23.08 %
# non_micro - selected_wgt_season_gt_2: 2 - 6 - 100.00 %
# non_micro - seasonality: 12 - 21 - 95.24 %
# non_micro - MomSeason: 22 - 16 - 25.00 %
# non_micro - MomRev: 35 - 29 - 55.17 %
# non_micro - MomTurnover: 44 - 20 - 80.00 %
# non_micro - MomInt: 157 - 137 - 25.55 %
# non_micro - IO_ShortInterest: 5 - 5 - 0.00 %
# non_micro - GPlag: 128 - 128 - 0.00 %
# non_micro - AccrualsBM: 5 - 5 - 0.00 %
#
# micro - selected_wgt_accruals_gt_0: 29 - 32 - 18.75 %
# micro - selected_wgt_val_gt_0: 171 - 165 - 18.79 %
# micro - selected_wgt_prof_gt_0: 96 - 100 - 21.00 %
# micro - selected_wgt_season_gt_2: 26 - 33 - 75.76 %
# micro - seasonality: 86 - 101 - 76.24 %
# micro - MomSeason: 5 - 8 - 75.00 %
# micro - MomRev: 139 - 113 - 25.66 %
# micro - MomTurnover: 64 - 55 - 80.00 %
# micro - MomInt: 208 - 226 - 34.07 %
# micro - IO_ShortInterest: New
# micro - GPlag: 127 - 127 - 0.00 %
# micro - AccrualsBM: 23 - 23 - 0.00 %