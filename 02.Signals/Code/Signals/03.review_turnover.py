curr_date, prev_date = '202405', '202404'
import pandas as pd
exec(open('_utility/_anomaly_portfolio.py').read())
exec(open('_utility/_data_loading.py').read())

writer = pd.ExcelWriter(fr'.\02.Signals\Reporting\turnover_{curr_date}.xlsx')
print(fr"Process-1: review the portfolio turnover: {curr_date} vs {prev_date} and save in .\02.Signals\turnover_{curr_date}.xlsx")
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
cols = _size_style
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
    for port, conditions_dict in _portfolios.items():
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

# Process-1: review the portfolio turnover: 202404 vs 202403 and save in .\02.Signals\turnover_202404.xlsx
#     non_micro: reporting turnover under each anomaly
#         format is like size - anomaly: # in curr, # in prev, % of new tickers in curr
#
#     non_micro - MomRev_StMom: 5 - 3 - 100.00%
#     non_micro - MomInt_StMom: 2 - 6 - 83.33%
#     non_micro - high_BM_Low_Accruals: 7 - 11 - 90.91%
#     non_micro - Mom_in_NYSE: 25 - 15 - 20.00%
#     non_micro - Mfg_in_focus: 55 - 57 - 10.53%
#     non_micro - Short_Squeeze: 5 - 5 - 20.00%
#     micro: reporting turnover under each anomaly
#         format is like size - anomaly: # in curr, # in prev, % of new tickers in curr
#
#     micro - MomRev_StMom: 13 - 28 - 92.86%
#     micro - MomInt_StMom: 8 - 12 - 83.33%
#     micro - high_BM_Low_Accruals: 7 - 69 - 98.55%
#     micro - Mom_in_NYSE: 1 - 1 - 0.00%
#     micro - Mfg_in_focus: 103 - 99 - 4.04%
#     micro - Short_Squeeze: New

