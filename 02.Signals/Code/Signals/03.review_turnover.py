curr_date, prev_date = '202312', '202311'
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
rows = (df_curr[['mega_k', 'mega_v', 'lg_k', 'lg_v', 'mid_k', 'mid_v', 'small_k', 'small_v']] < 0).all(axis=1)
dfs_curr = {"non_micro": df_curr[~rows].set_index(dims), "micro": df_curr[rows].set_index(dims)}
rows = (df_prev[['mega_k', 'mega_v', 'lg_k', 'lg_v', 'mid_k', 'mid_v', 'small_k', 'small_v']] < 0).all(axis=1)
dfs_prev = {"non_micro": df_prev[~rows].set_index(dims), "micro": df_prev[rows].set_index(dims)}

###########################################################
# Analysis: only focus on the key anomalies and their combinations.
###########################################################


for size in ["non_micro", "micro"]:
    tmp_curr = dfs_curr[size]
    tmp_prev = dfs_prev[size]
    _outs = {}
    print(f"{size}: reporting turnover")
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

