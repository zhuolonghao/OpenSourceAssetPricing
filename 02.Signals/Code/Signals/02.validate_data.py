date = '202401'
date_prev = '202312'

import pandas as pd
writer = pd.ExcelWriter(fr'.\02.Signals\validate_{date}.xlsx')
###########################################################
# Read data
###########################################################
df = pd.read_excel(fr'.\02.Signals\{date}.xlsx')
dims = df.columns[:18].tolist()
rows = (df[['mega_k', 'mega_v', 'lg_k', 'lg_v', 'mid_k', 'mid_v', 'small_k', 'small_v']] < 0).all(axis=1)
dfs = {"non_micro": df[~rows].set_index(dims), "micro": df[rows].set_index(dims)}

for p, df in dfs.items():
    _dfs = {}
    _dfs['Total'] = df.apply(lambda x: ~x.isna())
    _dfs['Buying'] = df.apply(lambda x: x == max(x) if max(x) > 0 else None)
    _dfs['Non-buying'] = df.apply(lambda x: x >= 0)
    _dfs['Invalid: total'] = df.apply(lambda x: x < 0 )
    _dfs['Invalid: -999'] = df.apply(lambda x: x == -999)
    _dfs['Invalid: -998'] = df.apply(lambda x: x == -998)

    # Summarize
    output = [df.sum().rename(col) for col, df in _dfs.items()]
    curr = pd.concat(output, axis=1)
    curr.sort_values('Buying').to_excel(writer, sheet_name=f"{p}")
    # Add printouts to compare with previous records
    prev = pd.read_excel(fr'.\02.Signals\validate_{date_prev}.xlsx', sheet_name=f"{p}").set_index('Unnamed: 0')
    compare = curr.merge(prev, how='left', left_index=True, right_index=True)
    compare['diff'] = compare['Invalid: total_x'] - compare['Invalid: total_y']

    print(compare.sort_values('diff', ascending=False)['diff'])
    print(f"Above shows changes in total invalid observations for: {p} \n",
          f"    ideal case: close to 0 (invalid data remains stable) \n",
          f"    worse case: negative and big number (invalid data points increased compared to last pull)\n",
          f"\n\n ")
    # Summarize by key segmentation
    dims = ['exchange', 'sector']
    for d in dims:
        output = [df.groupby(d).sum().melt(ignore_index=False).rename(columns={'variable': 'anomaly', 'value': col}).set_index('anomaly', append=True)
                  for col, df in _dfs.items()]
        pd.concat(output, axis=1).to_excel(writer, sheet_name=f"{p}_{d}")
    # Summarize by Style
    _output = {}
    dims = ['mega_k', 'mega_v', 'lg_k', 'lg_v', 'mid_k', 'mid_v', 'small_k', 'small_v']
    for d in dims:
        output = [df[df.index.get_level_values(d) > 0].sum().rename(col)for col, df in _dfs.items()]
        _output[d] = pd.concat(output, axis=1).assign(style=d)
    pd.concat(_output.values(), axis=0).reset_index().sort_values(['index', 'style'])\
        .set_index(['style', 'index']).to_excel(writer, sheet_name=f"{p}_style")

writer.close()