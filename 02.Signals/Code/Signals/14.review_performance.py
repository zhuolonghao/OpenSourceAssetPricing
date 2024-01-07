date = '202312'
eval_window = ['20231201', '20231231']

import pandas as pd
from pandas.tseries.offsets import BMonthBegin, BMonthEnd
exec(open('_utility/_anomaly_portfolio.py').read())
exec(open('_utility/_data_loading.py').read())

writer = pd.ExcelWriter(fr'.\02.Signals\rankings_{date}.xlsx')
###########################################################
# Read data
###########################################################
base = pd.read_parquet('02.Signals/Data/price.parquet')
base = normalize_date(df=base, from_to=eval_window)
base['ret'] = 1 + base.groupby('ticker')['close'].pct_change()
base['cum_ret'] = base.groupby('ticker')['ret'].cumprod()
base['assess_date'] = base.groupby('ticker')['date_ymd'].transform(lambda x: f"{min(x)}-{max(x)}")
base['trade_price'] = base.groupby('ticker')['closed'].transform('last')
ret = base.groupby('ticker').tail(1)

df = pd.read_excel(fr'.\02.Signals\{date}.xlsx')
dims = df.columns[:18].tolist()
cat = []
anomalies = []
for c, a in _anomalies.items():
    cat.append(c)
    anomalies.extend(a)
    df[a] = df[a].apply(lambda x: x == max(x) if max(x) > 0 else None)
    df[c] = df[a].sum(axis=1)
df['selected'] = df[cat].sum(axis=1)
df['selected, wgt'] = df[cat].gt(0).sum(axis=1)

df = df.merge(ret[['ticker', 'trade_price', 'assess_date', 'cum_ret']], how='left', on='ticker')
rows = (df[['mega_k', 'mega_v', 'lg_k', 'lg_v', 'mid_k', 'mid_v', 'small_k', 'small_v']] < 0).all(axis=1)
dfs = {"non_micro": df[~rows].set_index(dims), "micro": df[rows].set_index(dims)}

###########################################################
# Analysis
###########################################################
cols = ['trade_price', 'assess_date', 'cum_ret', 'selected', 'selected, wgt',
        'momentum', 'seasonality', 'reversal', 'valuation', 'accruals', 'profitability', 'investment', 'asset_comp', '13F'] + anomalies
for p, df in dfs.items():
    df[cols].to_excel(writer, sheet_name=f"{p}_tickers")

    with open(fr'.\02.Signals\Reporting\{p}_{date}.html', 'w') as file:
        df_zero = pd.DataFrame()
        file.write('<html>\n<body>\n')
        # Write first HTML table
        file.write(f'<h2>{p} Summary: one anomaly, one vote </h2>\n')
        out = df.groupby('selected')['cum_ret'].describe(percentiles=[.5])
        df_zero = pd.concat([df_zero, out.head(1).assign(Category=out.index.name)], ignore_index=True)
        file.write(format_df(out).to_html())
        # Write second HTML table
        file.write(f'<h2>{p} Summary: one group, one vote  </h2>\n')
        out = df.groupby('selected, wgt')['cum_ret'].describe(percentiles=[.5])
        df_zero = pd.concat([df_zero, out.head(1).assign(Category=out.index.name)], ignore_index=True)
        file.write(format_df(out).to_html())
        # Write mutliple HTML tables
        for i, c in enumerate(cat):
            file.write(f'<h2>{p}---{c} </h2>\n')
            out = df.groupby(c)['cum_ret'].describe(percentiles=[.5])
            df_zero = pd.concat([df_zero, out.head(1).assign(Category=out.index.name)], ignore_index=True)
            file.write(format_df(out).to_html())
        # Write last HTML tables
        file.write(f'<h2> Those non-selected: good anomaly yields lower return </h2>\n')
        df_zero = df_zero.set_index('Category').sort_values('mean')
        file.write(format_df(df_zero).to_html())
        file.write('</body>\n</html>')


    with open(fr'.\02.Signals\Reporting\{p}_individuals_{date}.html', 'w') as file:
        df_zero = pd.DataFrame()
        for c, a in _anomalies.items():
            for a1 in a:
                file.write(f'<h2>{p}---{c}---{a1} </h2>\n')
                out = df.groupby(a1)['cum_ret'].describe(percentiles=[.5])
                df_zero = pd.concat([df_zero, out.head(1).assign(Category=out.index.name)], ignore_index=True)
                file.write(format_df(out).to_html())
        # Write last HTML tables
        file.write(f'<h2> Those non-selected: good anomaly yields lower return </h2>\n')
        df_zero = df_zero.set_index('Category').sort_values('mean')
        file.write(format_df(df_zero).to_html())
        file.write('</body>\n</html>')

writer.close()
