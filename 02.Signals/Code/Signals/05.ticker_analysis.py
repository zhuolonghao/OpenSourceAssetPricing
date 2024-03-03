date = '202402'
focus = 'non_micro_tickers' # non_micro_tickers vs micro_tickers

import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
from pandas.tseries.offsets import BMonthBegin, BMonthEnd
exec(open('_utility/_anomaly_portfolio.py').read())
exec(open('_utility/_data_loading.py').read())
exec(open('_utility/_plotting.py').read())



df = pd.read_excel(fr'.\02.Signals\rankings_{date}.xlsx',  sheet_name=focus)
for key, value in _anomalies.items():
    df[key] = df[key] / len(value)
df_filter = df.reset_index()
###########################################################
# filtering based on portfolio conditions
###########################################################
for port, conditions_dict in portfolios.items():
    print(f'producing: {port}')
    pdf = PdfPages(f'{port}.pdf')
    for column, condition in conditions_dict.items():
        df_filter = df_filter[condition(df_filter[column])]
    ###########################################################
    # filtering based on portfolio conditions
    ###########################################################
    df_filter.sort_values(['sector', 'exchange'], inplace=True)
    # Number of rows in each subframe
    rows_per_subframe = 20
    # Calculate the number of subframes needed
    num_subframes = -(-len(df_filter) // rows_per_subframe)  # Ceiling division to get the number of subframes
    # Split the DataFrame into subframes
    subframes = np.array_split(df_filter, num_subframes)
    # Displaying the subframes
    for i, subframe in enumerate(subframes):
        print(f"producing charts for {port}")
        fig = plt.figure(figsize=(8.5, 11))
        for j in range(subframe.shape[0]):
            _ticker_chart(j + 1, subframe.iloc[j], sector_color, hexagon_vertices)
        plt.tight_layout()
        pdf.savefig(fig)
        plt.show()

    pdf.close()