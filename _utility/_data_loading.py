
def long_2_wide(df):
    df1 = pd.DataFrame()
    for t in df['signalname'].unique():
        try:
            tmp = df[df['signalname'].eq(t)]
            which_port = tmp['port'].unique()[-2]  # sign has been considered when creating portfolios.
            rows = tmp['port'].eq(which_port)
            df1 = pd.concat([df1, tmp[rows]], ignore_index=True)
        except:
            print(fr"{t} is not found in database {x} post 2015")
    return df1