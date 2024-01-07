import pandas as pd

def normalize_date(df, from_to=[]):
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")
    if not isinstance(from_to, list):
        raise TypeError("window must be a list, like ['20231201', 20231231]")

    df.columns = [x.lower() for x in df.columns]
    df['date_ymd'] = df['date_raw'].dt.strftime("%Y%m%d")
    df['date_ym'] = df['date_ymd'].str[:6]
    if len(from_to) == 2:
        start, end = from_to[0], from_to[1]
        rows = df['date_ymd'].ge(start) & df['date_ymd'].le(end)
        df = df[rows]
    df2 = df.sort_values(by=['ticker', 'date_raw'])
    return df2

# Custom formatting function
format_dict = {
    'count': lambda x: '{:.0f}'.format(x),
    'mean': lambda x: '{:.2f}%'.format((x - 1) * 100),
    'std': lambda x: '{:.2f}%'.format(x),
    'min': lambda x: '{:.2f}%'.format((x - 1) * 100),
    '50%': lambda x: '{:.2f}%'.format((x - 1) * 100),
    'max': lambda x: '{:.2f}%'.format((x - 1) * 100),
}
def format_df(df, format_dict=format_dict):
    return df.style \
        .format(format_dict) \
        .set_properties(**{'background-color': 'lightgray'}, subset=pd.IndexSlice[::2, :]) \
        .set_properties(**{'width': '60px', 'text-align': 'right'}) \
        .set_table_styles([{'selector': 'table', 'props': [('width', '100%')]}]) \
        .set_table_attributes('border="1"')

