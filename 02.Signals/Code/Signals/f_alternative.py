def valuation_profitability(others, keep_all=False):
    var_base = ['ticker', 'beta', 'sector']
    # valuation
    var_bm = ['pricetobook']
    var_EntMult = ['enterprisetoebitda']
    var_cfp = ['operatingcashflow', 'marketcap']
    var_free_cfp = ['freecashflow', 'marketcap']
    var_pe = ['trailingpe', 'forwardpe']
    var_ps = ['pricetosalestrailing12months']
    # profitability
    var_profits = ['returnonassets', 'returnonequity']

    vars = list(set(var_base+var_bm+var_EntMult+var_cfp+var_free_cfp+var_ps+var_pe+var_profits))
    df = others[vars].copy()

    df['BM'] = 1 / df['pricetobook']
    df['earnp'] = 1 / df['trailingpe']
    df['salep'] = 1 / df['pricetosalestrailing12months']
    df['EntMult'] = -1 * df['enterprisetoebitda']
    df['cfp'] = df['operatingcashflow']/df['marketcap']
    df['free_cfp'] = df['freecashflow']/df['marketcap']
    df['roa'] = df['returnonassets']
    df['roe'] = df['returnonequity']
    rows = df['sector'].eq('Financial Services')
    df.loc[rows, ['roa', 'roe']] = None

    df['BM_q'] = df['BM'].transform(lambda x: _bin(x, 10)).astype(str)
    df['eps2p_q'] = df['earnp'].transform(lambda x: _bin(x, 10)).astype(str)
    df['sale2p_q'] = df['salep'].transform(lambda x: _bin(x, 10)).astype(str)
    df['EntMult_q'] = df['EntMult'].transform(lambda x: _bin(x, 10)).astype(str)
    df['cfp_q'] = df['cfp'].transform(lambda x: _bin(x, 5)).astype(str)
    df['free_cfp_q'] = df['free_cfp'].transform(lambda x: _bin(x, 5)).astype(str)

    df['roa_q'] = df['roa'].transform(lambda x: _bin(x, 10)).astype(str)
    df['roe_q'] = df['roe'].transform(lambda x: _bin(x, 10)).astype(str)

    print("Complete: metrics using Yahoo Metric ")

    if keep_all:
        return df
    else:
        return df[['ticker',
                   'BM_q', 'eps2p_q', 'sale2p_q', 'EntMult_q', 'cfp_q', 'free_cfp_q',
                   'roa_q', 'roe_q']]



