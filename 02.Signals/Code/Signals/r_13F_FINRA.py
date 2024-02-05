# FINRA: provides equity short interest for public firms
# 13F from SEC: provides the share held by institutional investors

# Regulatory strategy: buy stocks using signal from short sellers.

# ShortInterest: https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/ShortInterest.do
#   context: short-sellers position themselves in stocks with low fundamental-to-price ratios
#           b/c such firms have systematically lower future stock returns.
#   conclusion: do not buy firms with high short-interest.

# IO_ShortInterest: https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/IO_ShortInterest.do
#   conclusion: despite it's not recommended to buy firms with high short-interest,
#                   firms are more attractive if they're largely owned by institutional shareholders.

# Recomm_ShortInterest: https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/DelDRC.do
#   context: in some cases, analysts and trades have different view on public firms
#           analysts tend to positively recommend stocks with high growth, high accruals, and low book-to-market ratios
#           traders may sell such stocks based on their information advantage.
#   conclusion: do not buy firms that analysts recommends but trades short heavily.
#                      buy firms that analysts do not recommend but trade short lightly.

# others.groupby('exchange')['ticker', 'marketcap'].apply(lambda x: x.sort_values('marketcap').tail(10))
# rows = 'Recomm_ShortInterest'
# SignalDoc[SignalDoc.Acronym.eq(rows)][cols].T
# print(SignalDoc[SignalDoc.Acronym.eq(rows)]["Detailed Definition"].values)
# finQ[[x for x in finQ.columns if 'deferred' in x.lower()]].count()

def regulatory_reporting(others, keep_all=False):
    # 2024.2.5 fix bug due to the changes in data suppliers.
    others = others.assign(
        sharesoutstanding=others['sharesOutstanding'],
        sharesshort=others['sharesShort'],
        sharespercentsharesout=others['sharesPercentSharesOut'],
        recommendationmean=others['recommendationMean'],
    )
    var_base = ['ticker', 'beta']
    var_ShortInterest = [
        # sharesshort / sharesoutstanding
        'sharesshort', 'sharesoutstanding', 'sharespercentsharesout']
    var_IO_ShortInterest = [
        # screening by short interest, short by ownership
        'sharespercentsharesout', 'heldPercentInstitutions', 'exchange']
    var_Recomm_ShortInterest = [
        # joint sorting
        'sharespercentsharesout', 'recommendationmean']

    vars = list(set(var_base+var_ShortInterest+var_IO_ShortInterest+var_Recomm_ShortInterest))
    #rows = others.sector.ne('Financials')
    #df = others[rows][vars].copy()
    df = others[vars].copy()

    df['short_to_shares'] = df['sharesshort'] / df['sharesoutstanding']


    df['ShortInterest'] = -1 * df['short_to_shares']
    df['ShortInterest_q'] = df['ShortInterest'].transform(lambda x: _bin(x, 5)).astype(str)


    df['IO_ShortInterest'] = df['heldPercentInstitutions'].str[:-1].astype('float')
    df['ShortInterest_p99'] = df['short_to_shares'].quantile(0.99)
    df.loc[df['short_to_shares'] < df['ShortInterest_p99'], 'IO_ShortInterest'] = None
    df.loc[~df['exchange'].isin(['ASE', 'NYQ']), 'IO_ShortInterest'] = None
    df['IO_ShortInterest_q'] = df['IO_ShortInterest'].transform(lambda x: _bin(x, 3)).astype(str)

    df['ConsRecomm'] = 6 - df['recommendationmean']
    df['ConsRecomm_q'] = df['ConsRecomm'].transform(lambda x: _bin(x, 5)).astype(str)
    df['short_to_shares_q'] = df['short_to_shares'].transform(lambda x: _bin(x, 5)).astype(str)
    df['Recomm_ShortInterest_q'] = None
    df.loc[df['ConsRecomm_q'].eq('1.0')&df['short_to_shares_q'].eq('1.0'), 'Recomm_ShortInterest_q'] = 1
    df.loc[df['ConsRecomm_q'].eq('10.0')&df['short_to_shares_q'].eq('10.0'), 'Recomm_ShortInterest_q'] = 0

    print('Complete: 13F, short interest')

    if keep_all:
        return df
    else:
        return df[['ticker', 'ShortInterest_q', 'IO_ShortInterest_q', 'Recomm_ShortInterest_q']]

# df = regulatory_reporting(others)



