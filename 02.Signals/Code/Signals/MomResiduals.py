# https://github.com/OpenSourceAP/CrossSection/blob/master/Signals/Code/Predictors/ZZ1_ResidualMomentum6m_ResidualMomentum.do
# strategy: it's to show the momentum exists after controlling FF 3-factor models.
# it's less meaningful to guide portfolio construction...

from statsmodels.regression.rolling import RollingOLS
import statsmodels.api as sm
import pandas as pd
import ray
@ray.remote
def resid_rolling(df, window=36):
    """
    compute the residuals, defined as f(actual_t, fitted_t) and fitted_t is computed using beta_t(window)
    :param df: pd.DataFrame that contains time-series of a ticker's excess return and FF factors.
    :param window: integer that specifies the timeframe of regression.
    :return: idiosyncratic returns after taking out FF3 factors and its related alpha.
    https://stackoverflow.com/questions/72119449/rolling-ols-regressions-and-predictions-by-group
    """
    if df.shape[0] < window:
        return None

    y = df['ret_rf']
    x = sm.add_constant(df[['mkt-rf', 'smb', 'hml']])
    betas = RollingOLS(endog=y, exog=x, window=window, min_nobs=window).fit().params
    df = df.assign(residuals=y-(betas*x).sum(1, skipna=False)) # skipna=False ensures None returns none
    return df
# Testing
# tmp = df[df.ticker.eq('AR')]
# tmp = resid_rolling(tmp, window=36)

def MomResiduals(base=base, ff=ff, keep_all=False):
    df = base.merge(ff, how='inner', left_on='date_ym', right_on='date_ym')
    df['ret_rf'] = df.groupby('ticker')['close'].pct_change(1) - df['rf']
    _dfs = [resid_rolling.remote(ticker_df, 36) for ticker, ticker_df in df.groupby('ticker')]
    df = pd.concat(ray.get(_dfs), axis=0, ignore_index=True)
    df['l1.residuals'] = df.groupby('ticker')['residuals'].shift(1)

    df['resid6_mean'] = df.groupby('ticker')['l1.residuals'].transform(lambda x: x.rolling(6, 6).mean())
    df['resid6_std'] = df.groupby('ticker')['l1.residuals'].transform(lambda x: x.rolling(6, 6).std())
    df['MomResid6m'] = df['resid6_mean'] / df['resid6_std']
    df['resid11_mean'] =  df.groupby('ticker')['l1.residuals'].transform(lambda x: x.rolling(11, 11).mean())
    df['resid11_std'] = df.groupby('ticker')['l1.residuals'].transform(lambda x: x.rolling(11, 11).std())
    df['MomResid12m'] = df['resid11_mean'] / df['resid11_std']

    rows = (df.date_ym == ff.date_ym.max())
    cols = ['ticker', 'date_ym', 'close', 'MomResid6m', 'MomResid12m']
    df = df[rows][cols]

    df['MomResiduals6m'] = df.groupby('date_ym', group_keys=False)['MomResid6m'].apply(lambda x: _bin(x, 10)).astype(str)
    df['MomResiduals12m'] = df.groupby('date_ym', group_keys=False)['MomResid12m'].apply(lambda x: _bin(x, 10)).astype(str)

    # Fama-French dataset lags two months or more. Only keep the most recent

    if keep_all:
        return df[rows]
    else:
        print('Completed: MomResiduals')
        return df[rows][['ticker', 'MomResiduals6m', 'MomResiduals12m']]

#df = MomResiduals(base, ff)