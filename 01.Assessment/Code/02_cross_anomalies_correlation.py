################################
# The code summarizes anomalies/risk factors prepared by open source asset pricing website.
################################
import os
import yfinance as yf #yf.__version__=='0.2.32'
import pandas as pd
import numpy as np
from sklearn.covariance import GraphicalLassoCV, ledoit_wolf
from scipy import linalg

exec(open("_utility/_segmentation.py").read())

SignalDoc = pd.read_csv(r'./01.Assessment/SignalDoc.csv')
cols = ['Acronym', 'Cat.Data', 'Cat.Economic', 'Sign', 'Portfolio Period', 'Start Month', 'Filter', 'LS Quantile']
SignalDoc = SignalDoc[cols]
print(SignalDoc.groupby('Cat.Data')['Acronym'].count())

df = pd.read_csv("./01.Assessment/Portfolios/PredictorPortsFull.csv")
df1 = pd.DataFrame()
for t in df['signalname'].unique():
    try:
        tmp = df[df['signalname'].eq(t)]
        which_port = tmp['port'].unique()[-2]  # sign has been considered when creating portfolios.
        rows = tmp['port'].eq(which_port)
        df1 = pd.concat([df1, tmp[rows]], ignore_index=True)
    except:
        print(fr"{t} is not found in database {x} post 2015")
del df
########################################################################
# different approach to compute correlation or covariance
# https://shorturl.at/hikyz
########################################################################

_base = df1[df1.date.ge("2015-01-01")]\
    .pivot(index='date', columns='signalname', values='ret')\
    .dropna(axis=1)
_colnames = _base.columns

# Empirical covariance
_base -= _base.mean(axis=0)
_base /= _base.std(axis=0)
_cov_emp = np.dot(_base.T, _base) / _base.shape[0]
_cor_emp = corr_from_cov(_cov_emp)

# covariance based on l1
model = GraphicalLassoCV()
model.fit(_base)
_cov_l1 = model.covariance_
_prec_l1 = model.precision_
_cor_l1 = corr_from_cov(_cov_l1)

# covariance based on l2
_cov_l2, _ = ledoit_wolf(_base)
_prec_l2 = linalg.inv(_cov_l2)
_cor_l2 = corr_from_cov(_cov_l2)


############################################################################################################
# Re-arrange correlation matrix into blocks/clusters such that they keeps similar anomalies closer
# https://shorturl.at/prQV4
# progressively pick the # of clusters // cannot do CV on unsupervised learning.
############################################################################################################
_coclustering = {
    'Accounting': 40, # too many. finished for now.
        # "Profitability": ["CBOperProf", "OperProfRD", "GP"],
        # "Asset_Composition": ["tang", "Cash", "realestate"],
        # "Valuation": ['BM', 'AccrualsBM', 'EntMult', 'cfp', 'EBM', 'CF', 'DivYieldST', 'Frontier'],
    '13F': 3, # finished. IO_shortinterest is distinct.
    'Analyst': 7, #finshed.  ChNAnalyst, Recomm_ShortInterest
    'Price': 10, # finished. MaxRet, Mom6mJunk, (BetaVIX, Coskewness), ResidualMomentum, IntMom, Seasonality, FirmAgeMom,
    'Other': 4, # finished, CustomerMomentum and sinAlgo are two distinct categories
    'Event': 5, # finished,  spinoff is distinct
    'Trading': 10, # unclear to me, but BidAskSpread appears distinct (rank consistently low)
    'Options': 5, # unclear to me.
}
for cat, n in _coclustering.items():
    signalnames = SignalDoc[SignalDoc['Cat.Data'].eq(cat)]['Acronym'].values
    idx = [i for i, col in enumerate(_colnames) if col in signalnames]
    _SpectralCoclustering(_cor_l2[idx][:,idx], n_clusters=n, title=cat, cols_nm=_colnames[idx])

np.where(_colnames=='sinAlgo')
_cor_tgt = _cor_l2[192]
_colnames[np.argsort(_cor_tgt)][-20:]

############################################################################################################
# Off-the-shelf clustering re-confirms the manual analysis above based on Co-clustering charting.
# https://shorturl.at/hklmR
############################################################################################################
_, labels = affinity_propagation(_cov_l2, random_state=0)
n_labels = labels.max()
for i in range(n_labels + 1):
    print(f"Cluster {i + 1}: {', '.join(_colnames[labels == i])}")

for cat, n in _coclustering.items():
    signalnames = SignalDoc[SignalDoc['Cat.Data'].eq(cat)]['Acronym'].values
    idx = [i for i, col in enumerate(_colnames) if col in signalnames]
    _, labels = affinity_propagation(_cor_l2[idx][:,idx], random_state=0)
    n_labels = labels.max()
    for i in range(n_labels + 1):
        print(f"Cluster {i + 1}: {', '.join(_colnames[idx][labels == i])}")
    print(f'Completed on {cat} \n')
#
# Cluster 1: ChNAnalyst
# Cluster 2: CustomerMomentum
# Cluster 3: DivSeason, DivYieldST, EquityDuration, FR, OptionVolume1, PS, PredictedFE, Recomm_ShortInterest, retConglomerate
# Cluster 4: AgeIPO, AnalystRevision, AnnouncementReturn, BPEBM, Beta, BetaFP, ChTax, ChangeInRecommendation, ConvDebt, DebtIssuance, DelDRC, DelNetFin, DivOmit, DownRecomm, ExchSwitch, FirmAge, GrLTNOA, GrSaleToGrInv, GrSaleToGrOverhead, IndIPO, IndMom, IndRetBig, MomSeason, OrderBacklogChg, RDIPO, RDS, REV6, RIO_Disp, RIO_Turnover, RIO_Volatility, RIVolSpread, RevenueSurprise, SmileSlope, TrendFactor, UpRecomm, dCPVolSpread, dVolCall, dVolPut, iomom_cust, iomom_supp, skew1
# Cluster 5: IO_ShortInterest
# Cluster 6: ConsRecomm, Coskewness, IdioVol3F, IdioVolAHT, RealizedVol, Spinoff
# Cluster 7: IntMom
# Cluster 8: AccrualsBM, AdExp, BMdec, EarnSupBig, Frontier, IntanSP, MRreversal, MomOffSeason, PayoutYield
# Cluster 9: ChEQ, ChInv, ChNNCOA, DelCOA, DelCOL, DelFINL, DolVol, InvGrowth, InvestPPEInv, MomOffSeason06YrPlus, MomSeasonShort, NetDebtFinance, OPLeverage, ResidualMomentum, ShareIss5Y, ShortInterest, dNoa, grcapx, grcapx3y, hire
# Cluster 10: DelBreadth, MS, RIO_MB, realestate
# Cluster 11: MaxRet
# Cluster 12: Mom12m, Mom12mOffSeason, Mom6m, MomRev, MomVol
# Cluster 13: Cash, Herf, NOA, NetDebtPrice, RD, tang
# Cluster 14: CBOperProf, GP, OperProfRD
# Cluster 15: RDAbility
# Cluster 16: BetaLiquidityPS, CPVolSpread, CompEquIss, CoskewACX, EarningsForecastDisparity, EarningsSurprise, FEPS, ForecastDispersion, High52, Investment, NetEquityFinance, NumEarnIncrease, OScore, OperProf, OrgCap, RoE, VarCF, roaq
# Cluster 17: AOP, Accruals, AnalystValue, BetaTailRisk, ChAssetTurnover, ChForecastAccrual, ChNWC, CompositeDebtIssuance, DelLTI, DivInit, EarningsConsistency, EarningsStreak, ExclExp, GrAdExp, MeanRankRevGrowth, MomOffSeason11YrPlus, MomSeason11YrPlus, OptionVolume2, OrderBacklog, PctAcc, PctTotAcc, ReturnSkew, ReturnSkew3F, ShareIss1Y, ShareRepurchase, Tax, VolumeTrend, XFIN, fgr5yrLag, sfe
# Cluster 18: AbnormalAccruals, AssetGrowth, BidAskSpread, BookLeverage, BrandInvest, ChInvIA, DelEqu, HerfAsset, HerfBE, LRreversal, Price, RDcap, STreversal, Size, SurpriseRD, TotalAccruals
# Cluster 19: AM, BM, CF, CashProd, EBM, EP, EntMult, IntanBM, IntanCFP, IntanEP, Leverage, MomOffSeason16YrPlus, MomSeason06YrPlus, MomSeason16YrPlus, NetPayoutYield, SP, ShareVol, cfp
# Cluster 20: sinAlgo
# Cluster 21: Illiquidity, VolMkt, VolSD, std_turn, zerotrade, zerotradeAlt1, zerotradeAlt12
#
# Cluster 1: CompEquIss, Investment, MS, OrgCap, VarCF, roaq
# Cluster 2: AdExp, BPEBM, ChTax, ConvDebt, DelDRC, DelFINL, DelLTI, DelNetFin, GrLTNOA, GrSaleToGrInv, GrSaleToGrOverhead, NetDebtFinance, OPLeverage, PctTotAcc, RDS, RevenueSurprise
# Cluster 3: Frontier
# Cluster 4: CBOperProf, GP, OperProfRD
# Cluster 5: OrderBacklogChg
# Cluster 6: DivYieldST, EquityDuration, PS
# Cluster 7: RDAbility
# Cluster 8: Accruals, ChAssetTurnover, ChNWC, CompositeDebtIssuance, EarningsConsistency, EarningsStreak, EarningsSurprise, FR, GrAdExp, MeanRankRevGrowth, NetEquityFinance, NumEarnIncrease, OScore, OperProf, OrderBacklog, PctAcc, PredictedFE, RoE, ShareIss1Y, ShareRepurchase, Tax, XFIN
# Cluster 9: AbnormalAccruals, AssetGrowth, BookLeverage, BrandInvest, ChInvIA, DebtIssuance, DelEqu, NetDebtPrice, RDcap, SurpriseRD, TotalAccruals
# Cluster 10: AM, BM, BMdec, CF, CashProd, EBM, EP, EntMult, IntanBM, IntanCFP, IntanEP, Leverage, NetPayoutYield, SP, cfp
# Cluster 11: AccrualsBM, ChEQ, ChInv, ChNNCOA, DelCOA, DelCOL, EarnSupBig, IntanSP, InvGrowth, InvestPPEInv, PayoutYield, ShareIss5Y, dNoa, grcapx, grcapx3y
# Cluster 12: realestate
# Cluster 13: Cash, NOA, RD, tang
# Completed on Accounting
#
# Cluster 1: DelBreadth, RIO_MB
# Cluster 2: IO_ShortInterest
# Cluster 3: RIO_Disp, RIO_Turnover, RIO_Volatility
# Completed on 13F
#
# Cluster 1: AOP, AnalystValue, Recomm_ShortInterest, fgr5yrLag, sfe
# Cluster 2: ChNAnalyst
# Cluster 3: ConsRecomm
# Cluster 4: AnalystRevision, ChForecastAccrual, ChangeInRecommendation, DownRecomm, EarningsForecastDisparity, ExclExp, REV6, UpRecomm
# Cluster 5: FEPS, ForecastDispersion
# Completed on Analyst
#
# Cluster 1: AnnouncementReturn, Beta, BetaFP, IndRetBig, LRreversal, MRreversal, MomOffSeason, MomOffSeason06YrPlus, MomSeason, MomSeason06YrPlus, MomSeason16YrPlus, MomSeasonShort, Price, STreversal, Size
# Cluster 2: Coskewness, IdioVol3F, IdioVolAHT, RealizedVol
# Cluster 3: IntMom
# Cluster 4: MaxRet
# Cluster 5: Mom12m, Mom12mOffSeason, Mom6m, MomRev, MomVol, ResidualMomentum
# Cluster 6: BetaLiquidityPS, BetaTailRisk, CoskewACX, High52, IndMom, MomOffSeason11YrPlus, MomOffSeason16YrPlus, MomSeason11YrPlus, ReturnSkew, ReturnSkew3F, TrendFactor, retConglomerate
# Completed on Price

# Cluster 1: CustomerMomentum
# Cluster 2: FirmAge, Herf, HerfAsset, HerfBE, hire, iomom_cust, iomom_supp
# Cluster 3: sinAlgo
# Completed on Other

# Cluster 1: DivInit, DivSeason
# Cluster 2: AgeIPO, DivOmit, ExchSwitch, IndIPO, RDIPO
# Cluster 3: Spinoff
# Completed on Event

# Cluster 1: BidAskSpread
# Cluster 2: DolVol, ShortInterest
# Cluster 3: ShareVol
# Cluster 4: Illiquidity, VolMkt, VolSD, VolumeTrend, std_turn, zerotrade, zerotradeAlt1, zerotradeAlt12
# Completed on Trading

# Cluster 1: CPVolSpread
# Cluster 2: RIVolSpread
# Cluster 3: OptionVolume1, OptionVolume2, SmileSlope, dCPVolSpread, dVolCall, dVolPut, skew1
# Completed on Options
