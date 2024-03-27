# Open Source Asset Pricing


## Roadmap
https://shorturl.at/pHTU2

## Key Artifacts

### General 
- **[econ.pdf](https://github.com/zhuolonghao/OpenSourceAssetPricing/blob/main/02.Signals/econ.pdf)** identifies three key signals. [doc](https://github.com/zhuolonghao/OpenSourceAssetPricing/blob/main/02.Signals/econ_details.docx) and [code](https://github.com/zhuolonghao/OpenSourceAssetPricing/blob/main/02.Signals/Code/Signals/06.econ_analysis.py) 
  
### ETF buys
- **ETF_yyyymm.xlsx** scores the winning/quality ETFs from 11 Sectors + 8 Size-style ETF [code](https://github.com/zhuolonghao/OpenSourceAssetPricing/blob/main/02.Signals/Code/Signals/ETF_01.create_signals.py) 

	|ETF|Vote|ST-reversal|Mom_6m|...|Season_1115
	|---      |---     			|---		|---			|--|---|
	|XLC|84.25|7.75|10|...|1.0|
	|MGK|83.25|3.25|7.75|...|1.0|
	|VUG|81.00|7.75|3.25|...|10.0|
	
### Ticker buys
- **s2ss_yyyymm.xlsx** picks the winning group characterized by sector x Size & Style. [template]((https://github.com/zhuolonghao/OpenSourceAssetPricing/blob/main/02.Signals/s2ss_yyyymm.xlsx)) and [code](https://github.com/zhuolonghao/OpenSourceAssetPricing/blob/main/02.Signals/Code/Signals/07.Sector_SizeStyle.py)
 
- **yyyymm.xlsx** scores companies by [anomalies](https://github.com/zhuolonghao/OpenSourceAssetPricing/blob/main/02.Signals/Code/Signals/01.create_signals.py) 

	|Ticker|Size-Style	|Sector|Mom_6m|...|BM_q|
	|---      |---     			|---		|---			|--|---|
	|MSFT|Mega-Growth|IT-Software|8.0|...|1.0|
	|OPRT|Micro|Fin-Con. Finance|1.0|...|10.0|
	|PR|Small-Value|Energy-E&P|6.0|...|7.0|

- **rankings_yyyymm.xlsx** picks the winning companies by [anomalies](https://github.com/zhuolonghao/OpenSourceAssetPricing/blob/main/02.Signals/Code/Signals/04.review_performance.py) 

	|Ticker|Mom_6m|...|BM_q|Selected	|Selected, wgt|Momentum|Valuation|...|Seasonality|MomRev|
	|---      |---     			|---		|---			|--|---|---|---|---|---|---|
	|MSFT|Flase|...|False|6|3|1|0|...|3|True|
	|PR|False|...|False|8|3|5|0|...|0|False|
	|OPRT|False|...|True|4|1|0|3|...|5|False|

## Other Outputs
- **validate_yyyymm.xlsx** examines how many tickers are not considerd for each anomaly due to data quality (missing values) / restrictions (e.g., limiting to NYSE/AMEX).
- **turnover_yyyymm.xlsx** marks the anomaly turnover (e.g., whether MSFT is selected in the current and previous month by MomRev).
-  For the anomaly constructed at the beginning of the month t
	 -  **xxx_zzz_yyyymm.xlsx** tracks the return in month t-1 
		> Goal: check if the anomaly is too hot to buy in case of reversal.
	-  **BT_xxx_zzz_yyyymm.xlsx** tracks its return in month t. 
		> Goal: examine if the anomaly return is good. 
