# Open Source Asset Pricing


## Roadmap
https://shorturl.at/pHTU2

## Key Artifacts

### General 
- **econ_yyyymm.pdf** trackes the most recent macroeconomic, monetary, and fiscal development. 
### ETF buys
- **ETF_yyyymm.xlsx** score the price trend in 19 ETFs (11 Sectors + 8 Size-style) based on [momentum, reversal, and seasonalities.](https://github.com/zhuolonghao/OpenSourceAssetPricing/blob/main/02.Signals/Code/Signals/ETF_01.create_signals.py) 

	|ETF|Vote|ST-reversal|Mom_6m|...|Season_1115
	|---      |---     			|---		|---			|--|---|
	|XLC|84.25|7.75|10|...|1.0|
	|MGK|83.25|3.25|7.75|...|1.0|
	|VUG|81.00|7.75|3.25|...|10.0|
	
### Ticker buys

- **yyyymm.xlsx** shows how a company scores/ranks in each anomaly. 

	|Ticker|Size-Style	|Sector|Mom_6m|...|BM_q|
	|---      |---     			|---		|---			|--|---|
	|MSFT|Mega-Growth|IT-Software|8.0|...|1.0|
	|OPRT|Micro|Fin-Con. Finance|1.0|...|10.0|
	|PR|Small-Value|Energy-E&P|6.0|...|7.0|


- **rankings_yyyymm.xlsx** exhibits which companies are selected in the anomaly portfolio, and categorizes them into [several major groups](https://github.com/zhuolonghao/OpenSourceAssetPricing/blob/main/_utility/_anomaly_portfolio.py). 

	|Ticker|Mom_6m|...|BM_q|Selected	|Selected, wgt|Momentum|Valuation|...|Seasonality|
	|---      |---     			|---		|---			|--|---|---|---|---|---|
	|MSFT|Flase|...|False|6|3|1|0|...|3|
	|PR|False|...|False|8|3|5|0|...|0|
	|OPRT|False|...|True|4|1|0|3|...|5|

- [In progress] **s2ss_yyyymm.xlsx** identifies some Sector x Size & Style that exhibits momentums.

## Other Outputs
- **validate_yyyymm.xlsx** examines how many tickers are not considerd for each anomaly due to data quality (missing values) / restrictions (e.g., limiting to NYSE/AMEX).
- **turnover_yyyymm.xlsx** marks the anomaly turnover (e.g., whether MSFT selected in current and previous month by MomRev).
-  For the anomly constructed in the beginning of month t
	 -  **xxx_zzz_yyyymm.xlsx** tracks the return in month t-1 
		> Goal: check if the anomaly is too hot to buy in case of reversal.
	-  **BT_xxx_zzz_yyyymm.xlsx** tracks the its reutrn in month t. 
		> Goal: examine if the anomaly returns is good. 
