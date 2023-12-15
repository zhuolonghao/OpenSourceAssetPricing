import yfinance as yf #yf.__version__=='0.2.32'

benchmark = yf.download('spy', period='10y', interval='1mo')