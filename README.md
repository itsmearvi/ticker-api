# ticker-api

There are several alternative APIs to yfinance for getting daily stock price data with all relevant columns in Python:

**Alpha Vantage**: Provides free daily time series data including OHLCV. Requires free API key registration. Python wrappers available like alpha_vantage.

**Twelve Data**: Offers historical and real-time stock data via API with a Python client library. Requires API key, supports daily data with OHLCV.

**Polygon.io**: Professional financial data API offering comprehensive market data including daily prices. Requires an API key and has Python SDK.

**IEX Cloud**: Real-time and historical stock data API with Python SDK, requiring signup and API token.

**Quandl**: Offers various financial datasets including stock prices, accessible via Python API with authorization.

These APIs generally require registering for a free or paid API key. They provide OHLCV data (Open, High, Low, Close, Volume, and sometimes Adjusted Close) and support date filtering.

For example, Alpha Vantage daily data can be accessed using their Python client by specifying symbol and output size. Similarly, Twelve Data and Polygon.io have official Python clients making integration straightforward.
