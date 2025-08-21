import requests
import pandas as pd
import time

from tabulate import tabulate

API_KEY = "YOUR_ALPHA_VANTAGE_API_KEY"  # Replace with your Alpha Vantage API key

def get_daily_stock_data(ticker, start_date, end_date):
    url = f"https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": ticker,
        "apikey": API_KEY,
        "outputsize": "full",
        "datatype": "json"
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    if "Time Series (Daily)" not in data:
        print(f"Error fetching data for {ticker}: {data.get('Note') or data.get('Error Message') or 'Unknown error'}")
        return pd.DataFrame()
    
    time_series = data["Time Series (Daily)"]
    df = pd.DataFrame.from_dict(time_series, orient='index')
    
    df.rename(columns={
        "1. open": "Open",
        "2. high": "High",
        "3. low": "Low",
        "4. close": "Close",
        "5. adjusted close": "Adj Close",
        "6. volume": "Volume",
        "7. dividend amount": "Dividend Amount"
    }, inplace=True)
    
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df = df.loc[(df.index >= pd.to_datetime(start_date)) & (df.index <= pd.to_datetime(end_date))]
    
    for col in ["Open", "High", "Low", "Close", "Adj Close", "Volume"]:
        df[col] = pd.to_numeric(df[col])
    
    df["Ticker"] = ticker
    df.reset_index(inplace=True)
    df.rename(columns={"index": "Date"}, inplace=True)
    
    return df[["Date", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"]]

# Read tickers from CSV
tickers_df = pd.read_csv("tickers.csv")  # CSV should have a column named 'Ticker'
tickers = tickers_df["Ticker"].tolist()

all_data = []

for ticker in tickers:
    df = get_daily_stock_data(ticker, "2025-08-01", "2025-08-20")
    all_data.append(df)
    time.sleep(12)  # To respect Alpha Vantage free tier call limits

combined_data = pd.concat(all_data)
combined_data = combined_data.sort_values(by="Date")

print(combined_data.head())

# After combining and sorting the data
print(tabulate(combined_data, headers='keys', tablefmt='psql', showindex=False))
