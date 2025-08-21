import requests
import pandas as pd
import time

from tabulate import tabulate

API_KEY = "QRR5XILJY4O9Y4NT'"  # Replace with your Alpha Vantage API key

def get_daily_stock_data(ticker, start_date, end_date):
    url = f"https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "apikey": API_KEY,
        "outputsize": "full",
        "datatype": "json"
    }
    
    response = requests.get(url, params=params)
    data = response.json()

    print("******************* RESPONSE DATA AS JSON STARTS *********************\n")
    print(data)
    
    print("******************* RESPONSE DATA AS JSON ENDS *********************\n")

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
        "5. volume": "Volume"
    }, inplace=True)

    print("#"*10, "DATEFRAME DATA", "#"*10,"\n")
    print(df)
  
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df = df.loc[(df.index >= pd.to_datetime(start_date)) & (df.index <= pd.to_datetime(end_date))]
    
    for col in ["Open", "High", "Low", "Close","Volume"]:
        df[col] = pd.to_numeric(df[col])
    
    df["Ticker"] = ticker
    df.reset_index(inplace=True)
    df.rename(columns={"index": "Date"}, inplace=True)
    
    return df[["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]]

# Read tickers from CSV
tickers_df = pd.read_csv("ticker.csv")  # CSV should have a column named 'Ticker'
tickers = tickers_df["Ticker"].tolist()

print(tickers)

all_data = []

for ticker in tickers:
    df = get_daily_stock_data(ticker, "2025-08-01", "2025-08-01")
    print("\t" *2, "#"*10, "START FILTERED DATA (", ticker, ") ", "#"*10,"\n")
    print(df)
    print("\t" *2, "#"*10, "END FILTERED DATA (", ticker, ") ", "#"*10,"\n")
    all_data.append(df)
    time.sleep(12)  # To respect Alpha Vantage free tier call limits

combined_data = pd.concat(all_data)
combined_data = combined_data.sort_values(by="Date")
