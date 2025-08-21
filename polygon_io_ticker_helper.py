import requests
import pandas as pd
import time
import json

API_KEY = ""  # Replace with your polygon.io API key

def get_daily_stock_data_polygon(ticker, start_date, end_date):
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 5000,
        "apiKey": API_KEY
    }

    response = requests.get(url, params=params)
    data = response.json()

    #json.dump(data, open(f"{ticker}.json", "w"))

    print("*"*10, f"RAW RESULT FROM POLYGON IO IS RETRIEVED FOR {ticker}f", "*"*10)

    if "results" not in data:
        print(f"Error fetching data for {ticker}: {data.get('error') or 'Unknown error'}")
        print(data)
        return pd.DataFrame()

    results = data["results"]
    df = pd.DataFrame(results)

    # Convert timestamp to datetime
    df['t'] = pd.to_datetime(df['t'], unit='ms')
    df.rename(columns={
        't': 'Date',
        'o': 'Open',
        'h': 'High',
        'l': 'Low',
        'c': 'Close',
        'v': 'Volume'
    }, inplace=True)

    df["Ticker"] = ticker
    df = df[["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]]
    #print(df)
    return df

# Read tickers from CSV file, assuming column "Ticker"
tickers_df = pd.read_csv("sample_data/tickers.csv")
tickers = tickers_df["Ticker"].tolist()
print(tickers)

API_KEY = input("Enter Polygon.io API KEY")
all_data = []

for ticker in tickers:
    df = get_daily_stock_data_polygon(ticker, "1999-11-01", "2025-08-20")
    all_data.append(df)
    time.sleep(12)  # Respect API rate limits

combined_data = pd.concat(all_data)
combined_data = combined_data.sort_values(by="Date")

print(combined_data.head())
combined_data = pd.concat(all_data)
combined_data = combined_data.sort_values(by="Date")
combined_data.to_csv("sample_data/polygon_io_stock_history.csv")

