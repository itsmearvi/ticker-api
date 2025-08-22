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

    print("*"*10, f"RAW RESULT FROM POLYGON IO IS RETRIEVED FOR {ticker}", "*"*10)

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

def get_sentiment_analysis_polygon(ticker, start_date, end_date):
    '''
    Fetches sentiment analysis for a ticker in a date range from polygon.io news/sentiment endpoint.
    '''
    url = "https://api.polygon.io/v2/reference/news"
    params = {
        "ticker": ticker,
        "published_utc.gte": start_date,
        "published_utc.lte": end_date,
        "limit": 100,  # max articles per request (adjust as needed)
        "sort": "published_utc",
        "apiKey": API_KEY
    }
    
    response = requests.get(url, params=params)
    data = response.json()

    print("!"*10, f"SENTIMENT DATA FROM POLYGON IO IS RETRIEVED FOR {ticker}", "!"*10)
    
    if "results" not in data:
        print(f"Error fetching sentiment for {ticker}: {data.get('error') or 'Unknown error'}")
        return pd.DataFrame()
    
    articles = data["results"]
    
    # Extract relevant sentiment info into DataFrame
    records = []
    for article in articles:
        records.append({
            "Date": pd.to_datetime(article.get("published_utc")).date(),
            "Ticker": ticker,
            "Headline": article.get("title"),
            "Sentiment": article.get("sentiment", {}).get("overall", None),
            "Source": article.get("source", None),
            "ArticleURL": article.get("article_url", None)
        })
    
    df = pd.DataFrame(records)
    return df

# Read tickers from CSV file, assuming column "Ticker"
tickers_df = pd.read_csv("sample_data/tickers.csv")
tickers = tickers_df["Ticker"].tolist()
print(tickers)

API_KEY = input("Enter Polygon.io API KEY --> ")
all_stock_data = []
all_sentiment_data = []
start_date = "1999-11-01"
end_date = "2025-08-20"

for ticker in tickers:
    stock_df = get_daily_stock_data_polygon(ticker, start_date=start_date, end_date=end_date)
    if stock_df.empty:
        continue
    
    all_stock_data.append(stock_df)
    sentiment_df = get_sentiment_analysis_polygon(ticker, start_date, end_date)
    all_sentiment_data.append(sentiment_df)

    time.sleep(12)  # Respect API rate limits

combined_stock_data = pd.concat(all_stock_data).sort_values(by="Date")

combined_sentiment_data = pd.concat(all_sentiment_data).sort_values(by="Date")

print(combined_stock_data.head())
print(combined_sentiment_data.head())

combined_stock_data = pd.concat(all_stock_data)
combined_stock_data = combined_stock_data.sort_values(by="Date")
combined_stock_data.to_csv("sample_data/polygon_io_stock_history.csv")

combined_sentiment_data = pd.concat(combined_sentiment_data)
combined_sentiment_data = combined_sentiment_data.sort_values(by="Date")
combined_sentiment_data.to_csv("sample_data/polygon_io_sentiment_history.csv")

