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
    # print(data)

    if "results" not in data:
        print(f"Error fetching data for {ticker}: {data.get('error') or 'Unknown error'}")
        print(data)
        return pd.DataFrame()

    results = data["results"]
    df = pd.DataFrame(results)
    
    # Convert timestamp to date
    #df['t'] = pd.to_datetime(df['t']).dt.date
    
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


def get_insight_analysis_polygon(article):
  positive_insight_count = 0
  negative_insight_count = 0
  neutral_insight_count = 0
  positive_sentiment_desc_list = []
  negative_sentiment_desc_list = []
  neutral_sentiment_desc_list = []
  
  tickers = "|".join(article.get("tickers", []))
  keywords = "|".join(article.get("keywords", []))
  

  if "insights" in article:
    # Process insights
    insights = article.get("insights", [])

    for insight in insights:
        sentiment = insight.get("sentiment")
        reasoning = insight.get("sentiment_reasoning", "").strip()
        if sentiment == "positive":
            positive_insight_count += 1
            if reasoning:
                positive_sentiment_desc_list.append(reasoning)
        elif sentiment == "negative":
            negative_insight_count += 1
            if reasoning:
                negative_sentiment_desc_list.append(reasoning)
        elif sentiment == "neutral":
            neutral_insight_count += 1
            if reasoning:
                neutral_sentiment_desc_list.append(reasoning)

  positive_sentiment_desc = "|".join(positive_sentiment_desc_list).strip()
  negative_sentiment_desc = "|".join(negative_sentiment_desc_list).strip()
  neutral_sentiment_desc = "|".join(neutral_sentiment_desc_list).strip()

  # print(positive_sentiment_desc)
  # print(negative_sentiment_desc)
  # print(neutral_sentiment_desc)
    
  return tickers, keywords, positive_insight_count, negative_insight_count, neutral_insight_count, positive_sentiment_desc, negative_sentiment_desc, neutral_sentiment_desc


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

    #print("!"*10, f"SENTIMENT DATA FROM POLYGON IO IS RETRIEVED FOR {ticker} \n\t {data}", "!"*10)

    if "results" not in data:
        print(f"Error fetching sentiment for {ticker}: {data.get('error') or 'Unknown error'}")
        return pd.DataFrame()

    articles = data["results"]

    # Extract relevant sentiment info into DataFrame
    records = []
    for article in articles:
        # print("#" *10, "ARTICLE SENTIMENT ", "#" *10)
        # print(article)

        tickers, keywords, positive_insight_count, negative_insight_count, neutral_insight_count, positive_sentiment_desc, negative_sentiment_desc, neutral_sentiment_desc = get_insight_analysis_polygon(article)

        # Append to records list
        records.append({
            "Date": pd.to_datetime(article.get("published_utc")).date(),
            "Ticker": ticker,
            "Tickers":tickers,
            "Keywords":keywords,
            "PositiveInsights": positive_insight_count,
            "NegativeInsights": negative_insight_count,
            "NeutralInsights": neutral_insight_count,
            "Headline": article.get("title"),
            "Source": article.get("publisher", None).get("name"),
            "ArticleURL": article.get("article_url", None),
            "PositiveSentimentDesc": positive_sentiment_desc,
            "NegativeSentimentDesc": negative_sentiment_desc,
            "NeutralSentimentDesc": neutral_sentiment_desc
        })

    df = pd.DataFrame(records)
    return df


def unique_pipe_separated(s):
    if not isinstance(s, str):
        return s
    parts = s.split('|')
    unique_parts = list(dict.fromkeys(parts))
    return '|'.join(unique_parts)
# Read tickers from CSV file, assuming column "Ticker"
tickers_df = pd.read_csv("sample_data/tickers.csv")
tickers = tickers_df["Ticker"].tolist()
#tickers=["AARK"]
print(tickers)

API_KEY = input("Enter Polygon.io API KEY --> ")
all_stock_data = []
all_sentiment_data = []
start_date = "1999-11-01"
end_date = "2025-08-22"

for ticker in tickers:
    stock_df = get_daily_stock_data_polygon(ticker, start_date, end_date)
    #print(f"Stock Data Frame is {stock_df}")
    if stock_df.empty:
        continue

    all_stock_data.append(stock_df)
    sentiment_df = get_sentiment_analysis_polygon(ticker, start_date, end_date)
    all_sentiment_data.append(sentiment_df)

    time.sleep(12)  # Respect API rate limits

# print(f"All Stock Data frame is {all_stock_data}")
# print(f"All Sentiment Data frame is {all_sentiment_data}")

if all_stock_data:
  combined_stock_data = pd.concat(all_stock_data).sort_values(by="Date")

if all_sentiment_data:
  combined_sentiment_data = pd.concat(all_sentiment_data).sort_values(by="Date")

# print(combined_stock_data.head())
# print(combined_sentiment_data.head())

combined_stock_data.to_csv("sample_data/polygon_io_stock_history.csv")

combined_sentiment_data.to_csv("sample_data/polygon_io_sentiment_history.csv")

# Merge stock and sentiment data on Date and Ticker
combined_stock_data["Date"] = pd.to_datetime(combined_stock_data['Date']).dt.date
combined_sentiment_data["Date"] = pd.to_datetime(combined_sentiment_data['Date']).dt.date

# Aggregate sentiment_df on Date and Ticker with required aggregation:
agg_functions = {
    "Tickers": lambda x: "|".join(sorted(set(x.dropna()))),
    "Keywords": lambda x: "|".join(sorted(set(x.dropna()))),
    "Headline": lambda x: " | ".join(x.dropna()),
    "Source": lambda x: " | ".join(x.dropna()),
    "ArticleURL": lambda x: " | ".join(x.dropna()),
    "PositiveSentimentDesc": lambda x: " ".join(x.dropna()),
    "NegativeSentimentDesc": lambda x: " ".join(x.dropna()),
    "NeutralSentimentDesc": lambda x: " ".join(x.dropna()),
    "PositiveInsights": "sum",
    "NegativeInsights": "sum",
    "NeutralInsights": "sum",
}

sentiment_agg = combined_sentiment_data.groupby(['Date', 'Ticker'], as_index=False).agg(agg_functions)

# Rename summed columns for clarity
sentiment_agg = sentiment_agg.rename(columns={
    "PositiveInsights": "Total Positive Insights",
    "NegativeInsights": "Total Negative Insights",
    "NeutralInsights": "Total Neutral Insights"
})

# Merge aggregated sentiment data into the stock DataFrame
merged_df = pd.merge(combined_stock_data, sentiment_agg, on=['Date', 'Ticker'], how='left')
merged_df.to_csv("sample_data/merged_output.csv", index=False)


merged_df = pd.merge(combined_stock_data, sentiment_agg, on=['Date', 'Ticker'], how='left')

sample_df = merged_df
print(len(sample_df))

print("*"*10, " BEFORE Replacement", "*"*10)
print(sample_df.iloc[8010]['Tickers'])

# print(sample_df.iloc[0]['Tickers'])
# sample_df.iloc[0] = sample_df.iloc[0].apply(unique_pipe_separated)
# print(sample_df.iloc[0]['Tickers'])

for col in ['Tickers', 'Keywords', "PositiveSentimentDesc", "NegativeSentimentDesc", "NeutralSentimentDesc"]:
    if col in sample_df.columns:
        sample_df[col] = sample_df[col].apply(unique_pipe_separated)
print("*"*10, " After Replacement", "*"*10)
print(sample_df.iloc[8010]['Tickers'])
sample_df.to_csv("sample_data/merged_compact_output.csv", index=False)
