import requests
import pandas as pd
import time

from tabulate import tabulate

TICKER_CSV_FILE = "ticker.csv"
API_KEY = ""  # Replace with your Alpha Vantage API key

def measure_time(func, *args, **kwargs):
    """
    Measures the execution time of a given function.

    Args:
        func (callable): The function to be timed.
        *args: Positional arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        tuple: A tuple containing the function's return value and the elapsed time in seconds.
    """
    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    return result, elapsed_time

def get_daily_stock_data(ticker, start_date, end_date):
    url = f"https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "apikey": API_KEY,
        "outputsize": "full",
        "datatype": "json"
    }
    
    print("Retrieving Ticker Data for ", ticker ,"\n")

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
        "5. volume": "Volume"
    }, inplace=True)

    print("#"*10, "DATEFRAME DATA", "#"*10,"\n")
    print(df)
  
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    #Filter data between the given time range
    #df = df.loc[(df.index >= pd.to_datetime(start_date)) & (df.index <= pd.to_datetime(end_date))]
    
    for col in ["Open", "High", "Low", "Close","Volume"]:
        df[col] = pd.to_numeric(df[col])
    
    df["Ticker"] = ticker
    df.reset_index(inplace=True)
    df.rename(columns={"index": "Date"}, inplace=True)
    
    return df[["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]]

# Read tickers from CSV
tickers_df = pd.read_csv(TICKER_CSV_FILE)  # CSV should have a column named 'Ticker'
tickers = tickers_df["Ticker"].tolist()

print(tickers)

all_data = []
API_KEY = input("Enter Alpha Vantage API Key")
total_time = 0

for ticker in tickers:
  # Time a call to my_method
    df, time_taken = measure_time(get_daily_stock_data, ticker, "2025-03-01", "2025-08-20")
    print(f"Method returned: {df}")
    print(f"Time taken: {time_taken:.6f} seconds")
    total_time += time_taken
    #df = get_daily_stock_data(ticker, "2025-03-01", "2025-08-20")
    
    # print("\t" *2, "#"*10, "START FILTERED DATA (", ticker, ") ", "#"*10,"\n")
    # print(df)
    # print("\t" *2, "#"*10, "END FILTERED DATA (", ticker, ") ", "#"*10,"\n")
    all_data.append(df)
    time.sleep(12)  # To respect Alpha Vantage free tier call limits

print (f"TOTAL ELAPSED TIME: {total_time:.6f} seconds")

def export_to_csv():

  combined_data = pd.concat(all_data)
  combined_data = combined_data.sort_values(by="Date")
  return combined_data.to_csv("myout.csv")
  
csv, time_taken = measure_time(export_to_csv)
print(f"Time taken for CSV export: {time_taken:.6f} seconds")

