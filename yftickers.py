import yfinance as yf
import pandas as pd

from tabulate import tabulate

# Read tickers from CSV file
tickers_df = pd.read_csv("tickers.csv")  # Replace with your CSV filepath
tickers = tickers_df["Ticker"].tolist()  # Extract ticker column as list

all_data = []

for ticker in tickers:
    data = yf.download(ticker, start="2025-08-01", end="2025-08-20", interval="1d")
    data.reset_index(inplace=True)
    data["Ticker"] = ticker
    
    needed_columns = ["Date", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
    available_columns = [col for col in needed_columns if col in data.columns]
    
    daily_data = data[available_columns]
    all_data.append(daily_data)

combined_data = pd.concat(all_data)
combined_data = combined_data.sort_values(by="Date")

print(combined_data.head())

print(tabulate(combined_data.head(), headers="firstrow", tablefmt="grid"))
